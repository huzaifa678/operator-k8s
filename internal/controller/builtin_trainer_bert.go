/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

// bertClassifierScript is the controller-supplied training script selected by
// Spec.BuiltinTrainer="BERTClassifier". Lives in Go so users don't paste
// Python into YAML; the TrainingRunReconciler materializes it as a ConfigMap
// just like a user-supplied Spec.Script.
//
// Defaults match the orders/articles pipeline:
//   - reads Parquet written by elt_orders.py (any partition glob)
//   - distilbert-base-uncased on CPU, ModernBERT-base when CUDA is visible
//   - TEXT_COLUMN/LABEL_COLUMN env-driven so the same script handles "title→source"
//     (articles) or "status→items_count" (legacy orders) without a code change
//
// Environment overrides (all optional):
//
//	BERT_MODEL           HuggingFace repo id (defaults: see above)
//	TEXT_COLUMN          column used as the BERT input             (default: "title")
//	LABEL_COLUMN         column used as the training label         (default: "source")
//	NUM_LABELS           override; 0 → auto-derive from label set
//	DATA_PARQUET_GLOB    where to load data from (default: /data/**/*.parquet recursive)
//	MODEL_OUT            where rank-0 saves the fine-tuned model   (default: /data/model)
//	EPOCHS / BATCH_SIZE / MAX_LENGTH / LR — hyperparams
const bertClassifierScript = `"""
BERTClassifier — controller-supplied training script.

Reads Parquet produced by the ELT pipeline, fine-tunes a HuggingFace BERT-family
model with DDP, saves to MODEL_OUT on rank 0.

Configurable purely via env vars; see Go: bertClassifierScript for the list.
"""
import glob
import json
import os

import pandas as pd
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.optim import AdamW
from torch.utils.data import DataLoader, Dataset, DistributedSampler
from transformers import AutoModelForSequenceClassification, AutoTokenizer

_CUDA = torch.cuda.is_available()
DEFAULT_MODEL = "answerdotai/ModernBERT-base" if _CUDA else "distilbert-base-uncased"

MODEL      = os.environ.get("BERT_MODEL", DEFAULT_MODEL)
TEXT_COL   = os.environ.get("TEXT_COLUMN", "title")
LABEL_COL  = os.environ.get("LABEL_COLUMN", "source")
NUM_LABELS = int(os.environ.get("NUM_LABELS", "0"))   # 0 → auto from data
DATA_GLOB  = os.environ.get("DATA_PARQUET_GLOB", "/data/**/*.parquet")
MODEL_OUT  = os.environ.get("MODEL_OUT", "/data/model")
EPOCHS     = int(os.environ.get("EPOCHS", "3"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "16" if _CUDA else "8"))
MAX_LENGTH = int(os.environ.get("MAX_LENGTH", "128"))
LR         = float(os.environ.get("LR", "3e-5"))

def load_parquet(pattern):
    files = glob.glob(pattern, recursive=True)
    if not files:
        raise RuntimeError(
            f"No Parquet files match {pattern}. "
            "Check the ELT job ran and datasetPVC mounts the right path."
        )
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)

def build_dataframe():
    df = load_parquet(DATA_GLOB)
    if TEXT_COL not in df.columns or LABEL_COL not in df.columns:
        raise RuntimeError(
            f"Required columns missing: text={TEXT_COL!r} label={LABEL_COL!r}. "
            f"Available: {list(df.columns)}"
        )
    df = df.dropna(subset=[TEXT_COL, LABEL_COL])
    df[TEXT_COL]  = df[TEXT_COL].astype(str)
    df[LABEL_COL] = df[LABEL_COL].astype(str)

    # Stable label encoding (sorted) — same data, same ints across ranks.
    classes = sorted(df[LABEL_COL].unique().tolist())
    label_to_id = {c: i for i, c in enumerate(classes)}
    df["_label"] = df[LABEL_COL].map(label_to_id)
    df["_text"]  = df[TEXT_COL]

    return df[["_text", "_label"]].reset_index(drop=True), label_to_id

class TextDataset(Dataset):
    def __init__(self, df, tok):
        self.labels = torch.tensor(df["_label"].tolist(), dtype=torch.long)
        self.enc = tok(
            df["_text"].tolist(),
            padding="max_length",
            truncation=True,
            max_length=MAX_LENGTH,
            return_tensors="pt",
        )
    def __len__(self): return len(self.labels)
    def __getitem__(self, i):
        return self.enc["input_ids"][i], self.enc["attention_mask"][i], self.labels[i]

def main():
    rank       = int(os.environ["RANK"])
    world_size = int(os.environ["WORLD_SIZE"])
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    backend    = os.environ.get("TORCH_DISTRIBUTED_DEFAULT_BACKEND",
                                "nccl" if _CUDA else "gloo")

    if _CUDA:
        torch.cuda.set_device(local_rank)
        device = torch.device(f"cuda:{local_rank}")
    else:
        device = torch.device("cpu")

    dist.init_process_group(backend=backend)
    print(f"[rank {rank}/{world_size}] model={MODEL} backend={backend} "
          f"device={device}", flush=True)

    df, label_to_id = build_dataframe()
    n_labels = NUM_LABELS or len(label_to_id)
    print(f"[data] {len(df):,} rows | {n_labels} classes | "
          f"text={TEXT_COL} label={LABEL_COL}", flush=True)

    tok = AutoTokenizer.from_pretrained(MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL, num_labels=n_labels).to(device)
    model = DDP(model, device_ids=[local_rank] if _CUDA else None)

    ds      = TextDataset(df, tok)
    sampler = DistributedSampler(ds, num_replicas=world_size, rank=rank, shuffle=True)
    loader  = DataLoader(ds, batch_size=BATCH_SIZE, sampler=sampler)
    optim   = AdamW(model.parameters(), lr=LR)

    for epoch in range(EPOCHS):
        sampler.set_epoch(epoch)
        total = 0.0
        for ids, mask, lab in loader:
            ids, mask, lab = ids.to(device), mask.to(device), lab.to(device)
            optim.zero_grad()
            out = model(input_ids=ids, attention_mask=mask, labels=lab)
            out.loss.backward()
            optim.step()
            total += out.loss.item()
        avg = total / max(len(loader), 1)
        print(f"[rank {rank}] epoch={epoch} avg_loss={avg:.4f}", flush=True)

    if rank == 0:
        os.makedirs(MODEL_OUT, exist_ok=True)
        model.module.save_pretrained(MODEL_OUT)
        tok.save_pretrained(MODEL_OUT)
        with open(os.path.join(MODEL_OUT, "label_to_id.json"), "w") as f:
            json.dump(label_to_id, f, indent=2)
        print(f"[rank 0] model + label map saved to {MODEL_OUT}", flush=True)

    dist.destroy_process_group()

if __name__ == "__main__":
    main()
`

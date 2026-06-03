#!/bin/bash
k3d cluster create compute-op --servers 1 --agents 2 --wait 2>&1 | tail -15
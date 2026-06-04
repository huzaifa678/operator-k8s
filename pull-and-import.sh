#!/bin/bash

docker pull apache/spark:3.5.3 2>&1 | tail -3 && k3d image import apache/spark:3.5.3 -c compute-op 2>&1 | tail -5
#!/usr/bin/env bash
set -euo pipefail

# Navigate to correct processing directory, regardless of invocation
SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR"
WORK_DIR="../../.."
cd "$WORK_DIR"

DATA_DIR="./data"
INPUT_DIR="$DATA_DIR/to_be_converted"
OUTPUT_DIR="$DATA_DIR/exports/direct"
mkdir -p "$OUTPUT_DIR"

# Set the number of concurrent process to the available cores
CONCURRENCY=$(nproc)

PY_MODULE="src.ingress.dsn.parquetify"

find "$INPUT_DIR/" -maxdepth 1 -type f -print0 |
  parallel -0 -j "$CONCURRENCY" \
    'python -m '"$PY_MODULE"' -z {} '"$OUTPUT_DIR"'/{/}.parquet'

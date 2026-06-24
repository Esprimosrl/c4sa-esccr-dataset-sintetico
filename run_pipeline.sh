#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHON="${PYTHON:-./bin/python}"
SEED="${SEED:-42}"
ANNO="${ANNO:-2024}"
JOBS="${JOBS:-2}"
FORCE="${FORCE:-}"

if [[ -n "$FORCE" ]]; then
  FORCE_ARGS=(--force)
else
  FORCE_ARGS=()
fi

"$PYTHON" dataset/pipeline_graph.py \
  --python "$PYTHON" \
  --anno "$ANNO" \
  --seed "$SEED" \
  --jobs "$JOBS" \
  "${FORCE_ARGS[@]}"

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHON="${PYTHON:-./bin/python}"
FORCE="${FORCE:-}"

ARGS=(--python "$PYTHON")
[[ -n "${SEED:-}" ]] && ARGS+=(--seed "$SEED")
[[ -n "${ANNO:-}" ]] && ARGS+=(--anno "$ANNO")
[[ -n "${JOBS:-}" ]] && ARGS+=(--jobs "$JOBS")

if [[ -n "$FORCE" ]]; then
  FORCE_ARGS=(--force)
else
  FORCE_ARGS=()
fi

"$PYTHON" dataset/pipeline_graph.py \
  "${ARGS[@]}" \
  "${FORCE_ARGS[@]}"

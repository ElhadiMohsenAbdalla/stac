#!/bin/bash
# ============================================================
# Standalone runner for caflood (no SLURM — this is a plain SSH box).
#
# Uses tmux so the pipeline keeps running if your SSH session drops —
# stage 2 can run for hours; you do NOT want that tied to your
# terminal window staying open.
#
# Usage:
#   ./run_standalone.sh 01      # run just stage 1
#   ./run_standalone.sh all     # run all four stages in sequence
#
# Then detach with Ctrl+B, D — the job keeps running.
# Reattach anytime with:  tmux attach -t catchment_cube
# Check it's alive without attaching:  tmux ls
# ============================================================
set -euo pipefail

SESSION="catchment_cube"
STAGE="${1:-all}"
mkdir -p logs

cmd_for_stage() {
  case "$1" in
    01) echo "python scripts/01_stac_search.py" ;;
    02) echo "python scripts/02_build_datacube.py" ;;
    03) echo "python scripts/03_compute_indices.py" ;;
    04) echo "python scripts/04_field_zonal_stats.py" ;;
    all) echo "python scripts/01_stac_search.py && \
               python scripts/02_build_datacube.py && \
               python scripts/03_compute_indices.py && \
               python scripts/04_field_zonal_stats.py" ;;
    *) echo "Unknown stage '$1'. Use 01, 02, 03, 04, or all." >&2; exit 1 ;;
  esac
}

CMD="$(cmd_for_stage "$STAGE")"
LOGFILE="logs/${STAGE}_$(date +%Y%m%d_%H%M%S).log"

# Keep BLAS/OMP threading from oversubscribing — Dask manages its own
# parallelism, and 16 cores gets oversubscribed fast if every library
# also tries to multithread underneath it.
FULL_CMD="export OMP_NUM_THREADS=1 MKL_NUM_THREADS=1; \
          conda activate catchment-cube; \
          cd $(pwd); \
          ${CMD} 2>&1 | tee ${LOGFILE}"

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "A '$SESSION' tmux session already exists. Attach with:"
  echo "  tmux attach -t $SESSION"
  echo "or kill it first with: tmux kill-session -t $SESSION"
  exit 1
fi

tmux new-session -d -s "$SESSION" "$FULL_CMD"
echo "Started in tmux session '$SESSION'. Log: $LOGFILE"
echo "Detach anytime: it keeps running. Reattach with:"
echo "  tmux attach -t $SESSION"

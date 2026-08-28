#!/bin/bash
# ============================================================
# SLURM batch script for the STAC -> Zarr pipeline on Isca.
#
# Submit with:  sbatch slurm_submit.sh
# Check status: squeue -u $USER
# Cancel:       scancel <job_id>
#
# Ask Research IT / Isca support for the correct --partition and
# --account values for your project before your first run —
# these are cluster-specific and this template can't guess them.
# ============================================================
#SBATCH --job-name=catchment_cube
#SBATCH --partition=pq              # CONFIRM with Isca docs/support
#SBATCH --account=your_project_code # CONFIRM with Isca docs/support
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=96G
#SBATCH --time=24:00:00
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

set -euo pipefail
mkdir -p logs

# --- environment -------------------------------------------------
module purge
module load Anaconda3   # exact module name varies by cluster — check `module avail anaconda`

source activate catchment-cube   # created via: conda env create -f environment.yml

# --- keep Dask/BLAS threading from oversubscribing the node -------
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

# --- run whichever stage you're currently on -----------------------
# Comment/uncomment as you progress through the pipeline. Running
# them as separate sbatch submissions (rather than all in one script)
# is safer — if stage 2 fails at year 4, you don't lose 1 and 2's
# work, and you can retry just the failed stage.

echo "Stage 1: STAC search"
python scripts/01_stac_search.py

# echo "Stage 2: build datacube (the long-running stage)"
# python scripts/02_build_datacube.py

# echo "Stage 3: compute indices"
# python scripts/03_compute_indices.py

# echo "Stage 4: field zonal statistics"
# python scripts/04_field_zonal_stats.py

echo "Done."

# Catchment Sentinel-2 datacube pipeline

Replaces the Google Earth Engine workflow with a STAC → COG → Zarr
pipeline. No shared GEE quota, no "user memory limit exceeded" —
memory is bounded explicitly at every stage by processing one year
(or one date) at a time.

Configured for **caflood** (standalone Linux box, no SLURM): 16 cores,
124GB RAM, 19TB free on `/data`. Storage and RAM headroom are both
generous here, so nothing in this pipeline needs to be frugal — the
memory-bounding (yearly chunking, per-timestep zonal stats) is kept
anyway because it's what avoids ever needing that headroom, not
because this box is tight.

## Setup

```bash
conda env create -f environment.yml
conda activate catchment-cube
```

**All data lives under `/data/ea667_catchment_project`** (set as
`data_root` in `config.yaml`), not in the repo checkout under
`/home` — `/home` only has ~64GB free, nowhere near enough for the
raster cube. Only code and config live in the repo.

Put two files under the data root before running anything:

- `/data/ea667_catchment_project/raw/catchment_boundary.gpkg` — one
  polygon, the 850 km² catchment.
- `/data/ea667_catchment_project/raw/catchment_fields.gpkg` — your
  field polygons, with a `field_id` column that matches the ID scheme
  in your ground-truth table and eventual SWAT+ HRUs (per the DMP §2
  naming convention — decide this ID once, don't change it).

Edit `config.yaml` if you want different paths, dates, or Dask
settings — current defaults (8 workers × 2 threads × 12GB) are sized
for this box's 16 cores / 124GB RAM.

## Pipeline order

```
./run_standalone.sh 01     # STAC search — metadata only, seconds
./run_standalone.sh 02     # build datacube — downloads + caches COGs, hours on first run
./run_standalone.sh 03     # compute indices — merges into raw bands, writes combined store
./run_standalone.sh 04     # field zonal stats -> field_timeseries.parquet
```

Or `./run_standalone.sh all` to chain them. Each runs inside a
`tmux` session (`catchment_cube`) so it survives your SSH connection
dropping — detach with `Ctrl+B, D`, reattach with
`tmux attach -t catchment_cube`. Logs land in `logs/`.

Run stages separately rather than always using `all` — if stage 2
fails partway through, you keep what already succeeded and just
retry that stage instead of re-running everything.

## COG caching

Stage 2 downloads every band it touches to
`/data/ea667_catchment_project/raw/cog_cache/<scene_id>/<band>.tif`
before reading it, and checks that cache first on every subsequent
run. Practically:

- **Re-running stage 2** (after a crash, or to extend the date range)
  never re-downloads a scene you already have.
- **Planetary Computer's signed URLs expire** (SAS tokens) — once a
  band is cached locally, that no longer matters for it. Stage 2
  re-signs items from Planetary Computer immediately before use, so
  even a stale `stac_items.json` from weeks ago still works.
- The cache stores the **full COG per band**, not just your
  catchment's window — costs more storage/bandwidth up front (~50–80GB
  total, estimated below) but means you can change the target bbox or
  resolution later without touching the network again.
- If a download fails, that one asset falls back to streaming from
  the remote URL for that run rather than failing the whole pipeline
  — check the stage 2 log for `failed` counts if that happens a lot.

## Combined raw + indices store

`datacube_indices.zarr` contains **both** the raw Sentinel-2 bands
and the derived indices (NDVI, NDRE, NDMI, NDWI, BSI, EVI, SAVI,
GNDVI) in one store — stage 4 and any QC work only need to open this
one file. `datacube_raw.zarr` (bands only) still exists as the
direct output of stage 2, in case you want to regenerate indices
without re-touching the slower raw-cube build.

## Pilot first

Before running this on the full 850 km², point `catchment.boundary_path`
at a single sub-catchment and run the whole pipeline end to end. This
was the DMP's own recommendation (prototype on a subset before
committing to full-catchment processing) — it also tells you your
real runtime and memory needs before you book a 24-hour SLURM
allocation for the full run.

## Storage estimate

~850 km² at 10 m ≈ 8.5M pixels, ~15 bands/indices, ~150–200 usable
scenes over 2020–2025, float32 → roughly 80–100 GB uncompressed,
~20–30 GB as Zarr with default blosc compression for the clipped
cube — a rounding error against 19TB free. The COG cache adds another
~50–80GB (full uncompressed scene tiles, not clipped to the
catchment). With the combined raw+indices store also duplicating the
raw bands, total footprint for this project lands somewhere around
150–200GB — still comfortably small for this box.

The one thing 2TB+ (or 19TB) local storage does **not** give you is
backup: confirm with whoever administers caflood whether `/data` is
backed up or replicated. If not, copy the finished
`processed/datacube_indices.zarr` and `processed/field_timeseries.parquet`
to Isca group storage or ORE periodically — those are the two things
that are genuinely expensive to regenerate; the COG cache and raw
cube aren't (they're just re-fetched/rebuilt from Planetary Computer,
now made even easier by the cache).

## Switching to Copernicus Dataspace

`01_stac_search.py` is written for Planetary Computer (no login to
search, free SAS-token signing for asset access). If you need a scene
PC doesn't have, Copernicus Dataspace's STAC endpoint is at
`https://catalogue.dataspace.copernicus.eu/stac` — you'll need a free
CDSE account and to swap the `modifier=planetary_computer.sign_inplace`
line for CDSE's OAuth token handling. Everything downstream (stages
2–4) is provider-agnostic since it only touches the STAC item list.

## What each output feeds

- `field_timeseries.parquet` → the classifier input from DMP §4
  (join against `ground_truth_operations.csv` on `field_id` + date
  window).
- `datacube_indices.zarr` → kept around for spatial QC and for
  re-running zonal stats if field boundaries change; not needed for
  day-to-day classifier work once the parquet table exists.

"""
Stage 3 — compute spectral indices.

Reads the raw band cube lazily (Dask-backed, nothing loaded into
memory until written) and writes a combined store containing BOTH the
raw bands and the derived indices — one place to open for classifier
input, visual QC, or reprocessing, at the cost of duplicating the raw
bands' storage against datacube_raw.zarr (cheap given the box's 19TB).

Run:
    python scripts/03_compute_indices.py
"""
from __future__ import annotations

import xarray as xr
from dask.distributed import Client, LocalCluster

from utils import data_path, get_logger, load_config

log = get_logger("03_compute_indices")


def compute_indices(ds: xr.Dataset) -> xr.Dataset:
    b02, b03, b04 = ds["B02"], ds["B03"], ds["B04"]
    b05, b08, b11 = ds["B05"], ds["B08"], ds["B11"]

    # Start from a copy of the raw cube so raw bands and derived
    # indices live together in one store — one thing to open for
    # both classification input and visual QC, at the cost of some
    # duplicated storage against datacube_raw.zarr (cheap here).
    out = ds.copy()

    # Vegetation
    out["NDVI"] = (b08 - b04) / (b08 + b04)
    out["EVI"] = 2.5 * (b08 - b04) / (b08 + 6 * b04 - 7.5 * b02 + 1)
    out["SAVI"] = 1.5 * (b08 - b04) / (b08 + b04 + 0.5)
    out["GNDVI"] = (b08 - b03) / (b08 + b03)

    # Red-edge / crop condition
    out["NDRE"] = (b08 - b05) / (b08 + b05)

    # Moisture
    out["NDMI"] = (b08 - b11) / (b08 + b11)
    out["NDWI"] = (b03 - b08) / (b03 + b08)

    # Soil exposure — flags bare/tilled ground, the key tillage-timing signal
    out["BSI"] = ((b11 + b04) - (b08 + b02)) / ((b11 + b04) + (b08 + b02))

    return out


def main():
    cfg = load_config()
    cluster = LocalCluster(
        n_workers=cfg["dask"]["n_workers"],
        threads_per_worker=cfg["dask"]["threads_per_worker"],
        memory_limit=cfg["dask"]["memory_limit"],
    )
    client = Client(cluster)
    log.info(f"Dask dashboard: {client.dashboard_link}")

    raw_path = data_path(cfg, cfg["paths"]["raw_zarr"])
    out_path = data_path(cfg, cfg["paths"]["indices_zarr"])

    log.info(f"Opening {raw_path} (lazy)")
    ds = xr.open_zarr(raw_path, consolidated=True)

    log.info("Computing indices (lazy graph, not yet executed)")
    indices = compute_indices(ds)
    indices = indices.chunk({"time": 1, "x": 1024, "y": 1024})

    log.info(f"Writing indices to {out_path}")
    indices.to_zarr(out_path, mode="w", consolidated=True)

    log.info("Done.")


if __name__ == "__main__":
    main()

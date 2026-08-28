"""
Stage 2 — build the datacube.

This is the stage that replaces GEE's reduceRegions/export workflow.
It reads Cloud-Optimised GeoTIFFs directly (never full SAFE products),
clips to the catchment, cloud-masks using SCL, and writes the result
to a Zarr store on your own storage/compute — no shared quota, no
"user memory limit exceeded".

Memory is bounded by processing ONE YEAR AT A TIME and appending to
the Zarr store, rather than materialising 2020-2025 in memory at once.
This is the direct equivalent of "export per year, not one giant task"
from the GEE workflow, but running on infrastructure you control.

Run (on a compute node — this is the heavy stage):
    python scripts/02_build_datacube.py
"""
from __future__ import annotations

import pandas as pd
import xarray as xr
from dask.distributed import Client, LocalCluster
from odc.stac import load as odc_load
from pystac import ItemCollection

from utils import (
    all_band_names,
    cache_item_assets,
    data_path,
    get_logger,
    load_catchment_geometry,
    load_config,
    load_json,
    strip_existing_signature,
)

log = get_logger("02_build_datacube")


def cloud_mask(ds: xr.Dataset, scl_band: str, keep_classes: list[int]) -> xr.Dataset:
    """Mask every band to NaN wherever SCL is not in keep_classes."""
    scl = ds[scl_band]
    valid = scl.isin(keep_classes)
    masked = ds.where(valid)
    # Keep SCL itself unmasked — useful for later QC (e.g. computing
    # per-field cloud-free observation counts).
    masked[scl_band] = scl
    return masked


def load_year(items, year: int, bands: list[str], resolution: int, crs: str,
              bbox, chunks: dict, cache_dir, provider: str, log,
              download_max_workers: int) -> xr.Dataset:
    year_items = [
        it for it in items
        if it.datetime is not None and it.datetime.year == year
    ]
    if not year_items:
        return None
    log.info(f"  {year}: {len(year_items)} scenes")

    if provider == "planetary_computer":
        import planetary_computer
        # Strip any stale SAS query string first (see docstring on
        # strip_existing_signature), THEN sign — otherwise sign()
        # treats an already-query-stringed href as already-signed and
        # is a silent no-op, leaving yesterday's expired token in place.
        year_items = [
            planetary_computer.sign(strip_existing_signature(it))
            for it in year_items
        ]

    year_items = cache_item_assets(year_items, cache_dir, bands, log,
                                    max_workers=download_max_workers)

    if provider == "planetary_computer":
        import planetary_computer
        # Re-sign AGAIN, right before building the load graph. The
        # first signing (above) only needs to survive the caching
        # download itself; but assets that failed to cache still
        # carry that same signature into odc_load()/to_zarr(), which
        # can run tens of minutes LATER (caching alone took 37 min in
        # one observed run). SAS tokens are short-lived (~45 min
        # observed) — a token that was fine for downloading can be
        # dead by the time the actual Zarr write reads it, which
        # crashes the whole script (no try/except around to_zarr()
        # catches that). Assets already switched to a local file://
        # href are left untouched by sign() (it only matches known
        # blob-storage hostnames), so this is a no-op for anything
        # that cached successfully — it only refreshes the stragglers.
        year_items = [
            planetary_computer.sign(strip_existing_signature(it))
            for it in year_items
        ]

    ds = odc_load(
        year_items,
        bands=bands,
        resolution=resolution,
        crs=crs,
        bbox=bbox,          # (minx, miny, maxx, maxy) in EPSG:4326
        chunks=chunks,
        groupby="solar_day",  # merge same-day tiles into one time step
    )
    return ds


def main():
    cfg = load_config()
    log.info("Starting Dask LocalCluster")
    cluster = LocalCluster(
        n_workers=cfg["dask"]["n_workers"],
        threads_per_worker=cfg["dask"]["threads_per_worker"],
        memory_limit=cfg["dask"]["memory_limit"],
    )
    client = Client(cluster)
    log.info(f"Dask dashboard: {client.dashboard_link}")

    _, bbox_target_crs, bbox_4326 = load_catchment_geometry(cfg)

    items_path = data_path(cfg, cfg["paths"]["stac_items_json"])
    item_collection = ItemCollection.from_dict(load_json(items_path))
    items = list(item_collection)
    log.info(f"Loaded {len(items)} STAC items from {items_path}")

    bands = all_band_names(cfg)
    resolution = cfg["catchment"]["resolution"]
    crs = cfg["catchment"]["target_crs"]
    scl_band = cfg["bands"]["scl"]
    keep_classes = cfg["scl_keep_classes"]

    out_path = data_path(cfg, cfg["paths"]["raw_zarr"])
    cache_dir = data_path(cfg, cfg["paths"]["cog_cache"])
    provider = cfg["stac"]["provider"]

    start_year = pd.Timestamp(cfg["date_range"]["start"]).year
    end_year = pd.Timestamp(cfg["date_range"]["end"]).year

    chunks = {"time": 1, "x": 1024, "y": 1024}

    first_write = True
    for year in range(start_year, end_year + 1):
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            ds = load_year(items, year, bands, resolution, crs, list(bbox_4326),
                           chunks, cache_dir, provider, log,
                           cfg["download"]["max_workers"])
            if ds is None:
                log.info(f"  {year}: no scenes, skipping")
                break

            ds = cloud_mask(ds, scl_band, keep_classes)
            ds = ds.chunk({"time": 1, "x": 1024, "y": 1024})

            log.info(f"  {year}: writing to {out_path} (attempt {attempt}/{max_attempts})")
            try:
                if first_write:
                    ds.to_zarr(out_path, mode="w", consolidated=True)
                    first_write = False
                else:
                    ds.to_zarr(out_path, mode="a", append_dim="time", consolidated=True)
                del ds
                client.restart()
                break  # success — move on to the next year
            except Exception as e:
                log.warning(f"  {year}: write failed on attempt {attempt}/{max_attempts}: {e}")
                del ds
                client.restart()  # clear out any half-finished worker state
                if attempt == max_attempts:
                    log.error(f"  {year}: giving up after {max_attempts} attempts — "
                              "stopping rather than silently skipping a year")
                    raise
                log.info(f"  {year}: retrying — re-caching is cheap now (most "
                         "assets already local), and remaining remote assets "
                         "get a freshly-signed token timed right before the "
                         "retry's write, not left over from the failed attempt.")

    log.info(f"Datacube complete: {out_path}")
    log.info(
        "Sanity check: open with `xr.open_zarr(path)` and confirm the "
        "'time' dimension length matches your expected scene count, and "
        "spot-check a cloud-free summer date visually before moving on."
    )


if __name__ == "__main__":
    main()

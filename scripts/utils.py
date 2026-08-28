"""Shared helpers used by every stage of the pipeline."""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path

import geopandas as gpd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_config(config_path: str | Path = None) -> dict:
    """Load config.yaml from the repo root (or a custom path)."""
    path = Path(config_path) if config_path else REPO_ROOT / "config.yaml"
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return cfg


def resolve_path(cfg: dict, relative: str) -> Path:
    """Resolve a path from config.yaml relative to the repo root, and
    make sure its parent directory exists (harmless if it already does)."""
    p = REPO_ROOT / relative
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def data_path(cfg: dict, relative: str) -> Path:
    """Resolve a path relative to cfg['data_root'] (e.g. /data/ea667_...),
    NOT the repo checkout. Use this for anything raster/vector-sized —
    the repo's home-directory checkout doesn't have room for it.
    Creates the parent directory if needed."""
    p = Path(cfg["data_root"]) / relative
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger(name)


def load_catchment_geometry(cfg: dict):
    """Load the catchment boundary, reproject to target CRS, return
    (geodataframe, bbox_in_target_crs, geometry_for_clipping)."""
    boundary_path = data_path(cfg, cfg["catchment"]["boundary_path"])
    if not boundary_path.exists():
        raise FileNotFoundError(
            f"Catchment boundary not found at {boundary_path}. "
            "Put your catchment polygon there before running the pipeline "
            "(update catchment.boundary_path in config.yaml if you'd rather "
            "keep it somewhere else)."
        )
    gdf = gpd.read_file(boundary_path)
    target_crs = cfg["catchment"]["target_crs"]
    gdf_target = gdf.to_crs(target_crs)
    # bbox also needed in EPSG:4326 for the STAC search itself
    gdf_4326 = gdf.to_crs("EPSG:4326")
    return gdf_target, gdf_target.total_bounds, gdf_4326.total_bounds


def all_band_names(cfg: dict) -> list[str]:
    return cfg["bands"]["ten_m"] + cfg["bands"]["twenty_m"] + [cfg["bands"]["scl"]]


def cache_item_assets(items, cache_dir, bands: list[str], log, max_workers: int = 16) -> list:
    """Download each requested band asset to a local cache dir if it
    isn't already there, and rewrite the item's asset href to point at
    the local file instead of the remote COG.

    This is a persistent, on-disk cache — unlike GDAL's VSICURL cache
    (which is in-memory and per-process), files written here survive
    between runs. Two direct benefits:
      1. Re-running stage 2 (retry after a failure, or extending the
         date range later) never re-downloads a scene you already have.
      2. Planetary Computer's signed URLs (SAS tokens) expire — once a
         band is cached locally, expiry no longer matters for it.

    Downloads the full COG per band (not windowed to the catchment),
    so this trades some extra bandwidth/storage on the first run for
    being able to reprocess with a different bbox or resolution later
    without touching the network again.

    Downloads run concurrently via a thread pool (this is network I/O,
    not CPU work, so threads — not Dask workers — are the right tool;
    max_workers is independent of your core count).
    """
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    cache_dir.mkdir(parents=True, exist_ok=True)

    def download_one(item, band, asset, local_path, max_retries: int = 3):
        import time

        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(asset.href, stream=True, timeout=180)
                resp.raise_for_status()
                # Write to a temp file then rename, so a killed job (tmux
                # session dying, box rebooting) never leaves a half-written
                # file that looks "cached" but isn't. Include a thread-safe
                # unique suffix so two concurrent downloads for the same
                # nominal path can never collide.
                tmp_path = local_path.with_suffix(f".tmp{os.getpid()}_{threading.get_ident()}")
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                tmp_path.rename(local_path)
                return (item.id, band, local_path, None)
            except Exception as e:
                last_err = e
                if attempt < max_retries:
                    time.sleep(2 ** attempt)  # 2s, 4s, ... backoff between retries
        return (item.id, band, None, str(last_err))

    # Split into "already cached" (instant, no thread needed) and
    # "needs downloading" (goes to the pool).
    to_download = []
    n_cached = 0
    for item in items:
        item_dir = cache_dir / item.id
        item_dir.mkdir(exist_ok=True)
        for band in bands:
            if band not in item.assets:
                continue
            asset = item.assets[band]
            local_path = item_dir / f"{band}.tif"
            if local_path.exists() and local_path.stat().st_size > 0:
                # Use a file:// URI, not a bare path. pystac/odc-stac
                # resolve any href without its own URL scheme as
                # RELATIVE to the item's original STAC URL — a bare
                # "/data/.../SCL.tif" gets silently joined onto
                # Microsoft's domain (producing a URL that looks
                # plausible but 404s). file:// has its own scheme, so
                # it's left untouched by that resolution step.
                asset.href = local_path.resolve().as_uri()
                n_cached += 1
            else:
                to_download.append((item, band, asset, local_path))

    n_downloaded, n_failed = 0, 0
    if to_download:
        # Map item_id -> item so we can find it again to set asset.href
        # once its download finishes (assets are per-item dict entries).
        item_by_id = {it.id: it for it, _, _, _ in to_download}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(download_one, item, band, asset, local_path)
                for item, band, asset, local_path in to_download
            ]
            for fut in as_completed(futures):
                item_id, band, local_path, err = fut.result()
                if err is None:
                    item_by_id[item_id].assets[band].href = local_path.resolve().as_uri()
                    n_downloaded += 1
                else:
                    log.warning(f"Failed to cache {item_id}/{band}: {err} "
                                "— falling back to the remote URL for this asset")
                    n_failed += 1

    log.info(f"  cache: {n_cached} already local, {n_downloaded} newly "
             f"downloaded ({max_workers} concurrent), {n_failed} failed "
             "(using remote fallback)")
    return items


def strip_existing_signature(item):
    """Remove any existing SAS query string (?st=...&se=...&sig=...) from
    every asset href on this item, in place, and return it.

    Needed because planetary_computer.sign() appears to treat an href
    that already has query parameters as "already signed" and passes
    it through unchanged — which means calling sign() on items loaded
    from a stage-1 JSON file (saved with tokens baked in, possibly a
    day or more stale) silently does nothing. Stripping first forces
    a genuinely fresh signature every time, regardless of the exact
    internal logic sign() uses to decide "already signed".
    """
    from urllib.parse import urlsplit, urlunsplit

    for asset in item.assets.values():
        parts = urlsplit(asset.href)
        if parts.query:
            asset.href = urlunsplit((parts.scheme, parts.netloc, parts.path, "", parts.fragment))
    return item


def save_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f)


def load_json(path: Path):
    with open(path) as f:
        return json.load(f)

"""
Stage 1 — STAC search.

Finds every Sentinel-2 L2A scene intersecting the catchment boundary and
date range, pre-filtered by scene-level cloud cover. Cheap and fast —
this only touches metadata, no imagery is read yet. Saves the item
list to disk so later stages don't have to re-query.

Run:
    python scripts/01_stac_search.py
"""
from __future__ import annotations

import pystac_client
import planetary_computer

from utils import get_logger, load_config, load_catchment_geometry, save_json, data_path

log = get_logger("01_stac_search")

STAC_ENDPOINTS = {
    "planetary_computer": "https://planetarycomputer.microsoft.com/api/stac/v1",
    # Copernicus Dataspace: needs `pip install cdsetool` or OAuth token
    # handling for asset access; STAC search endpoint if you switch:
    "copernicus_dataspace": "https://catalogue.dataspace.copernicus.eu/stac",
}


def main():
    cfg = load_config()
    provider = cfg["stac"]["provider"]
    if provider != "planetary_computer":
        raise NotImplementedError(
            "This script is written for Planetary Computer. Copernicus "
            "Dataspace needs OAuth asset signing — see the README for "
            "notes on swapping providers if you need scenes PC doesn't have."
        )

    _, bbox_target_crs, bbox_4326 = load_catchment_geometry(cfg)
    log.info(f"Catchment bbox (EPSG:4326): {list(bbox_4326)}")

    catalog = pystac_client.Client.open(
        STAC_ENDPOINTS[provider],
        modifier=planetary_computer.sign_inplace,
    )

    date_range = f"{cfg['date_range']['start']}/{cfg['date_range']['end']}"
    cloud_pct = cfg["stac"]["max_cloud_cover_pct"]

    log.info(
        f"Searching {cfg['stac']['collection']} | {date_range} | "
        f"cloud < {cloud_pct}%"
    )

    search = catalog.search(
        collections=[cfg["stac"]["collection"]],
        bbox=list(bbox_4326),
        datetime=date_range,
        query={"eo:cloud_cover": {"lt": cloud_pct}},
    )

    items = list(search.item_collection())
    log.info(f"Found {len(items)} scenes matching the filter.")

    if len(items) == 0:
        log.warning(
            "Zero scenes found — check the boundary file's CRS/geometry "
            "and the date range before continuing."
        )

    # Save as a STAC ItemCollection (self-describing JSON, easy to inspect
    # and re-load in stage 2 without hitting the API again).
    out_path = data_path(cfg, cfg["paths"]["stac_items_json"])
    from pystac import ItemCollection

    save_json(ItemCollection(items).to_dict(), out_path)
    log.info(f"Saved item list to {out_path}")

    # Quick per-year summary so you can sanity-check coverage before
    # spending compute building the cube.
    from collections import Counter

    year_counts = Counter(item.datetime.year for item in items)
    for year in sorted(year_counts):
        log.info(f"  {year}: {year_counts[year]} scenes")


if __name__ == "__main__":
    main()

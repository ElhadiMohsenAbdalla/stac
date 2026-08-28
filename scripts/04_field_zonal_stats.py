"""
Stage 4 — field-level zonal statistics.

This is the direct equivalent of GEE's reduceRegions() step: collapse
the raster cube down to one row per field per date per variable. Once
this runs, you never need the multi-GB cube for day-to-day analysis —
just this table (tens of MB), which is what feeds the classifier and,
eventually, the SWAT+ management generation script.

Processes one time step at a time to keep memory flat regardless of
how many dates are in the cube — same principle as the yearly chunking
in stage 2.

Run:
    python scripts/04_field_zonal_stats.py
"""
from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from rasterstats import zonal_stats
from tqdm import tqdm

from utils import data_path, get_logger, load_config

log = get_logger("04_field_zonal_stats")


def main():
    cfg = load_config()
    indices_path = data_path(cfg, cfg["paths"]["indices_zarr"])
    fields_path = data_path(cfg, cfg["paths"]["field_polygons"])
    field_id_col = cfg["paths"]["field_id_column"]
    out_path = data_path(cfg, cfg["paths"]["zonal_stats_out"])

    log.info(f"Opening indices cube: {indices_path}")
    ds = xr.open_zarr(indices_path, consolidated=True)

    log.info(f"Loading field polygons: {fields_path}")
    fields = gpd.read_file(fields_path)
    if field_id_col not in fields.columns:
        raise ValueError(
            f"'{field_id_col}' column not found in {fields_path}. "
            f"Columns present: {list(fields.columns)}. Fix the field "
            "boundary file or update paths.field_id_column in config.yaml — "
            "and keep this ID scheme consistent with your ground-truth "
            "table and SWAT+ HRUs, as noted in the DMP."
        )
    fields = fields.to_crs(ds.rio.crs if hasattr(ds, "rio") else cfg["catchment"]["target_crs"])

    variables = [v for v in ds.data_vars if v != "SCL"]
    times = pd.to_datetime(ds["time"].values)

    log.info(f"{len(fields)} fields x {len(times)} dates x {len(variables)} variables")

    records = []
    for t_idx, t in enumerate(tqdm(times, desc="dates")):
        slice_ds = ds.isel(time=t_idx).compute()  # pull just this one time step into memory
        affine = slice_ds.rio.transform() if hasattr(slice_ds, "rio") else None

        for var in variables:
            arr = slice_ds[var].values.astype("float64")
            if affine is None:
                # Fallback: build affine from x/y coords if rioxarray
                # accessor isn't registered on this array.
                x = slice_ds["x"].values
                y = slice_ds["y"].values
                res_x = x[1] - x[0]
                res_y = y[1] - y[0]
                from rasterio.transform import from_origin
                affine = from_origin(x[0] - res_x / 2, y[0] - res_y / 2, res_x, -res_y)

            stats = zonal_stats(
                fields,
                arr,
                affine=affine,
                stats=["mean", "count"],
                nodata=np.nan,
                all_touched=False,
            )
            for field_id, s in zip(fields[field_id_col], stats):
                records.append(
                    {
                        "field_id": field_id,
                        "date": t,
                        "variable": var,
                        "mean": s["mean"],
                        "n_valid_pixels": s["count"],
                    }
                )

    df = pd.DataFrame.from_records(records)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    log.info(f"Wrote {len(df):,} rows to {out_path}")
    log.info(
        "Next: pivot this long table to wide (one column per variable) "
        "and join against ground_truth_operations.csv on field_id + date "
        "window, per the DMP §4 classifier workflow."
    )


if __name__ == "__main__":
    main()

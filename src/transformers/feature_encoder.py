"""Feature encoding for item & user feature parquet exports.

Produces:
  - item_features.parquet: item_id, category_id, category_code, brand, price,
    popularity, is_cold (zero-history flag), encoded categorical indexes.
  - user_features.parquet: user_id, age_band, geo_province, device_category,
    customer_ltv, interaction_count, encoded categorical indexes.

Encoders are ordinal maps persisted alongside the parquet for downstream
model consumption. Vectorized with Polars.
"""

import json
from pathlib import Path
from typing import Any

import polars as pl


def _ordinal_encode(
    df: pl.DataFrame, col: str, out_col: str, mapping: dict[str, int] | None = None
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Ordinal-encode a string column; returns frame + mapping."""
    if mapping is None:
        cats = df[col].drop_nulls().unique().sort().to_list()
        mapping = {c: i + 1 for i, c in enumerate(cats)}  # 0 reserved for unknown/null
    expr = (
        pl.when(pl.col(col).is_null())
        .then(pl.lit(0))
        .otherwise(pl.col(col).replace_strict(mapping, default=0))
        .alias(out_col)
    )
    return df.with_columns(expr), mapping


def build_item_features(
    events: pl.DataFrame,
    cold_item_ids: set[str] | None,
    out_path: str | Path,
) -> tuple[Path, dict[str, Any]]:
    """Build item_features.parquet with ordinal-encoded categorical indexes."""
    popularity = (
        events.group_by("item_id")
        .agg(pl.len().alias("popularity"))
    )
    meta = (
        events.group_by("item_id")
        .agg(
            pl.col("category_id").first(),
            pl.col("category_code").first(),
            pl.col("brand").first(),
            pl.col("price").mean().alias("price"),
        )
    )
    items = meta.join(popularity, on="item_id", how="left")
    items = items.with_columns(
        pl.lit(False).alias("is_cold")
    )
    if cold_item_ids:
        items = items.with_columns(
            pl.col("item_id").is_in(list(cold_item_ids)).alias("is_cold")
        )

    items, cat_map = _ordinal_encode(items, "category_code", "category_code_idx")
    items, brand_map = _ordinal_encode(items, "brand", "brand_idx")
    items, catid_map = _ordinal_encode(items, "category_id", "category_id_idx")

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    items.write_parquet(out_path)

    maps = {
        "category_code_idx": cat_map,
        "brand_idx": brand_map,
        "category_id_idx": catid_map,
    }
    map_path = out_path.with_suffix(".encoders.json")
    map_path.write_text(json.dumps(maps))
    return out_path, maps


def build_user_features(
    events: pl.DataFrame,
    out_path: str | Path,
) -> tuple[Path, dict[str, Any]]:
    """Build user_features.parquet with ordinal-encoded categorical indexes."""
    users = (
        events.group_by(pl.col("user_id"))
        .agg(
            pl.col("age_band").first(),
            pl.col("geo_province").first(),
            pl.col("device_category").first(),
            pl.col("customer_ltv").first(),
            pl.len().alias("interaction_count"),
        )
    )

    users, age_map = _ordinal_encode(users, "age_band", "age_band_idx")
    users, geo_map = _ordinal_encode(users, "geo_province", "geo_province_idx")
    users, dev_map = _ordinal_encode(users, "device_category", "device_idx")

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    users.write_parquet(out_path)

    maps = {
        "age_band_idx": age_map,
        "geo_province_idx": geo_map,
        "device_idx": dev_map,
    }
    map_path = out_path.with_suffix(".encoders.json")
    map_path.write_text(json.dumps(maps))
    return out_path, maps
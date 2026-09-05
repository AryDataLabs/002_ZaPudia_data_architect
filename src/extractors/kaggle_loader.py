"""Kaggle multi-source loader and schema harmonization.

Reads raw Kaggle behavior + order parquet dumps, normalizes them to the
Unified Data Schema (§1.2), and writes a harmonized intermediate parquet.
Vectorized with Polars; no Python row-level loops on hot paths.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl


_KAGGLE_BEHAVIOR_SCHEMA = pl.Schema(
    {
        "event_time": pl.Datetime("ms"),
        "event_type": pl.Utf8,
        "product_id": pl.Utf8,
        "category_id": pl.Utf8,
        "category_code": pl.Utf8,
        "brand": pl.Utf8,
        "price": pl.Float64,
        "user_id": pl.Utf8,
        "user_session": pl.Utf8,
    }
)

_KAGGLE_ORDER_SCHEMA = pl.Schema(
    {
        "customer_id": pl.Utf8,
        "product_id": pl.Utf8,
        "order_timestamp": pl.Datetime("ms"),
        "order_status": pl.Utf8,
        "price": pl.Float64,
        "brand": pl.Utf8,
        "category": pl.Utf8,
        "region": pl.Utf8,
        "age": pl.Int32,
        "discount": pl.Boolean,
        "ltv": pl.Float64,
    }
)


def _bin_age(age_expr: pl.Expr) -> pl.Expr:
    """Map numeric age into the unified age_band enum."""
    return (
        pl.when(age_expr < 25)
        .then(pl.lit("18-24"))
        .when(age_expr < 35)
        .then(pl.lit("25-34"))
        .when(age_expr < 45)
        .then(pl.lit("35-44"))
        .otherwise(pl.lit("45+"))
    )


def load_kaggle_behavior(
    path_pattern: str,
    event_type_map: dict[str, str],
) -> pl.LazyFrame:
    """Load and harmonize Kaggle behavior logs to the unified schema.

    Backfills ``engagement_time_msec`` is left NULL here; the GA4 join and the
    session dwell backfill (§1.2.3 rule 2) fill it downstream.
    """
    return (
        pl.scan_parquet(path_pattern, schema=_KAGGLE_BEHAVIOR_SCHEMA)
        .with_columns(
            pl.col("event_time").cast(pl.Datetime("ms")).dt.epoch_time_unit("ms").alias("event_time_ms"),
            pl.col("event_type").replace_strict(event_type_map, default=None).alias("event_type"),
            pl.col("price").cast(pl.Float64),
            pl.col("user_id").cast(pl.Utf8).alias("user_id"),
            pl.lit(None, dtype=pl.Utf8).alias("user_pseudo_id"),
            pl.lit(None, dtype=pl.Int64).alias("engagement_time_msec"),
            pl.lit(None, dtype=pl.Utf8).alias("device_category"),
            pl.lit(None, dtype=pl.Utf8).alias("geo_province"),
            pl.lit(False, dtype=pl.Boolean).alias("discount_flag"),
            pl.lit(None, dtype=pl.Utf8).alias("fulfillment_status"),
            pl.lit(None, dtype=pl.Float64).alias("customer_ltv"),
            pl.lit(None, dtype=pl.Utf8).alias("age_band"),
        )
        .select(
            pl.col("event_time_ms").alias("event_time"),
            pl.col("event_type"),
            pl.col("user_id"),
            pl.col("user_pseudo_id"),
            pl.col("product_id").alias("item_id"),
            pl.col("category_id"),
            pl.col("category_code"),
            pl.col("brand"),
            pl.col("price"),
            pl.col("user_session"),
            pl.col("engagement_time_msec"),
            pl.col("device_category"),
            pl.col("geo_province"),
            pl.col("discount_flag"),
            pl.col("fulfillment_status"),
            pl.col("customer_ltv"),
            pl.col("age_band"),
        )
    )


def load_kaggle_orders(
    path_pattern: str,
    region_to_province: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """Load Kaggle order/LTV sources and project to a purchase-event stream.

    Each order row becomes a single ``purchase`` event enriched with LTV,
    fulfillment status, discount flag, age band and province.
    """
    region_to_province = region_to_province or {}
    return (
        pl.scan_parquet(path_pattern, schema=_KAGGLE_ORDER_SCHEMA)
        .with_columns(
            pl.col("order_timestamp").cast(pl.Datetime("ms")).dt.epoch_time_unit("ms").alias("event_time"),
            pl.lit("purchase").alias("event_type"),
            pl.col("customer_id").cast(pl.Utf8).alias("user_id"),
            pl.lit(None, dtype=pl.Utf8).alias("user_pseudo_id"),
            pl.col("product_id").cast(pl.Utf8).alias("item_id"),
            pl.lit(None, dtype=pl.Utf8).alias("category_id"),
            pl.col("category").alias("category_code"),
            pl.col("brand"),
            pl.col("price").cast(pl.Float64).alias("price"),
            pl.lit(None, dtype=pl.Utf8).alias("user_session"),
            pl.lit(None, dtype=pl.Int64).alias("engagement_time_msec"),
            pl.lit(None, dtype=pl.Utf8).alias("device_category"),
            pl.col("region").replace_strict(region_to_province, default=None).alias("geo_province"),
            pl.col("discount").alias("discount_flag"),
            pl.col("order_status").alias("fulfillment_status"),
            pl.col("ltv").cast(pl.Float64).alias("customer_ltv"),
            _bin_age(pl.col("age")).alias("age_band"),
        )
        .select(
            "event_time", "event_type", "user_id", "user_pseudo_id", "item_id",
            "category_id", "category_code", "brand", "price", "user_session",
            "engagement_time_msec", "device_category", "geo_province",
            "discount_flag", "fulfillment_status", "customer_ltv", "age_band",
        )
    )


def harmonize_kaggle(
    behavior_pattern: str,
    order_pattern: str,
    event_type_map: dict[str, str],
    region_to_province: dict[str, str] | None = None,
    out_path: str | Path,
) -> Path:
    """Concatenate behavior + order streams into one harmonized parquet file."""
    behavior = load_kaggle_behavior(behavior_pattern, event_type_map)
    orders = load_kaggle_orders(order_pattern, region_to_province)
    unified = pl.concat([behavior.collect_schema(), orders.collect_schema()])  # schema align check
    # concat the lazy frames directly (schemas are identical by construction)
    combined = pl.concat([behavior, orders], how="vertical_relaxed")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.sink_parquet(out_path)
    _ = unified  # noqa: F841
    return out_path
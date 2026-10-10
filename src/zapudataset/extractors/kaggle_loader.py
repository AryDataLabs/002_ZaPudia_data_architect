#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.4"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-10-05"

"""
Kaggle multi-source loader and schema harmonization.
Reads raw Kaggle behavior + order parquet dumps, normalizes them to the
Unified Data Schema (§1.2), and writes a harmonized intermediate parquet.
Vectorized with Polars; no Python row-level loops on hot paths.
"""

import polars as pl
from pathlib  import Path
from glob     import glob

_KAGGLE_BEHAVIOR_SCHEMA = pl.Schema(
    {"event_time"     : pl.Datetime("ms"),
     "event_type"     : pl.Utf8,
     "product_id"     : pl.Utf8,
     "category_id"    : pl.Utf8,
     "category_code"  : pl.Utf8,
     "brand"          : pl.Utf8,
     "price"          : pl.Float64,
     "user_id"        : pl.Utf8,
     "user_session"   : pl.Utf8,})

_KAGGLE_ORDER_SCHEMA = pl.Schema(
    {"customer_id"    : pl.Utf8,
     "product_id"     : pl.Utf8,
     "order_timestamp": pl.Datetime("ms"),
     "order_status"   : pl.Utf8,
     "price"          : pl.Float64,
     "brand"          : pl.Utf8,
     "category"       : pl.Utf8,
     "region"         : pl.Utf8,
     "age"            : pl.Int32,
     "discount"       : pl.Boolean,
     "ltv"            : pl.Float64,})


def _bin_age(age_expr: pl.Expr) -> pl.Expr:
    """Map numeric age into the unified age_band enum."""
    return (pl.when(age_expr < 25)
            .then(pl.lit("18-24"))
            .when(age_expr < 35)
            .then(pl.lit("25-34"))
            .when(age_expr < 45)
            .then(pl.lit("35-44"))
            .otherwise(pl.lit("45+")))


def load_kaggle_behavior(
    path_pattern: str,
    event_type_map: dict[str, str],
) -> pl.LazyFrame:
    """Load and harmonize Kaggle behavior logs to the unified schema."""
    files = glob(path_pattern)
    if not files:
        raise FileNotFoundError(
            f"No parquet files found matching pattern: {path_pattern}\n"
            f"Please download Kaggle datasets first or check your config paths.\n"
            f"Expected location: data/raw/kaggle_behavior/*.parquet"
        )
    
    return (
        pl.scan_parquet(path_pattern)
        .with_columns(
            pl.col("event_time").cast(pl.Utf8).str.to_datetime("%Y-%m-%d %H:%M:%S %Z", strict=False)
            .dt.timestamp(time_unit="ms").alias("event_time_ms"),
            pl.col("event_type").cast(pl.Utf8).replace_strict(event_type_map, default=None).alias("event_type"),
            pl.col("product_id").cast(pl.Utf8).alias("product_id"),
            pl.col("category_id").cast(pl.Utf8).alias("category_id"),
            pl.col("category_code").cast(pl.Utf8).alias("category_code"),
            pl.col("brand").cast(pl.Utf8).alias("brand"),
            pl.col("price").cast(pl.Float64),
            pl.col("user_id").cast(pl.Utf8).alias("user_id"),
            pl.col("user_session").cast(pl.Utf8).alias("user_session"),
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
    """
    Load Kaggle order/LTV sources and project to a purchase-event stream.
    Dynamically checks available columns to support multiple dataset structures.
    """
    files = glob(path_pattern)
    if not files:
        raise FileNotFoundError(
            f"No parquet files found matching pattern: {path_pattern}\n"
            f"Please download Kaggle datasets first or check your config paths.\n"
            f"Expected location: data/raw/kaggle_orders/*.parquet"
        )
    region_to_province = region_to_province or dict()

    lf = pl.scan_parquet(path_pattern)
    cols = lf.collect_schema().names()

    # Dynamic Column Resolution
    time_col = "order_timestamp" if "order_timestamp" in cols else "event_time"
    user_col = "customer_id" if "customer_id" in cols else "user_id"

    cat_code_expr = (
        pl.col("category").cast(pl.Utf8).alias("category_code") if "category" in cols 
        else (pl.col("category_code").cast(pl.Utf8).alias("category_code") if "category_code" in cols else pl.lit(None, dtype=pl.Utf8).alias("category_code"))
    )
    cat_id_expr = pl.col("category_id").cast(pl.Utf8).alias("category_id") if "category_id" in cols else pl.lit(None, dtype=pl.Utf8).alias("category_id")
    session_expr = pl.col("user_session").cast(pl.Utf8).alias("user_session") if "user_session" in cols else pl.lit(None, dtype=pl.Utf8).alias("user_session")
    region_expr = pl.col("region").cast(pl.Utf8).replace_strict(region_to_province, default=None).alias("geo_province") if "region" in cols else pl.lit(None, dtype=pl.Utf8).alias("geo_province")
    discount_expr = pl.col("discount").cast(pl.Boolean).alias("discount_flag") if "discount" in cols else pl.lit(False, dtype=pl.Boolean).alias("discount_flag")
    status_expr = pl.col("order_status").cast(pl.Utf8).alias("fulfillment_status") if "order_status" in cols else pl.lit("completed", dtype=pl.Utf8).alias("fulfillment_status")
    ltv_expr = pl.col("ltv").cast(pl.Float64).alias("customer_ltv") if "ltv" in cols else pl.lit(None, dtype=pl.Float64).alias("customer_ltv")
    age_expr = _bin_age(pl.col("age").cast(pl.Int32)).alias("age_band") if "age" in cols else pl.lit(None, dtype=pl.Utf8).alias("age_band")

    return (
        lf.with_columns(
            pl.col(time_col).cast(pl.Utf8).str.to_datetime(strict=False)
            .dt.timestamp(time_unit="ms").alias("event_time"),
            pl.lit("purchase").alias("event_type"),
            pl.col(user_col).cast(pl.Utf8).alias("user_id"),
            pl.lit(None, dtype=pl.Utf8).alias("user_pseudo_id"),
            pl.col("product_id").cast(pl.Utf8).alias("item_id"),
            cat_id_expr,
            cat_code_expr,
            pl.col("brand").cast(pl.Utf8) if "brand" in cols else pl.lit(None, dtype=pl.Utf8).alias("brand"),
            pl.col("price").cast(pl.Float64).alias("price") if "price" in cols else pl.lit(None, dtype=pl.Float64).alias("price"),
            session_expr,
            pl.lit(None, dtype=pl.Int64).alias("engagement_time_msec"),
            pl.lit(None, dtype=pl.Utf8).alias("device_category"),
            region_expr,
            discount_expr,
            status_expr,
            ltv_expr,
            age_expr,
        )
        .select(
            "event_time", 
            "event_type", 
            "user_id", 
            "user_pseudo_id", 
            "item_id",
            "category_id", 
            "category_code", 
            "brand", 
            "price", 
            "user_session",
            "engagement_time_msec", 
            "device_category", 
            "geo_province",
            "discount_flag", 
            "fulfillment_status", 
            "customer_ltv", 
            "age_band",
        )
    )


def harmonize_kaggle(
    behavior_pattern: str,
    order_pattern: str,
    event_type_map: dict[str, str],
    region_to_province: dict[str, str] | None = None,
    out_path: str | Path | None = None,
) -> Path:
    """Concatenate behavior + order streams into one harmonized parquet file."""
    behavior = load_kaggle_behavior(behavior_pattern, event_type_map)
    orders   = load_kaggle_orders(order_pattern, region_to_province)
    combined = pl.concat([behavior, orders], how="vertical_relaxed")
    
    if out_path is None:
        raise ValueError("out_path tidak boleh None")
        
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.sink_parquet(out_path)
    return out_path


if __name__ == '__main__':
    pass
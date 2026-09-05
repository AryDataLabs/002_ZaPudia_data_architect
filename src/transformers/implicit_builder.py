"""Implicit feedback rating engine (DuckDB-powered).

Builds the implicit-score + min-max-normalized pseudo-rating matrix from the
unified event stream using DuckDB SQL over parquet, so multi-gigabyte logs are
processed without memory exhaustion. Implements §3.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


def build_implicit_matrix(
    events_parquet_path: str | Path,
    weights: dict[str, float],
    norm_min: float = 1.0,
    norm_max: float = 5.0,
) -> pl.DataFrame:
    """Execute the DuckDB implicit-rating query and return a Polars frame.

    Columns: user_id, item_id, raw_score, pseudo_rating.
    """
    conn = duckdb.connect()
    try:
        query = f"""
        WITH raw_aggregated AS (
            SELECT
                COALESCE(user_id, user_pseudo_id) AS user_id,
                item_id,
                -- N_view, N_cart, N_buy counts
                COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS n_view,
                COUNT(CASE WHEN event_type = 'cart'     THEN 1 END) AS n_cart,
                COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS n_buy,
                SUM(COALESCE(engagement_time_msec, 0)) AS t_dwell_ms
            FROM read_parquet('{events_parquet_path}')
            WHERE event_type IN ('view', 'cart', 'purchase')
            GROUP BY 1, 2
        ),
        scored AS (
            SELECT
                user_id,
                item_id,
                n_view * {weights['view']}
                  + n_cart * {weights['cart']}
                  + n_buy * {weights['buy']}
                  + LOG2(1 + t_dwell_ms / 1000.0) * {weights['dwell']} AS raw_score
            FROM raw_aggregated
        ),
        bounds AS (
            SELECT MIN(raw_score) AS min_s, MAX(raw_score) AS max_s FROM scored
        )
        SELECT
            s.user_id,
            s.item_id,
            s.raw_score,
            ROUND(
                {norm_min} + ({norm_max} - {norm_min}) *
                (s.raw_score - b.min_s) / NULLIF(b.max_s - b.min_s, 0),
                3
            ) AS pseudo_rating
        FROM scored s, bounds b
        """
        return conn.execute(query).pl()
    finally:
        conn.close()


def write_implicit_matrix(
    events_parquet_path: str | Path,
    out_path: str | Path,
    weights: dict[str, float],
    norm_min: float = 1.0,
    norm_max: float = 5.0,
) -> Path:
    """Build and persist the implicit matrix as partitioned parquet."""
    df = build_implicit_matrix(events_parquet_path, weights, norm_min, norm_max)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    return out_path
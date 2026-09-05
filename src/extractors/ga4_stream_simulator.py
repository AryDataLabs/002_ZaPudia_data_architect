"""GA4 BigQuery export simulator.

Real GA4 data arrives via the BigQuery export. For a reproducible offline
build we *simulate* GA4-shaped event streams from the harmonized Kaggle stream:
we synthesize ``user_pseudo_id`` for anonymous users, ``engagement_time_msec``
dwell, ``device_category`` and ``geo_province``, then project to the GA4
native schema and merge back into the unified stream.
"""
from pathlib import Path

import numpy as np
import polars as pl


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def simulate_ga4_events(
    events: pl.DataFrame,
    seed: int,
    provinces: list[str],
    devices: list[str] = ("mobile", "desktop", "tablet"),
    device_weights: tuple[float, float, float] = (0.78, 0.18, 0.04),
) -> pl.DataFrame:
    """Augment a harmonized event frame with GA4-native telemetry columns.

    - Generates deterministic ``user_pseudo_id`` for rows lacking a ``user_id``.
    - Backfills ``engagement_time_msec`` from the within-session inter-event
      delta, capped at 30 minutes (§1.2.3 rule 2).
    - Samples ``device_category`` and ``geo_province``.
    """
    rng = _rng(seed)
    n = events.height

    # 1. user_pseudo_id for anonymous users (hash of row index for determinism)
    needs_pseudo = events["user_id"].is_null()
    pseudo_ids = np.array(
        [f"pseudo_{abs(hash((seed, i))) % (10**12)}" for i in range(n)],
        dtype=object,
    )
    pseudo_col = pl.when(needs_pseudo).then(pl.Series(pseudo_ids)).otherwise(events["user_pseudo_id"])

    # 2. device + geo
    device_col = pl.Series(
        rng.choice(devices, size=n, p=np.asarray(device_weights) / sum(device_weights))
    )
    # weighted provinces: Jakarta dominant
    prov_weights = np.array([0.32, 0.21, 0.14, 0.13, 0.04, 0.06, 0.07, 0.03])
    prov_weights = prov_weights / prov_weights.sum()
    geo_col = pl.Series(rng.choice(provinces, size=n, p=prov_weights))

    # 3. dwell backfill from session-ordered inter-event delta
    dwell = _backfill_dwell(events, cap_ms=30 * 60 * 1000)

    return events.with_columns(
        pseudo_col.alias("user_pseudo_id"),
        dwell.alias("engagement_time_msec"),
        device_col.alias("device_category"),
        geo_col.alias("geo_province"),
    )


def _backfill_dwell(events: pl.DataFrame, cap_ms: int) -> pl.Series:
    """Dwell = forward gap to next event in same session, capped."""
    sorted_idx = (
        events.with_row_index()
        .sort(["user_session", "event_time"])
        .select("index", "user_session", "event_time")
    )
    # gap to next row within session
    gaps = (
        sorted_idx.with_columns(
            pl.col("event_time").shift(-1).over("user_session").alias("next_t")
        )
        .with_columns(
            (pl.col("next_t") - pl.col("event_time")).alias("gap")
        )
        .with_columns(
            pl.when(pl.col("gap").is_null())
            .then(pl.lit(cap_ms))
            .otherwise(pl.col("gap").clip(0, cap_ms))
            .alias("dwell_ms")
        )
        .sort("index")
        .select("dwell_ms")
    )
    return gaps.to_series()
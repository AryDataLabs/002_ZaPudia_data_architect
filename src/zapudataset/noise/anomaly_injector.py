#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-06"


"""Real-world noise & anomaly injection (§2), vectorized with Polars.

Five injectors, applied in the fixed order defined in §2.6:
  1. schema degradation (item-level NULL of category_code/brand)
  2. cold-start rot (zero-history SKU creation)
  3. out-of-order timestamp jitter
  4. telemetry drops (pre-purchase cart removal)
  5. bot activity flooding

Deterministic given a seed.
"""
from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass(frozen=True)
class NoiseConfig:
    telemetry_drop_rate: float = 0.08
    bot_fraction: float = 0.05
    bot_lambda_per_sec: float = 150.0
    bot_interarrival_min_ms: int = 1
    bot_interarrival_max_ms: int = 10
    oo_lambda_per_sec: float = 0.5
    schema_null_rate: float = 0.25
    cold_start_rate: float = 0.15
    seed: int = 20260831


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


# ---- §2.4 Schema degradation ------------------------------------------------

def inject_schema_degradation(
    events: pl.DataFrame, cfg: NoiseConfig
) -> tuple[pl.DataFrame, set[str]]:
    """Null category_code and brand for cfg.schema_null_rate of items.

    Applied at item level; the same item's metadata is nulled across all its
    events. Returns the degraded frame plus the set of nulled item_ids.
    """
    rng = _rng(cfg.seed)
    item_ids = events["item_id"].unique().to_numpy()
    null_mask = rng.random(len(item_ids)) < cfg.schema_null_rate
    null_items = set(item_ids[null_mask].tolist())

    degraded = events.with_columns(
        pl.when(pl.col("item_id").is_in(list(null_items)))
        .then(None)
        .otherwise(pl.col("category_code"))
        .alias("category_code"),
        pl.when(pl.col("item_id").is_in(list(null_items)))
        .then(None)
        .otherwise(pl.col("brand"))
        .alias("brand"),
    )
    return degraded, null_items


# ---- §2.5 Cold-Start Catalog Rot --------------------------------------------

def partition_cold_items(
    events: pl.DataFrame, cfg: NoiseConfig
) -> tuple[set[str], pl.DataFrame, pl.DataFrame]:
    """Select cfg.cold_start_rate of active items as cold (zero-history).

    Returns (cold_item_ids, warm_events, cold_item_catalog_rows).
    Warm events = events whose item is NOT cold; cold-item events are removed
    from the training stream but retained in the item catalog.
    """
    rng = _rng(cfg.seed + 1)
    active_items = events["item_id"].unique().to_numpy()
    n_cold = int(round(cfg.cold_start_rate * len(active_items)))
    cold_idx = rng.choice(len(active_items), size=n_cold, replace=False)
    cold_items = set(active_items[cold_idx].tolist())

    warm_events = events.filter(~pl.col("item_id").is_in(list(cold_items)))
    cold_catalog = events.filter(pl.col("item_id").is_in(list(cold_items))).unique(
        subset=["item_id"], maintain_order=False
    )
    return cold_items, warm_events, cold_catalog


# ---- §2.3 Out-of-Order Timestamp Jitter -------------------------------------

def inject_out_of_order(events: pl.DataFrame, cfg: NoiseConfig) -> pl.DataFrame:
    """Add exponential jitter to event_time; retain true_event_time for eval."""
    rng = _rng(cfg.seed + 2)
    n = events.height
    # Exponential(lambda) in ms; mean = 1/lambda seconds * 1000
    jitter_ms = rng.exponential(
        scale=1000.0 / cfg.oo_lambda_per_sec, size=n
    ).astype(np.int64)
    # ensure non-negative integer ms
    jitter_ms = np.clip(jitter_ms, 0, None)
    return events.with_columns(
        pl.col("event_time").alias("true_event_time")
    ).with_columns(
        (pl.col("event_time") + pl.Series(jitter_ms)).alias("event_time")
    )


# ---- §2.1 Telemetry Drops (pre-purchase cart) -------------------------------

def inject_telemetry_drops(events: pl.DataFrame, cfg: NoiseConfig) -> pl.DataFrame:
    """Drop cfg.telemetry_drop_rate of cart events that precede a purchase in
    the same (user, item) session.

    The drop is conditioned on a subsequent purchase existing, isolating
    telemetry loss from genuine abandonment (§2.1).
    """
    rng = _rng(cfg.seed + 3)
    # identify cart events with a later purchase in same (user_id, item_id, user_session)
    enriched = events.with_columns(
        pl.col("event_type").alias("_et"),
        pl.col("event_time").alias("_t"),
    )
    # for each cart event, max purchase time in same group
    cart_with_purchase = (
        enriched.sort(["user_id", "item_id", "user_session", "event_time"])
        .with_columns(
            pl.col("event_time")
            .filter(pl.col("event_type") == "purchase")
            .max()
            .over(["user_id", "item_id", "user_session"])
            .alias("_max_purchase_t")
        )
        .filter(
            (pl.col("event_type") == "cart")
            & pl.col("_max_purchase_t").is_not_null()
            & (pl.col("_max_purchase_t") > pl.col("event_time"))
        )
    )
    # vectorized dropout mask
    n_cart = cart_with_purchase.height
    if n_cart == 0:
        return events
    drop_mask = rng.random(n_cart) < cfg.telemetry_drop_rate
    drop_keys = (
        cart_with_purchase.with_columns(pl.Series(drop_mask).alias("_drop"))
        .filter(pl.col("_drop"))
        .select(["user_id", "item_id", "user_session", "event_time"])
    )
    if drop_keys.height == 0:
        return events
    # anti-join: remove exactly these cart events
    return events.join(
        drop_keys,
        on=["user_id", "item_id", "user_session", "event_time"],
        how="anti",
    )


# ---- §2.2 Bot Activity Flooding --------------------------------------------

def inject_bot_flood(
    events: pl.DataFrame, cfg: NoiseConfig, provinces: list[str]
) -> pl.DataFrame:
    """Inject synthetic bot streams with sub-10ms inter-arrival times.

    Bot target items sampled from a power-law popularity distribution.
    Bots are anonymous (user_id NULL, user_pseudo_id set) so they must be
    detected behaviorally, not by flag.
    """
    rng = _rng(cfg.seed + 4)
    n_human = events.height
    n_bot_events = int(cfg.bot_fraction * n_human)
    if n_bot_events == 0:
        return events

    # power-law item popularity for bot targets
    pop = (
        events.group_by("item_id")
        .agg(pl.len().alias("p"))
        .with_columns((pl.col("p") ** 0.75).alias("weight"))
    )
    weights = pop["weight"].to_numpy()
    weights = weights / weights.sum()
    items = pop["item_id"].to_numpy()
    bot_items = rng.choice(items, size=n_bot_events, p=weights)

    # number of bots and events-per-bot
    n_bots = max(1, n_bot_events // 200)
    events_per_bot = n_bot_events // n_bots
    pseudo_ids = [
        f"bot_{cfg.seed}_{i}" for i in range(n_bots)
    ]

    rows: list[dict] = []
    base_t = int(events["event_time"].min())
    for b in range(n_bots):
        pid = pseudo_ids[b]
        ne = events_per_bot if b < n_bots - 1 else n_bot_events - b * events_per_bot
        # truncated exponential inter-arrival in [min,max] ms
        iats = rng.exponential(
            scale=1000.0 / cfg.bot_lambda_per_sec, size=ne
        )
        iats = np.clip(iats, cfg.bot_interarrival_min_ms, cfg.bot_interarrival_max_ms).astype(np.int64)
        ts = base_t + np.cumsum(iats)
        item_slice = bot_items[b * events_per_bot : b * events_per_bot + ne]
        province = rng.choice(provinces)
        for t, it in zip(ts, item_slice):
            rows.append(
                {
                    "event_time": int(t),
                    "event_type": "view",
                    "user_id": None,
                    "user_pseudo_id": pid,
                    "item_id": str(it),
                    "category_id": None,
                    "category_code": None,
                    "brand": None,
                    "price": None,
                    "user_session": None,
                    "engagement_time_msec": rng.integers(1, 10),
                    "device_category": "desktop",
                    "geo_province": province,
                    "discount_flag": False,
                    "fulfillment_status": None,
                    "customer_ltv": None,
                    "age_band": None,
                    "true_event_time": int(t),
                }
            )
    bot_df = pl.DataFrame(rows)
    return pl.concat([events, bot_df], how="vertical_relaxed")


# ---- Orchestrator (§2.6 ordering) ------------------------------------------

def inject_all(
    events: pl.DataFrame,
    cfg: NoiseConfig,
    provinces: list[str],
) -> tuple[pl.DataFrame, set[str], set[str]]:
    """Apply all five injections in the §2.6 order.

    Returns (corrupted_events, cold_item_ids, null_item_ids).
    """
    # 1. schema degradation
    degraded, null_items = inject_schema_degradation(events, cfg)
    # 2. cold-start rot
    cold_items, warm_events, _ = partition_cold_items(degraded, cfg)
    # 3. out-of-order jitter
    jittered = inject_out_of_order(warm_events, cfg)
    # 4. telemetry drops
    dropped = inject_telemetry_drops(jittered, cfg)
    # 5. bot flood
    corrupted = inject_bot_flood(dropped, cfg, provinces)
    return corrupted, cold_items, null_items
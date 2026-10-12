#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.2"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-10-12"


"""
Real-world noise & anomaly injection, vectorized with Polars.
Five injectors, applied in the fixed order:
  1. schema degradation (item-level NULL of category_code/brand)
  2. cold-start rot (zero-history SKU creation)
  3. out-of-order timestamp jitter
  4. telemetry drops (pre-purchase cart removal)
  5. bot activity flooding
Deterministic given a seed.
"""

from   dataclasses import dataclass
import numpy  as np
import polars as pl


@dataclass(frozen = True)
class NoiseConfig:
    telemetry_drop_rate     : float = 0.08
    bot_fraction            : float = 0.05
    bot_lambda_per_sec      : float = 150.0
    bot_interarrival_min_ms : int   = 1
    bot_interarrival_max_ms : int   = 10
    oo_lambda_per_sec       : float = 0.5
    schema_null_rate        : float = 0.25
    cold_start_rate         : float = 0.15
    seed                    : int   = 20260831

def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)

def inject_schema_degradation(
        events : pl.DataFrame, 
        cfg    : NoiseConfig,
    ) -> tuple[pl.DataFrame, set[str]]:
    """
    Null category_code and brand for cfg.schema_null_rate of items.
    """
    if events.height == 0:
        return events, set()

    rng        = _rng(cfg.seed)
    item_ids   = events["item_id"].unique().to_numpy()
    null_mask  = rng.random(len(item_ids)) < cfg.schema_null_rate
    null_items = set(item_ids[null_mask].tolist())
    
    cols = events.columns
    exprs = []
    if "category_code" in cols:
        exprs.append(
            pl.when(pl.col("item_id").is_in(list(null_items)))
            .then(None)
            .otherwise(pl.col("category_code"))
            .alias("category_code")
        )
    if "brand" in cols:
        exprs.append(
            pl.when(pl.col("item_id").is_in(list(null_items)))
            .then(None)
            .otherwise(pl.col("brand"))
            .alias("brand")
        )
        
    degraded = events.with_columns(exprs) if exprs else events
    return degraded, null_items

def partition_cold_items(
        events : pl.DataFrame, 
        cfg    : NoiseConfig,
    ) -> tuple[set[str], pl.DataFrame, pl.DataFrame]:
    """
    Select cfg.cold_start_rate of active items as cold (zero-history).
    """
    if events.height == 0:
        return set(), events, events

    rng          = _rng(cfg.seed + 1)
    active_items = events["item_id"].unique().to_numpy()
    if len(active_items) == 0:
        return set(), events, events

    n_cold       = int(round(cfg.cold_start_rate * len(active_items)))
    cold_idx     = rng.choice(len(active_items), size = n_cold, replace = False)
    cold_items   = set(active_items[cold_idx].tolist())
    
    warm_events  = events.filter(~pl.col("item_id").is_in(list(cold_items)))
    cold_catalog = events.filter(
        pl.col("item_id").is_in(list(cold_items))
    ).unique(
        subset = ["item_id"] if "item_id" in events.columns else None, 
        maintain_order = False,
    )
    return cold_items, warm_events, cold_catalog

def inject_out_of_order(
        events : pl.DataFrame, 
        cfg    : NoiseConfig,
    ) -> pl.DataFrame:
    """
    Add exponential jitter to event_time; retain true_event_time for eval.
    Supports both numeric (timestamp ms) and Datetime dtypes.
    """
    n = events.height
    if n == 0:
        return events

    rng       = _rng(cfg.seed + 2)
    jitter_ms = rng.exponential(
                scale = 1000.0 / cfg.oo_lambda_per_sec,
                size  = n).astype(np.int64)
    jitter_ms = np.clip(jitter_ms, 0, None)

    time_dtype = events.schema["event_time"]
    if time_dtype in (pl.Datetime, pl.Date):
        time_expr = pl.col("event_time") + pl.duration(milliseconds=pl.Series(jitter_ms))
    else:
        time_expr = pl.col("event_time") + pl.Series(jitter_ms)

    return events.with_columns(
        pl.col("event_time").alias("true_event_time")
    ).with_columns(
        time_expr.alias("event_time")
    )

def inject_telemetry_drops(
        events : pl.DataFrame, 
        cfg    : NoiseConfig,
    ) -> pl.DataFrame:
    """
    Drop cfg.telemetry_drop_rate of cart events that precede a purchase in
    the same (user, item) session.
    Uses row indexing to avoid issues with null keys in anti-joins.
    """
    if events.height == 0:
        return events

    rng = _rng(cfg.seed + 3)
    indexed = events.with_row_index("_row_id")
    
    carts = (
        indexed.with_columns(
            pl.when(pl.col("event_type") == "purchase")
            .then(pl.col("event_time"))
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

    n_cart = carts.height
    if n_cart == 0:
        return events

    drop_mask    = rng.random(n_cart) < cfg.telemetry_drop_rate
    drop_row_ids = carts.filter(pl.Series(drop_mask))["_row_id"].to_list()

    if not drop_row_ids:
        return events

    return indexed.filter(~pl.col("_row_id").is_in(drop_row_ids)).drop("_row_id")

def inject_bot_flood(
        events    : pl.DataFrame, 
        cfg       : NoiseConfig, 
        provinces : list[str],
    ) -> pl.DataFrame:
    """
    Inject synthetic bot streams with sub-10ms inter-arrival times.
    Fully vectorized dataframe construction & diagonal concatenation.
    """
    n_human      = events.height
    n_bot_events = int(cfg.bot_fraction * n_human)
    if n_bot_events == 0:
        return events

    rng = _rng(cfg.seed + 4)

    # Item popularity sampling
    pop = (
        events.group_by("item_id")
        .agg(pl.len().alias("p"))
        .with_columns((pl.col("p") ** 0.75).alias("weight"))
    )
    weights   = pop["weight"].to_numpy()
    weights   = weights / weights.sum()
    items     = pop["item_id"].to_numpy()
    bot_items = rng.choice(items, size=n_bot_events, p=weights)

    # Number of bots and event allocation per bot
    n_bots    = max(1, n_bot_events // 200)
    eventsbot = n_bot_events // n_bots
    counts    = np.full(n_bots, eventsbot, dtype=int)
    counts[-1] += n_bot_events - (eventsbot * n_bots)

    pseudo_ids        = np.array([f"bot_{cfg.seed}_{i}" for i in range(n_bots)])
    bot_pseudo_id_arr = np.repeat(pseudo_ids, counts)

    sampled_provinces = rng.choice(provinces, size=n_bots)
    bot_province_arr  = np.repeat(sampled_provinces, counts)

    # Determine base_t according to event_time dtype
    time_dtype = events.schema["event_time"]
    min_t      = events["event_time"].min()

    if time_dtype in (pl.Datetime, pl.Date):
        base_t = int(min_t.timestamp() * 1000) if min_t is not None else 0
    else:
        base_t = int(min_t) if min_t is not None else 0

    # Vectorized inter-arrival times generation
    iats = rng.exponential(scale=1000.0 / cfg.bot_lambda_per_sec, size=n_bot_events)
    iats = np.clip(
        iats, 
        cfg.bot_interarrival_min_ms, 
        cfg.bot_interarrival_max_ms
    ).astype(np.int64)

    # Calculate cumulative timestamp offsets per bot
    ts_list = []
    idx = 0
    for c in counts:
        bot_iats = iats[idx : idx + c]
        ts_list.append(base_t + np.cumsum(bot_iats))
        idx += c
    bot_ts_arr = np.concatenate(ts_list)

    if time_dtype in (pl.Datetime, pl.Date):
        event_time_col = pl.Series(bot_ts_arr).cast(pl.Datetime("ms"))
    else:
        event_time_col = bot_ts_arr

    # Construct bot DataFrame vectorially
    bot_dict = {
        "event_time"          : event_time_col,
        "event_type"          : "view",
        "user_id"             : None,
        "user_pseudo_id"      : bot_pseudo_id_arr,
        "item_id"             : bot_items,
        "user_session"        : None,
        "engagement_time_msec": rng.integers(1, 10, size=n_bot_events),
        "device_category"     : "desktop",
        "geo_province"        : bot_province_arr,
        "discount_flag"       : False,
        "true_event_time"     : event_time_col,
    }

    bot_df = pl.DataFrame(bot_dict)

    # Use diagonal_relaxed to seamlessly handle schema differences
    return pl.concat([events, bot_df], how="diagonal_relaxed")

def inject_all(
        events    : pl.DataFrame,
        cfg       : NoiseConfig,
        provinces : list[str],
    ) -> tuple[pl.DataFrame, set[str], set[str]]:
    """
    Apply all five injections.
    Returns (corrupted_events, cold_item_ids, null_item_ids).
    """
    degraded, null_items       = inject_schema_degradation(events, cfg)
    cold_items, warm_events, _ = partition_cold_items(degraded, cfg)
    jittered                   = inject_out_of_order(warm_events, cfg)
    dropped                    = inject_telemetry_drops(jittered, cfg)
    corrupted                  = inject_bot_flood(dropped, cfg, provinces)
    return corrupted, cold_items, null_items


if __name__ == "__main__":
    pass
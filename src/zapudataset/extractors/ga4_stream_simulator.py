#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-10"


"""
GA4 BigQuery export simulator.
  Real GA4 data arrives via the BigQuery export. For a reproducible offline
  build we *simulate* GA4-shaped event streams from the harmonized Kaggle stream:
  we synthesize ``user_pseudo_id`` for anonymous users, ``engagement_time_msec``
  dwell, ``device_category`` and ``geo_province``, then project to the GA4
  native schema and merge back into the unified stream.
"""

import numpy   as np
import polars  as pl
from pathlib   import Path
from typing    import Tuple
from datetime  import datetime, timedelta
from ..configs import PipeConfig, load_config, logger

def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def _backfill_dwell(
        events: pl.DataFrame, 
        cap_ms: int) -> pl.Series:
    """
    Calculate the engagement duration (dwell_ms) 
    until the next event within the same session. 
    The last event in the session has a value 
    of 0 ms, while the others are clipped to cap_ms.
    """
    return (
    events.with_row_index("orig_idx")
    .sort(["user_session", "event_time"])
    .with_columns(
        pl.col("event_time").shift(-1).over("user_session").alias("next_t"))
    .with_columns((pl.col("next_t") - pl.col("event_time")).alias("gap"))
    .with_columns(
        pl.when(pl.col("gap").is_null())
        .then(pl.lit(0))
        .otherwise(pl.col("gap").clip(0, cap_ms))
        .alias("dwell_ms"))
    .sort("orig_idx")
    .get_column("dwell_ms"))


def simulate_ga4_events(
        events         : pl.DataFrame,
        config         : PipeConfig,
        devices        : Tuple[str, ...] = ("mobile", "desktop", "tablet"),
        device_weights : Tuple[float, float, float] = (0.78, 0.18, 0.04),
    ) -> pl.DataFrame:
    """
    Augment the DataFrame events with GA4 telemetry using the PipeConfig configuration. 
    - Seed is obtained from `config.pipeline.seed`
    - Geo provinces are obtained from `config.geo_provinces`
    - The dwell time cap is obtained from `config.session.gap_minutes` (converted to ms)    """
    seed      = config.pipeline.seed
    provinces = config.geo_provinces
    cap_ms    = config.session.gap_minutes * 60 * 1000
    logger.info("Starting GA4 events simulation | Pipeline: %s v%s | Total events: %d",
                 config.pipeline.name,
                 config.pipeline.version,
                 events.height,)
    if events.is_empty():
        logger.warning("The events DataFrame is empty; aborting the simulation.")
        return events
    if not provinces:
        raise ValueError("The 'geo_provinces' list in PipeConfig cannot be empty.")

    rng             = _rng(seed)
    unique_sessions = events.select("user_session").unique()
    n_sessions      = unique_sessions.height
    logger.info("Terdeteksi %d user_session unik", n_sessions)

    # Generate a deterministic standard GA4 user_pseudo_id (<10 digits>.<10 digits>)
    part1           = rng.integers(1000000000, 9999999999, size = n_sessions)
    part2           = rng.integers(1000000000, 9999999999, size = n_sessions)
    pseudo_ids      = [f"{p1}.{p2}" for p1, p2 in zip(part1, part2)]
    dev_p           = np.asarray(device_weights) / sum(device_weights)
    sampled_devices = rng.choice(list(devices), size=n_sessions, p=dev_p)
    n_prov          = len(provinces)
    if n_prov == 8:
        prov_p = np.array([0.32, 0.21, 0.14, 0.13, 0.04, 0.06, 0.07, 0.03])
    else:
        logger.warning(
        "Number of provinces (%d) != 8; using uniform distribution.", n_prov)
        prov_p   = np.ones(n_prov)
    prov_p       = prov_p / prov_p.sum()
    sampled_geos = rng.choice(provinces, size = n_sessions, p = prov_p)
    session_meta = pl.DataFrame({
                   "user_session": unique_sessions["user_session"],
                   "generated_pseudo_id": pseudo_ids,
                   "device_category": sampled_devices,
                   "geo_province": sampled_geos,})
    logger.debug("Merging metadata sesi ke DataFrame event...")
    events_augmented = events.join(session_meta, on = "user_session", how = "left")

    # Resolusi user_pseudo_id (gunakan yang ada, atau backfill dari generated)
    if "user_pseudo_id" in events_augmented.columns:
        pseudo_expr  = (pl.when(pl.col("user_pseudo_id").is_not_null())
                          .then(pl.col("user_pseudo_id"))
                          .otherwise(pl.col("generated_pseudo_id")))
    else:
        pseudo_expr  = pl.col("generated_pseudo_id")
    events_augmented = events_augmented.with_columns(
                       pseudo_expr.alias("user_pseudo_id")
                       ).drop("generated_pseudo_id")
    logger.debug("Calculating dwell time (cap_ms=%d ms)...", cap_ms)
    dwell_series = _backfill_dwell(events_augmented, cap_ms=cap_ms)
    logger.info("The GA4 event simulation was successfully processed.")
    return events_augmented.with_columns(
           dwell_series.alias("engagement_time_msec"))


if __name__ == "__main__":
    conf_path = Path("./pipeconf.yaml")
    if not conf_path.exists():
        raise FileNotFoundError(f"Config file {conf_path} was not found!")
    cfg: PipeConfig = load_config(str(conf_path), schema = PipeConfig)
    t_base          = datetime(2026, 9, 10, 10, 0, 0)
    dummy_events    = pl.DataFrame({
    "user_session"  : ["sess_001",
                       "sess_001",
                       "sess_001",
                       "sess_002",
                       "sess_002",],
    "event_time"    : [t_base,
                       t_base + timedelta(seconds = 12),
                       t_base + timedelta(seconds = 45),
                       t_base + timedelta(minutes = 10),
                       t_base + timedelta(minutes = 10, seconds = 30),],
    "event_name"    : ["page_view",
                       "add_to_cart",
                       "purchase",
                       "page_view",
                       "select_item",],
    "user_id"       : ["usr_101", "usr_101", "usr_101", None, None],
    "user_pseudo_id": [None, None, None, None, None],
    }).with_columns(pl.col("event_time").dt.timestamp("ms"))
    
    df_result       = simulate_ga4_events(
                      events = dummy_events, 
                      config = cfg)
    logger.warning(f"HASIL SIMULASI GA4 ({cfg.pipeline.name} v{cfg.pipeline.version})")
    logger.warning(df_result.select(
                   "user_session",
                   "event_name",
                   "user_pseudo_id",
                   "device_category",
                   "geo_province",
                   "engagement_time_msec",))


#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-07"

from logging    import getLogger
from typing     import Dict, List
from confreader import load_config, ConfigError
from pydantic   import (BaseModel, 
                        Field, 
                        NonNegativeFloat,
                        PositiveInt)

logger = logging.getLogger("SchemaRead")

class PipelineMeta(BaseModel):
    name             : str
    version          : str
    seed             : int
    output_dir       : str
    intermediate_dir : str
    partition_count  : PositiveInt = Field(
                       description = "Parquet row-group partitioning for DuckDB scan")

class KaggleBehaviorSource(BaseModel):
    path           : str
    event_type_map : Dict[str, str]

class KaggleOrdersSource(BaseModel):
    path: str

class GA4Source(BaseModel):
    path           : str
    event_name_map : Dict[str, str]

class SourcesConfig(BaseModel):
    kaggle_behavior: KaggleBehaviorSource
    kaggle_orders  : KaggleOrdersSource
    ga4_bigquery   : GA4Source

class ImplicitWeights(BaseModel):
    view  : NonNegativeFloat = 1.0
    cart  : NonNegativeFloat = 3.0
    buy   : NonNegativeFloat = 5.0
    dwell : NonNegativeFloat = 0.5

class ImplicitNormalization(BaseModel):
    min_rating      : float = 1.0
    max_rating      : float = 5.0
    alpha_confidence: NonNegativeFloat = Field(
                      default          = 40.0,
                      description      = "iALS confidence scaling")

class NoiseConfig(BaseModel):
    telemetry_drop_rate     : float = Field(ge = 0.0, le = 1.0)
    bot_fraction            : float = Field(ge = 0.0, le = 1.0)
    bot_lambda_per_sec      : NonNegativeFloat
    bot_interarrival_min_ms : PositiveInt
    bot_interarrival_max_ms : PositiveInt
    oo_lambda_per_sec       : NonNegativeFloat
    schema_null_rate        : float = Field(ge = 0.0, le = 1.0)
    cold_start_rate         : float = Field(ge = 0.0, le = 1.0)

class SplitConfig(BaseModel):
    test_fraction           : float = Field(ge=0.0, le=1.0)
    split_by                : str = Field(pattern = "^(user|item|time)$")
    min_user_interactions   : PositiveInt

class SessionConfig(BaseModel):
    gap_minutes: PositiveInt

class LoggingConfig(BaseModel):
    log_file : str = Field(default     = "app.log", 
                           description = "Output log file name")
    level    : str = Field(default     = "INFO",
                           pattern     = "^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")

class PipeConfig(BaseModel):
    pipeline               : PipelineMeta
    logging                : LoggingConfig = Field(default_factory = LoggingConfig)
    sources                : SourcesConfig
    implicit_weights       : ImplicitWeights
    implicit_normalization : ImplicitNormalization
    noise                  : NoiseConfig
    split                  : SplitConfig
    session                : SessionConfig
    geo_provinces          : List[str]
    categories             : Dict[str, List[str]]

def SchemaRead(confpath):
    '''
    Run this for main Schema reading
    '''
    try:
        cfg                              : PipeConfig = load_config(str(confpath), 
                                           schema     = PipeConfig)
        logger.debug(f"Loaded Config     : {cfg.pipeline.name} v{cfg.pipeline.version}")
        logger.debug(f"DuckDB Partitions : {cfg.pipeline.partition_count}")
        logger.debug(f"iALS Alpha        : {cfg.implicit_normalization.alpha_confidence}")
        logger.debug(f"Bot Fraction      : {cfg.noise.bot_fraction * 100}%")
        logger.debug(f"Provinces Target  : {len(cfg.geo_provinces)} Wilayah")
        logger.debug(f"Categories        : {list(cfg.categories.keys())}")
    except ConfigError as e:
        logger.error(f"[CRITICAL] Pipeline Config Invalid:\n{e}")

if __name__ == "__main__":
    conf = "./pipeconf.yaml"
    SchemaRead(conf)
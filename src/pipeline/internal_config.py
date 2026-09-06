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


"""Pipeline configuration management with validation and type safety.

Handles YAML config loading, validation, and provides typed access to
pipeline parameters.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Type-safe pipeline configuration."""
    
    # Pipeline settings
    seed: int
    output_dir: Path
    intermediate_dir: Path
    
    # Source paths
    kaggle_behavior_path: str
    kaggle_orders_path: str
    event_type_map: dict[str, str]
    
    # Geographic data
    geo_provinces: list[str]
    
    # Noise injection parameters
    telemetry_drop_rate: float
    bot_fraction: float
    bot_lambda_per_sec: float
    bot_interarrival_min_ms: int
    bot_interarrival_max_ms: int
    oo_lambda_per_sec: float
    schema_null_rate: float
    cold_start_rate: float
    
    # Train/test split
    test_fraction: float
    min_user_interactions: int
    
    # Implicit feedback weights
    implicit_weights: dict[str, float]
    norm_min: float
    norm_max: float
    
    # Parallelization
    n_jobs: int = -1  # -1 means use all CPUs
    
    @classmethod
    def from_yaml(cls, config_path: str | Path) -> PipelineConfig:
        """Load and validate configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Validated PipelineConfig instance
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid or missing required fields
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        logger.info(f"Loading configuration from {config_path}")
        
        with open(config_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)
        
        try:
            return cls._parse_config(raw_config)
        except KeyError as e:
            raise ValueError(f"Missing required config field: {e}") from e
        except Exception as e:
            raise ValueError(f"Invalid configuration: {e}") from e
    
    @classmethod
    def _parse_config(cls, cfg: dict[str, Any]) -> PipelineConfig:
        """Parse raw config dict into typed PipelineConfig."""
        return cls(
            # Pipeline
            seed=cfg["pipeline"]["seed"],
            output_dir=Path(cfg["pipeline"]["output_dir"]),
            intermediate_dir=Path(cfg["pipeline"]["intermediate_dir"]),
            
            # Sources
            kaggle_behavior_path=cfg["sources"]["kaggle_behavior"]["path"],
            kaggle_orders_path=cfg["sources"]["kaggle_orders"]["path"],
            event_type_map=cfg["sources"]["kaggle_behavior"]["event_type_map"],
            
            # Geographic
            geo_provinces=cfg["geo_provinces"],
            
            # Noise
            telemetry_drop_rate=cfg["noise"]["telemetry_drop_rate"],
            bot_fraction=cfg["noise"]["bot_fraction"],
            bot_lambda_per_sec=cfg["noise"]["bot_lambda_per_sec"],
            bot_interarrival_min_ms=cfg["noise"]["bot_interarrival_min_ms"],
            bot_interarrival_max_ms=cfg["noise"]["bot_interarrival_max_ms"],
            oo_lambda_per_sec=cfg["noise"]["oo_lambda_per_sec"],
            schema_null_rate=cfg["noise"]["schema_null_rate"],
            cold_start_rate=cfg["noise"]["cold_start_rate"],
            
            # Split
            test_fraction=cfg["split"]["test_fraction"],
            min_user_interactions=cfg["split"]["min_user_interactions"],
            
            # Implicit weights
            implicit_weights=cfg["implicit_weights"],
            norm_min=cfg["implicit_normalization"]["min_rating"],
            norm_max=cfg["implicit_normalization"]["max_rating"],
            
            # Parallelization
            n_jobs=cfg.get("n_jobs", -1),
        )
    
    def ensure_directories(self) -> None:
        """Create output and intermediate directories if they don't exist."""
        for directory in [self.output_dir, self.intermediate_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    def validate(self) -> None:
        """Validate configuration parameters.
        
        Raises:
            ValueError: If any parameter is invalid
        """
        if not 0 <= self.test_fraction <= 1:
            raise ValueError(f"test_fraction must be in [0, 1], got {self.test_fraction}")
        
        if self.min_user_interactions < 1:
            raise ValueError(f"min_user_interactions must be >= 1, got {self.min_user_interactions}")
        
        if not 0 <= self.telemetry_drop_rate <= 1:
            raise ValueError(f"telemetry_drop_rate must be in [0, 1], got {self.telemetry_drop_rate}")
        
        if not 0 <= self.bot_fraction <= 1:
            raise ValueError(f"bot_fraction must be in [0, 1], got {self.bot_fraction}")
        
        logger.info("Configuration validation passed")

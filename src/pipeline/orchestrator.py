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


"""Main pipeline orchestrator with class-based architecture.

Provides:
- DatasetPipeline: Main orchestrator class
- Proper logging configuration (console + file)
- Progress tracking and timing
- Error handling and recovery
- Joblib-optimized parallel processing
- Comprehensive build manifest

Usage:
    from pipeline_orchestrator import DatasetPipeline
    
    pipeline = DatasetPipeline(
        config_path="configs/pipeline_config.yaml",
        log_level="INFO"
    )
    manifest = pipeline.run()
"""
from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from noise.anomaly_injector import NoiseConfig
from pipeline_config import PipelineConfig
from pipeline_stages import (
    DataExtractionStage,
    FeatureEngineeringStage,
    ImplicitMatrixStage,
    NoiseInjectionStage,
    TelemetryAugmentationStage,
    TrainTestSplitStage,
)


@dataclass
class PipelineMetrics:
    """Runtime metrics for pipeline execution."""
    stage_durations: dict[str, float]
    total_duration: float
    n_events_raw: int
    n_events_train: int
    n_events_test: int
    n_users_train: int
    n_users_test: int
    n_items_catalog: int
    n_cold_items: int
    n_null_items: int


class DatasetPipeline:
    """End-to-end dataset building pipeline orchestrator.
    
    Coordinates all pipeline stages with proper logging, error handling,
    and parallel processing optimization.
    
    Attributes:
        config: Pipeline configuration
        logger: Pipeline logger instance
        metrics: Runtime metrics tracker
    """
    
    def __init__(
        self,
        config_path: str | Path = "data_pipeline/configs/pipeline_config.yaml",
        log_level: str = "INFO",
        log_file: str | Path | None = None,
    ):
        """Initialize pipeline orchestrator.
        
        Args:
            config_path: Path to YAML configuration file
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            log_file: Optional log file path (None = console only)
        """
        # Setup logging first
        self._setup_logging(log_level, log_file)
        self.logger = logging.getLogger(__name__)
        
        # Load and validate configuration
        self.logger.info("Initializing DatasetPipeline")
        self.config = PipelineConfig.from_yaml(config_path)
        self.config.validate()
        self.config.ensure_directories()
        
        # Initialize stages
        self._init_stages()
        
        # Metrics tracking
        self.stage_durations: dict[str, float] = {}
        self.start_time: float | None = None
        
        self.logger.info(f"Pipeline initialized with config from {config_path}")
        self.logger.info(f"Output directory: {self.config.output_dir}")
        self.logger.info(f"Parallel jobs: {self.config.n_jobs}")
    
    def _setup_logging(
        self, 
        log_level: str, 
        log_file: str | Path | None
    ) -> None:
        """Configure logging with console and optional file handlers."""
        # Root logger configuration
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Console handler with detailed format
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # Optional file handler
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_path, mode='w')
            file_handler.setLevel(logging.DEBUG)  # Always DEBUG for file
            file_formatter = logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
    
    def _init_stages(self) -> None:
        """Initialize all pipeline stages."""
        self.stages = {
            "extraction": DataExtractionStage(n_jobs=self.config.n_jobs),
            "telemetry": TelemetryAugmentationStage(),
            "noise": NoiseInjectionStage(),
            "split": TrainTestSplitStage(),
            "implicit": ImplicitMatrixStage(),
            "features": FeatureEngineeringStage(n_jobs=self.config.n_jobs),
        }
    
    def _time_stage(self, stage_name: str, func, *args, **kwargs):
        """Execute a function and track its duration."""
        self.logger.info(f"\n{'#'*80}")
        self.logger.info(f"# Stage {len(self.stage_durations) + 1}/6: {stage_name}")
        self.logger.info(f"{'#'*80}\n")
        
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        
        self.stage_durations[stage_name] = duration
        self.logger.info(f"Stage '{stage_name}' completed in {duration:.2f}s\n")
        
        return result
    
    def run(self) -> dict[str, Any]:
        """Execute the complete pipeline.
        
        Returns:
            Build manifest dictionary with outputs and metadata
            
        Raises:
            Exception: If any pipeline stage fails
        """
        self.start_time = time.time()
        
        try:
            self.logger.info("\n" + "*" * 80)
            self.logger.info("*" + " " * 78 + "*")
            self.logger.info("*" + " " * 20 + "DATASET PIPELINE STARTED" + " " * 34 + "*")
            self.logger.info("*" + " " * 78 + "*")
            self.logger.info("*" * 80 + "\n")
            
            # Stage 1: Data Extraction
            events = self._time_stage(
                "Data Extraction",
                self.stages["extraction"].execute,
                behavior_path=self.config.kaggle_behavior_path,
                orders_path=self.config.kaggle_orders_path,
                event_type_map=self.config.event_type_map,
            )
            n_events_raw = events.height
            
            # Stage 2: Telemetry Augmentation
            events = self._time_stage(
                "Telemetry Augmentation",
                self.stages["telemetry"].execute,
                events=events,
                seed=self.config.seed,
                provinces=self.config.geo_provinces,
            )
            
            # Stage 3: Noise Injection
            noise_config = NoiseConfig(
                telemetry_drop_rate=self.config.telemetry_drop_rate,
                bot_fraction=self.config.bot_fraction,
                bot_lambda_per_sec=self.config.bot_lambda_per_sec,
                bot_interarrival_min_ms=self.config.bot_interarrival_min_ms,
                bot_interarrival_max_ms=self.config.bot_interarrival_max_ms,
                oo_lambda_per_sec=self.config.oo_lambda_per_sec,
                schema_null_rate=self.config.schema_null_rate,
                cold_start_rate=self.config.cold_start_rate,
                seed=self.config.seed,
            )
            
            corrupted, cold_items, null_items = self._time_stage(
                "Noise Injection",
                self.stages["noise"].execute,
                events=events,
                config=noise_config,
                provinces=self.config.geo_provinces,
                output_dir=self.config.output_dir,
            )
            
            # Stage 4: Train/Test Split
            train_path, test_path, train, test = self._time_stage(
                "Train/Test Split",
                self.stages["split"].execute,
                events=corrupted,
                test_fraction=self.config.test_fraction,
                min_user_interactions=self.config.min_user_interactions,
                seed=self.config.seed,
                output_dir=self.config.output_dir,
            )
            
            # Stage 5: Implicit Matrix
            implicit_path = self._time_stage(
                "Implicit Matrix",
                self.stages["implicit"].execute,
                train_path=train_path,
                output_path=self.config.output_dir / "implicit_matrix.parquet",
                weights=self.config.implicit_weights,
                norm_min=self.config.norm_min,
                norm_max=self.config.norm_max,
            )
            
            # Stage 6: Feature Engineering
            item_path, user_path, item_maps, user_maps = self._time_stage(
                "Feature Engineering",
                self.stages["features"].execute,
                train=train,
                test=test,
                cold_items=cold_items,
                output_dir=self.config.output_dir,
            )
            
            # Build manifest
            manifest = self._build_manifest(
                n_events_raw=n_events_raw,
                train=train,
                test=test,
                cold_items=cold_items,
                null_items=null_items,
                train_path=train_path,
                test_path=test_path,
                item_path=item_path,
                user_path=user_path,
                implicit_path=implicit_path,
                noise_config=noise_config,
            )
            
            # Save manifest
            manifest_path = self.config.output_dir / "build_manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2))
            self.logger.info(f"Build manifest saved to {manifest_path}")
            
            self._log_completion(manifest)
            
            return manifest
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    def _build_manifest(
        self,
        n_events_raw: int,
        train: pl.DataFrame,
        test: pl.DataFrame,
        cold_items: set[str],
        null_items: set[str],
        train_path: Path,
        test_path: Path,
        item_path: Path,
        user_path: Path,
        implicit_path: Path,
        noise_config: NoiseConfig,
    ) -> dict[str, Any]:
        """Build comprehensive pipeline manifest."""
        total_duration = time.time() - self.start_time
        
        full_catalog = pl.concat([train, test], how="vertical_relaxed")
        
        return {
            "pipeline": {
                "version": "2.0",
                "architecture": "class-based with joblib optimization",
                "seed": self.config.seed,
                "output_dir": str(self.config.output_dir),
                "n_jobs": self.config.n_jobs,
            },
            "metrics": {
                "n_events_raw": n_events_raw,
                "n_events_train": int(train.height),
                "n_events_test": int(test.height),
                "n_users_train": int(train["user_id"].n_unique()),
                "n_users_test": int(test["user_id"].n_unique()),
                "n_items_catalog": int(full_catalog["item_id"].n_unique()),
                "n_cold_items": len(cold_items),
                "n_null_items": len(null_items),
            },
            "outputs": {
                "train_interactions": str(train_path),
                "test_interactions": str(test_path),
                "item_features": str(item_path),
                "user_features": str(user_path),
                "implicit_matrix": str(implicit_path),
                "cold_items": str(self.config.output_dir / "cold_items.json"),
                "null_items": str(self.config.output_dir / "null_items.json"),
            },
            "timing": {
                "stage_durations_seconds": self.stage_durations,
                "total_duration_seconds": total_duration,
                "total_duration_formatted": self._format_duration(total_duration),
            },
            "noise_config": asdict(noise_config),
            "built_at": datetime.now(timezone.utc).isoformat(),
        }
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = seconds % 60
            return f"{mins}m {secs:.1f}s"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m"
    
    def _log_completion(self, manifest: dict[str, Any]) -> None:
        """Log pipeline completion summary."""
        self.logger.info("\n" + "*" * 80)
        self.logger.info("*" + " " * 78 + "*")
        self.logger.info("*" + " " * 18 + "PIPELINE COMPLETED SUCCESSFULLY" + " " * 29 + "*")
        self.logger.info("*" + " " * 78 + "*")
        self.logger.info("*" * 80)
        
        metrics = manifest["metrics"]
        timing = manifest["timing"]
        
        self.logger.info("\n=== PIPELINE SUMMARY ===")
        self.logger.info(f"Total Duration: {timing['total_duration_formatted']}")
        self.logger.info(f"\nData Metrics:")
        self.logger.info(f"  Raw Events:       {metrics['n_events_raw']:>10,}")
        self.logger.info(f"  Train Events:     {metrics['n_events_train']:>10,}")
        self.logger.info(f"  Test Events:      {metrics['n_events_test']:>10,}")
        self.logger.info(f"  Train Users:      {metrics['n_users_train']:>10,}")
        self.logger.info(f"  Test Users:       {metrics['n_users_test']:>10,}")
        self.logger.info(f"  Catalog Items:    {metrics['n_items_catalog']:>10,}")
        self.logger.info(f"  Cold Items:       {metrics['n_cold_items']:>10,}")
        self.logger.info(f"  Null Items:       {metrics['n_null_items']:>10,}")
        
        self.logger.info(f"\nStage Durations:")
        for stage, duration in timing['stage_durations_seconds'].items():
            pct = (duration / timing['total_duration_seconds']) * 100
            self.logger.info(f"  {stage:<25} {duration:>8.2f}s ({pct:>5.1f}%)")
        
        self.logger.info(f"\nOutputs saved to: {self.config.output_dir}")
        self.logger.info("=" * 80 + "\n")


def main():
    """Command-line entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run the dataset building pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default config
  python pipeline_orchestrator.py
  
  # Use custom config and log file
  python pipeline_orchestrator.py \\
    --config my_config.yaml \\
    --log-file pipeline.log \\
    --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="data_pipeline/configs/pipeline_config.yaml",
        help="Path to pipeline configuration YAML"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Optional log file path (default: console only)"
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = DatasetPipeline(
        config_path=args.config,
        log_level=args.log_level,
        log_file=args.log_file,
    )
    
    manifest = pipeline.run()
    
    print(f"\n✓ Pipeline completed successfully!")
    print(f"✓ Outputs saved to: {pipeline.config.output_dir}")
    print(f"✓ Manifest: {pipeline.config.output_dir / 'build_manifest.json'}")


if __name__ == "__main__":
    main()

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


"""Example usage of the refactored DatasetPipeline.

Demonstrates various ways to run the pipeline with different
configurations and logging setups.
"""

from datetime import datetime
from pathlib import Path
from src.zapudataset.pipeline import DatasetPipeline
from src.zapudataset.configs import logger

def example_basic():
    """Basic pipeline execution with console logging."""
    logger.info("\n" + "="*80)
    logger.info("Example 1: Basic Execution")
    logger.info("="*80 + "\n")
    
    pipeline = DatasetPipeline(
    config_path="src/zapudataset/configs/pipeconf.yaml")
    
    manifest = pipeline.run()

    train_path = Path(manifest["outputs"]["train_interactions"])
    test_path = Path(manifest["outputs"]["test_interactions"])
    assert train_path.exists() and train_path.stat().st_size > 0
    assert test_path.exists() and test_path.stat().st_size > 0
    assert manifest["metrics"]["n_events_train"] > 0
    assert manifest["metrics"]["n_events_test"] > 0

    logger.info(f"\n✓ Pipeline completed successfully!")
    logger.info(f"✓ Total duration: {manifest['timing']['total_duration_formatted']}")
    logger.info(f"✓ Outputs: {pipeline.config.output_dir}")
    
    return manifest


def example_with_file_logging():
    """Pipeline with file logging for detailed diagnostics."""
    logger.info("\n" + "="*80)
    logger.info("Example 2: With File Logging")
    logger.info("="*80 + "\n")
    
    # Create timestamped log file
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
    
    pipeline = DatasetPipeline(
        config_path="src/zapudataset/configs/pipeconf.yaml",
        log_level="INFO",
        log_file=log_file
    )
    
    logger.info(f"Log file: {log_file}")
    
    manifest = pipeline.run()
    
    logger.info(f"\n✓ Pipeline completed!")
    logger.info(f"✓ Check detailed logs in: {log_file}")
    
    return manifest


def example_debug_mode():
    """Debug mode with maximum verbosity."""
    logger.info("\n" + "="*80)
    logger.info("Example 3: Debug Mode")
    logger.info("="*80 + "\n")
    
    log_file = Path("logs") / f"debug_{datetime.now():%Y%m%d_%H%M%S}.log"
    
    pipeline = DatasetPipeline(
        config_path="src/zapudataset/configs/pipeconf.yaml",
        log_level="DEBUG",  # Maximum verbosity
        log_file=log_file
    )
    
    manifest = pipeline.run()
    
    logger.info(f"\n✓ Debug run completed!")
    logger.info(f"✓ Full debug logs: {log_file}")
    
    return manifest


def example_custom_config():
    """Use a custom configuration file."""
    logger.info("\n" + "="*80)
    logger.info("Example 4: Custom Configuration")
    logger.info("="*80 + "\n")
    
    # You can create different configs for different experiments
    custom_config = "src/zapudataset/configs/pipeconf.yaml"  # Use main config for now
    
    pipeline = DatasetPipeline(
        config_path=custom_config,
        log_level="INFO"
    )
    
    manifest = pipeline.run()
    
    logger.info(f"\n✓ Custom config run completed!")
    logger.info(f"✓ Config: {custom_config}")
    
    return manifest


def example_inspect_manifest():
    """Run pipeline and inspect the manifest."""
    logger.info("\n" + "="*80)
    logger.info("Example 5: Inspect Build Manifest")
    logger.info("="*80 + "\n")
    
    pipeline = DatasetPipeline(
        config_path="data_pipeline/configs/pipeline_config.yaml",
        log_level="INFO"
    )
    
    manifest = pipeline.run()
    
    # Inspect results
    logger.info("\n" + "-"*80)
    logger.info("MANIFEST INSPECTION")
    logger.info("-"*80)
    
    logger.info(f"\nPipeline Version: {manifest['pipeline']['version']}")
    logger.info(f"Architecture: {manifest['pipeline']['architecture']}")
    
    logger.info(f"\nData Metrics:")
    metrics = manifest['metrics']
    for key, value in metrics.items():
        logger.info(f"  {key:25s}: {value:>10,}")
    
    logger.info(f"\nTiming Breakdown:")
    for stage, duration in manifest['timing']['stage_durations_seconds'].items():
        total = manifest['timing']['total_duration_seconds']
        pct = (duration / total) * 100
        logger.info(f"  {stage:30s}: {duration:6.2f}s ({pct:5.1f}%)")
    
    logger.info(f"\nOutput Files:")
    for name, path in manifest['outputs'].items():
        size = Path(path).stat().st_size / (1024 * 1024)  # MB
        logger.info(f"  {name:25s}: {Path(path).name} ({size:.2f} MB)")
    
    return manifest


def example_error_handling():
    """Demonstrate error handling with invalid config."""
    logger.info("\n" + "="*80)
    logger.info("Example 6: Error Handling")
    logger.info("="*80 + "\n")
    
    try:
        # This will fail with a clear error message
        pipeline = DatasetPipeline(
            config_path="nonexistent_config.yaml",
            log_level="INFO"
        )
        manifest = pipeline.run()
    except FileNotFoundError as e:
        logger.info(f"✗ Expected error caught: {e}")
        logger.info("✓ Error handling works correctly!")
    except Exception as e:
        logger.info(f"✗ Unexpected error: {e}")


def main():
    """Run all examples (comment out the ones you don't need)."""
    logger.info("\n" + "#"*80)
    logger.info("#" + " "*78 + "#")
    logger.info("#" + " "*20 + "DATASET PIPELINE - USAGE EXAMPLES" + " "*25 + "#")
    logger.info("#" + " "*78 + "#")
    logger.info("#"*80)
    
    # Run the example you want (uncomment the one you need)
    
    # Basic usage - recommended for first run
    manifest = example_basic()
    
    # With file logging - recommended for production
    # manifest = example_with_file_logging()
    
    # Debug mode - use when troubleshooting
    # manifest = example_debug_mode()
    
    # Custom config - for experiments
    # manifest = example_custom_config()
    
    # Inspect manifest - see detailed results
    # manifest = example_inspect_manifest()
    
    # Error handling demo
    # example_error_handling()
    
    logger.info("\n" + "#"*80)
    logger.info("#" + " "*78 + "#")
    logger.info("#" + " "*25 + "ALL EXAMPLES COMPLETED" + " "*31 + "#")
    logger.info("#" + " "*78 + "#")
    logger.info("#"*80 + "\n")


if __name__ == "__main__":
    main()

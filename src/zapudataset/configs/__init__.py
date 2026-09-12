#!/usr/bin/env python3

"""ZaPuDia Data Pipeline Infrastructure & Configuration Package.

This package provides production-grade configuration parsing, environment variable 
interpolation, Pydantic V2 schema validation, and centralized logging management 
for the ZaPuDia Unified Dataset build pipeline.

Example:
    >>> from data_pipeline import load_config, PipeConfig, logconfig
    >>> logger = logconfig()
    >>> config = load_config("configs/pipeconf.yaml", schema=PipeConfig)
    >>> logger.info("Initialized pipeline: %s v%s", config.pipeline.name, config.pipeline.version)
"""

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-07"


# --------------------------------------------------------------------------
# Relative Imports from Internal Modules
# --------------------------------------------------------------------------
from .logconf    import  logconfig
from .confreader import (ConfigError,
                         ConfigNotFoundError,
                         ConfigValidationError,
                         EnvVarYamlLoader,
                         load_config,)
from .schemaread import (GA4Source,
                         ImplicitNormalization,
                         ImplicitWeights,
                         KaggleBehaviorSource,
                         KaggleOrdersSource,
                         NoiseConfig,
                         PipeConfig,
                         PipelineMeta,
                         SessionConfig,
                         SourcesConfig,
                         SplitConfig,
                         SchemaRead,)
logger  = logconfig()

__all__ = [# Metadata
           "__author__",
           "__copyright__",
           "__license__",
           "__version__",
           "__maintainer__",
           "__email__",
           
           # Logging Utilities
           "logconfig",
           
           # Configuration Reader & Exceptions
           "load_config",
           "EnvVarYamlLoader",
           "ConfigError",
           "ConfigNotFoundError",
           "ConfigValidationError",
           
           # Pydantic Schemas for PipeConfig
           "PipeConfig",
           "PipelineMeta",
           "SourcesConfig",
           "KaggleBehaviorSource",
           "KaggleOrdersSource",
           "GA4Source",
           "ImplicitWeights",
           "ImplicitNormalization",
           "NoiseConfig",
           "SplitConfig",
           "SessionConfig",
           
           # Pipeline Execution Utilities
           "SchemaRead",]
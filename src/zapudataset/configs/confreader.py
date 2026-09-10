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

import os
import re
import yaml
import logging
from pathlib   import Path
from functools import lru_cache
from typing    import TypeVar, Type, Any, Dict, Optional, Union
from pydantic  import BaseModel, ValidationError

logger = logging.getLogger("ConfigReader")

T = TypeVar("T", bound=BaseModel)

class ConfigError(Exception):
    """Base exception untuk error konfigurasi."""
    pass

class ConfigNotFoundError(ConfigError, FileNotFoundError):
    pass

class ConfigValidationError(ConfigError):
    pass


class EnvVarYamlLoader(yaml.SafeLoader):
    """Custom PyYAML SafeLoader dengan dukungan Environment Variable Substitution."""
    pass


# Regex untuk mendeteksi ${VAR_NAME} atau ${VAR_NAME:default_value}
ENV_PATTERN = re.compile(r"\$\{([^}:\s]+)(?::-?([^}]*))?\}")

def _env_var_constructor(loader: yaml.SafeLoader, node: yaml.Node) -> Any:
    value = loader.construct_scalar(node)
    
    def replace_match(match: re.Match) -> str:
        env_var, default_val = match.groups()
        res = os.environ.get(env_var)
        if res is not None:
            return res
        if default_val is not None:
            return default_val
        raise ConfigError(
            f"Environment variable '{env_var}' tidak ditemukan dan tidak ada default value!"
        )

    return ENV_PATTERN.sub(replace_match, value)

# Register implicit resolver dan constructor
EnvVarYamlLoader.add_implicit_resolver("!env", ENV_PATTERN, None)
EnvVarYamlLoader.add_constructor("!env", _env_var_constructor)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Rekursif deep merge dua dictionary."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_extends(config_dict: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Resolusi hirarki '_extends'."""
    if "_extends" not in config_dict:
        return config_dict

    parent_rel_path = config_dict.pop("_extends")
    parent_path = (base_dir / parent_rel_path).resolve()

    if not parent_path.exists():
        raise ConfigNotFoundError(f"Parent config '_extends: {parent_rel_path}' tidak ditemukan di {parent_path}")

    parent_dict = _raw_load_yaml(parent_path)
    parent_dict = _resolve_extends(parent_dict, parent_path.parent)

    return _deep_merge(parent_dict, config_dict)


def _raw_load_yaml(file_path: Path) -> dict[str, Any]:
    """Load YAML via SafeLoader kustom."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = yaml.load(f, Loader=EnvVarYamlLoader)
            return content if content is not None else {}
    except yaml.YAMLError as e:
        raise ConfigError(f"Gagal parsing YAML pada [{file_path}]: {e}") from e


@lru_cache(maxsize=32)
def load_config(
    config_path: Union[str, Path], 
    schema: Optional[Type[T]] = None
) -> Union[T, dict[str, Any]]:
    """Fungsi utama pembaca konfigurasi YAML.

    Args:
        config_path: Path ke file .yaml/.yml
        schema: (Opsional) Class Pydantic BaseModel untuk validasi tipe data.

    Returns:
        Instance Pydantic BaseModel (jika schema diberikan) atau dict murni.
    """
    path = Path(config_path).resolve()

    if not path.exists():
        raise ConfigNotFoundError(f"File konfigurasi tidak ditemukan di path: {path}")

    # 1. Parse YAML + Interpolasi Env Var
    raw_dict = _raw_load_yaml(path)

    # 2. Resolve Inheritance (_extends)
    merged_dict = _resolve_extends(raw_dict, path.parent)

    # 3. Validasi Pydantic Schema jika disediakan
    if schema is not None:
        try:
            return schema.model_validate(merged_dict)
        except ValidationError as e:
            raise ConfigValidationError(
                f"Validasi skema gagal untuk file [{path.name}]:\n{e}"
            ) from e

    return merged_dict


if __name__ == "__main__":
    pass
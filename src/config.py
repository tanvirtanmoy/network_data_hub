"""Loads the pipeline configuration."""

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)

    # Resolve relative paths against the project root so jobs work from any
    # working directory. Absolute paths and s3a:// URIs are left as they are.
    for key, value in cfg["paths"].items():
        if "://" not in value and not Path(value).is_absolute():
            cfg["paths"][key] = str(PROJECT_ROOT / value)
    return cfg

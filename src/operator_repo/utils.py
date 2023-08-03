"""
    Utility functions to load yaml files
"""

import logging
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)


def _find_yaml(path: Path) -> Path:
    """Look for yaml files with alternate extensions"""

    if path.is_file():
        return path
    tries = [path]
    for alt_extension in [".yaml", ".yml"]:
        if alt_extension == path.suffix:
            continue
        new_path = path.with_suffix(alt_extension)
        if new_path.is_file():
            return new_path
        tries.append(new_path)
    tries_str = ", ".join([str(x) for x in tries])
    raise FileNotFoundError(f"Can't find yaml file. Tried: {tries_str}")


def _load_yaml_strict(path: Path) -> Any:
    """Returns the parsed contents of the YAML file at the given path"""

    log.debug("Loading %s", path)
    with path.open("r") as yaml_file:
        return yaml.safe_load(yaml_file)


def load_yaml(path: Path) -> Any:
    """Same as _load_yaml_strict but tries both .yaml and .yml extensions"""
    return _load_yaml_strict(_find_yaml(path))

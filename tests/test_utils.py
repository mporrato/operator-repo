from pathlib import Path

import pytest
from operator_repo.utils import load_yaml
from tests import create_files


def test_load_yaml(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        {"data/en.yml": {"hello": "world"}},
        {"data/it.yaml": {"ciao": "mondo"}},
        {"data/something.txt": {"foo": "bar"}},
    )
    assert load_yaml(tmp_path / "data/en.yaml") == {"hello": "world"}
    assert load_yaml(tmp_path / "data/en.yml") == {"hello": "world"}
    assert load_yaml(tmp_path / "data/it.yaml") == {"ciao": "mondo"}
    assert load_yaml(tmp_path / "data/it.yml") == {"ciao": "mondo"}
    assert load_yaml(tmp_path / "data/something.txt") == {"foo": "bar"}
    with pytest.raises(FileNotFoundError):
        _ = load_yaml(tmp_path / "data/something.yaml")
    with pytest.raises(FileNotFoundError):
        _ = load_yaml(tmp_path / "data/something.yml")

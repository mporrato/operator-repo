from pathlib import Path

import pytest

from operator_repo import Bundle, Repo
from tests import bundle_files, catalog_files, create_files


@pytest.fixture
def mock_bundle(tmp_path: Path) -> Bundle:
    """
    Create a dummy file structure for a bundle and return the corresponding
    Bundle object
    """
    create_files(tmp_path, bundle_files("hello", "0.0.1"))
    repo = Repo(tmp_path)
    return repo.operator("hello").bundle("0.0.1")


@pytest.fixture
def mock_repo(tmp_path: Path) -> Repo:
    """
    Create a dummy file structure for an operator repo with two operators
    and a total of four bundles and return the corresponding Repo object
    """
    create_files(
        tmp_path,
        bundle_files("hello", "0.0.1"),
        bundle_files("hello", "0.0.2"),
        bundle_files("world", "0.0.1"),
        bundle_files("world", "0.0.2"),
        catalog_files("v4.17", "hello"),
        catalog_files("v4.18", "hello"),
    )
    repo = Repo(tmp_path)
    return repo

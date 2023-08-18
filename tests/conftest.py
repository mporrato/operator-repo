from pathlib import Path

import pytest

from operator_repo import Bundle, Repo
from tests import bundle_files, create_files


@pytest.fixture
def mock_bundle(tmp_path: Path) -> Bundle:
    """
    Create a dummy file structure for a bundle and return the corresponding
    Bundle object
    """
    create_files(tmp_path, bundle_files("hello", "0.0.1"))
    repo = Repo(tmp_path)
    return repo.operator("hello").bundle("0.0.1")

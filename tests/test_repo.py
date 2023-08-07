from pathlib import Path

import pytest

from operator_repo import Repo
from operator_repo.exceptions import InvalidRepoException
from tests import bundle_files, create_files


def test_repo_non_existent() -> None:
    with pytest.raises(InvalidRepoException, match="Not a valid operator repository"):
        _ = Repo("/non_existent")


def test_repo_empty(tmp_path: Path) -> None:
    with pytest.raises(InvalidRepoException, match="Not a valid operator repository"):
        _ = Repo(str(tmp_path))


def test_repo_no_operators(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        {
            "operators": None,
            "ci/pipeline-config.yaml": {"hello": "world"},
        },
    )
    repo = Repo(tmp_path)
    assert len(list(repo)) == 0
    assert repo.config == {"hello": "world"}
    assert repo.root == tmp_path.resolve()
    assert str(tmp_path) in repr(repo)


def test_repo_one_bundle(tmp_path: Path) -> None:
    create_files(tmp_path, bundle_files("hello", "0.0.1"))
    repo = Repo(tmp_path)
    assert repo.config == {}
    assert len(list(repo)) == 1
    assert set(repo) == {repo.operator("hello")}
    assert repo.has("hello")

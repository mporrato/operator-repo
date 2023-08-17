from pathlib import Path

import pytest

from operator_repo import Repo
from operator_repo.checks.operator import check_upgrade
from tests import bundle_files, create_files


@pytest.mark.parametrize(
    "bundles, operator_name, expected_results",
    [
        (
            [bundle_files("hello", "0.0.1")],
            "hello",
            set(),
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                bundle_files("hello", "0.0.2"),
            ],
            "hello",
            {"Channel beta has dangling bundles: {Bundle(hello/0.0.1)}"},
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                bundle_files(
                    "hello", "0.0.2", csv={"spec": {"replaces": "hello.v0.0.1"}}
                ),
            ],
            "hello",
            set(),
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                bundle_files("hello", "0.0.2", csv={"spec": {"replaces": "rubbish"}}),
            ],
            "hello",
            {"Bundle(hello/0.0.2) has invalid 'replaces' field: 'rubbish'"},
        ),
    ],
    False,
    [
        "Single bundle",
        "Two bundles, no replaces",
        "Two bundles",
        "Two bundles, invalid replaces",
    ],
)
def test_upgrade(
    tmp_path: Path, bundles: list[dict], operator_name: str, expected_results: set[str]
) -> None:
    create_files(tmp_path, *bundles)
    repo = Repo(tmp_path)
    operator = repo.operator(operator_name)
    assert {x.reason for x in check_upgrade(operator)} == expected_results

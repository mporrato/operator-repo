from pathlib import Path

import pytest

from operator_repo import OperatorCatalog, Repo
from operator_repo.exceptions import InvalidOperatorCatalogException
from tests import bundle_files, catalog_files, create_files


def test_invalid_catalog(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        catalog_files("v4.14", "fake-operator"),
        bundle_files("fake-operator", "0.0.1"),
    )
    repo = Repo(tmp_path)
    with pytest.raises(InvalidOperatorCatalogException):
        OperatorCatalog(repo.root / "catalogs" / "4.14" / "not-found")


def test_operator_catalog(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        catalog_files("v4.14", "fake-operator"),
        catalog_files("v4.13", "fake-operator-2"),
        bundle_files("fake-operator", "0.0.1"),
    )
    repo = Repo(tmp_path)
    catalog = repo.catalog("v4.14")
    assert len(list(catalog.all_operator_catalogs())) == 1
    operator_catalog = catalog.operator_catalog("fake-operator")

    assert repr(operator_catalog) == "OperatorCatalog(v4.14/fake-operator)"

    assert operator_catalog.repo == repo
    assert operator_catalog.catalog == catalog

    assert (
        operator_catalog.catalog_content_path == operator_catalog.root / "catalog.yaml"
    )

    assert operator_catalog.catalog_content == [{"foo": "bar"}]

    assert operator_catalog == catalog.operator_catalog("fake-operator")

    operator_catalog = OperatorCatalog(operator_catalog.root)
    assert operator_catalog.catalog == catalog

    assert operator_catalog.operator == repo.operator("fake-operator")

    assert list(repo.operator("fake-operator").all_catalogs()) == [catalog]


def test_catalog_operator_compare(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        catalog_files("v4.14", "fake-operator"),
        catalog_files("v4.14", "fake-operator-2"),
        bundle_files("fake-operator", "0.0.1"),
    )
    repo = Repo(tmp_path)
    catalog = repo.catalog("v4.14")
    operator1 = catalog.operator_catalog("fake-operator")
    operator2 = catalog.operator_catalog("fake-operator-2")

    assert operator1 == operator1
    assert operator1 != operator2
    assert operator1 < operator2

    assert operator1 != "foo"
    with pytest.raises(TypeError):
        operator1 < "foo"

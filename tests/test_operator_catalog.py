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
        bundle_files("fake-operator", "0.0.1"),
    )
    repo = Repo(tmp_path)
    catalog = repo.catalog("v4.14")
    assert len(list(catalog.all_operator_catalogs())) == 1
    operator = catalog.operator_catalog("fake-operator")

    assert repr(operator) == "OperatorCatalog(fake-operator)"

    assert operator.repo == repo
    assert operator.catalog == catalog

    assert operator.catalog_content_path == operator.root / "catalog.yaml"

    assert operator == catalog.operator_catalog("fake-operator")

    operator = OperatorCatalog(operator.root)
    assert operator.catalog == catalog


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

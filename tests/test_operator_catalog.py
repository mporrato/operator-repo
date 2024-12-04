from pathlib import Path

import pytest

from operator_repo import OperatorCatalog, OperatorCatalogList, Repo
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
    assert operator_catalog == "v4.14/fake-operator"

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
    assert operator1 != 42
    with pytest.raises(TypeError):
        operator1 < "foo"


def test_operator_catalog_list(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        catalog_files("v4.14", "fake-operator"),
        catalog_files("v4.14", "fake-operator-2"),
        catalog_files("v4.14", "fake-operator-3"),
        catalog_files("v4.14", "fake-operator-4"),
        bundle_files("fake-operator", "0.0.1"),
    )
    repo = Repo(tmp_path)
    catalog = repo.catalog("v4.14")
    operator_catalog = catalog.operator_catalog("fake-operator")
    operator_catalog2 = catalog.operator_catalog("fake-operator-2")
    operator_catalog3 = catalog.operator_catalog("fake-operator-3")
    operator_catalog4 = catalog.operator_catalog("fake-operator-4")

    empty_operator_catalog_list = OperatorCatalogList()
    assert len(empty_operator_catalog_list) == 0
    operator_catalog_list = OperatorCatalogList([operator_catalog, operator_catalog])
    assert len(operator_catalog_list) == 1
    assert repr(operator_catalog_list) == "[OperatorCatalog(v4.14/fake-operator)]"
    operator_catalog_list.append(operator_catalog2)
    operator_catalog_list.extend([operator_catalog3])
    operator_catalog_list.insert(0, operator_catalog4)
    assert len(operator_catalog_list) == 4

    assert operator_catalog in operator_catalog_list
    assert "v4.14/fake-operator-4" in operator_catalog_list

    with pytest.raises(ValueError):
        operator_catalog_list.append(operator_catalog)
    with pytest.raises(ValueError):
        operator_catalog_list.insert(0, operator_catalog)
    with pytest.raises(ValueError):
        operator_catalog_list.extend([operator_catalog])
    with pytest.raises(TypeError):
        OperatorCatalogList(["foo", 1])  # type: ignore
    with pytest.raises(TypeError):
        operator_catalog_list.append("foo")
    with pytest.raises(TypeError):
        operator_catalog_list.extend([1, 2, 3])
    with pytest.raises(TypeError):
        operator_catalog_list.insert(0, {"foo": "bar"})

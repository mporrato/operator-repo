from pathlib import Path
from typing import Any, Union

import pytest

from operator_repo import Repo
from operator_repo.checks.bundle import check_image, check_operator_name, check_semver
from tests import bundle_files, create_files, make_nested_dict


@pytest.mark.parametrize(
    "base_bundle_files, extra_files, expected_results",
    [
        (
            bundle_files("hello", "0.0.1"),
            {},
            set(),
        ),
        (
            bundle_files(
                "hello",
                "0.0.1",
                annotations={"operators.operatorframework.io.bundle.package.v1": "foo"},
            ),
            {},
            {
                "Operator name from annotations.yaml (foo) does not match the operator's directory name (hello)",
                "Operator name from annotations.yaml (foo) does not match the name defined in the CSV (hello)",
            },
        ),
        (
            bundle_files("hello", "0.0.1"),
            {"operators/hello/0.0.1/metadata/annotations.yaml": {"annotations": {}}},
            {
                "Bundle does not define the operator name in annotations.yaml",
            },
        ),
    ],
    indirect=False,
    ids=["Names ok", "Wrong annotations.yaml", "Empty annotations.yaml"],
)
def test_operator_name(
    tmp_path: Path,
    base_bundle_files: dict[str, Any],
    extra_files: dict[str, Any],
    expected_results: set[str],
) -> None:
    create_files(tmp_path, base_bundle_files, extra_files)
    repo = Repo(tmp_path)
    operator = next(repo.all_operators())
    bundle = next(operator.all_bundles())
    assert {x.reason for x in check_operator_name(bundle)} == expected_results


@pytest.mark.parametrize(
    "base_bundle_files, extra_files, expected_results",
    [
        (
            bundle_files("hello", "0.0.1"),
            {},
            {"CSV doesn't define .metadata.annotations.containerImage"},
        ),
        (
            bundle_files(
                "hello",
                "0.0.1",
                csv=make_nested_dict(
                    {
                        "metadata.annotations.containerImage": "example.com/namespace/image:tag",
                    }
                ),
            ),
            {},
            {"CSV doesn't define .spec.install.spec.deployments"},
        ),
        (
            bundle_files(
                "hello",
                "0.0.1",
                csv=make_nested_dict(
                    {
                        "metadata.annotations.containerImage": "example.com/namespace/image:tag",
                        "spec.install.spec.deployments": [
                            make_nested_dict(
                                {
                                    "spec.template.spec.containers": [
                                        {"image": "example.com/namespace/image:tag"}
                                    ]
                                }
                            ),
                        ],
                    }
                ),
            ),
            {},
            set(),
        ),
        (
            bundle_files(
                "hello",
                "0.0.1",
                csv=make_nested_dict(
                    {
                        "metadata.annotations.containerImage": "example.com/namespace/image:tag",
                        "spec.install.spec.deployments": [
                            make_nested_dict(
                                {
                                    "spec.template.spec.containers": [
                                        {
                                            "image": "example.com/namespace/image:othertag"
                                        }
                                    ]
                                }
                            ),
                        ],
                    }
                ),
            ),
            {},
            {
                "container image example.com/namespace/image:tag not used by any deployment"
            },
        ),
        (
            bundle_files("hello", "0.0.1"),
            {"operators/hello/0.0.1/manifests/hello.clusterserviceversion.yaml": ""},
            "Invalid CSV contents ",
        ),
    ],
    indirect=False,
    ids=[
        "Missing containerImage",
        "Missing deployments",
        "Matching images",
        "Mismatched images",
        "Empty CSV",
    ],
)
def test_image(
    tmp_path: Path,
    base_bundle_files: dict[str, Any],
    extra_files: dict[str, Any],
    expected_results: Union[set[str], str],
) -> None:
    create_files(tmp_path, base_bundle_files, extra_files)
    repo = Repo(tmp_path)
    operator = next(repo.all_operators())
    bundle = next(operator.all_bundles())
    reasons = {x.reason for x in check_image(bundle)}
    if isinstance(expected_results, str):
        assert len(reasons) == 1
        reason = reasons.pop()
        assert expected_results in reason
    else:
        assert reasons == expected_results


@pytest.mark.parametrize(
    "base_bundle_files, extra_files, expected_results",
    [
        (
            bundle_files("hello", "0.0.1"),
            {},
            set(),
        ),
        (
            bundle_files("hello", "latest"),
            {},
            {
                "Version from CSV (latest) is not valid semver",
                "Version from filesystem (latest) is not valid semver",
            },
        ),
    ],
    indirect=False,
    ids=["All versions ok", "Both versions invalid"],
)
def test_semver(
    tmp_path: Path,
    base_bundle_files: dict[str, Any],
    extra_files: dict[str, Any],
    expected_results: set[str],
) -> None:
    create_files(tmp_path, base_bundle_files, extra_files)
    repo = Repo(tmp_path)
    operator = next(repo.all_operators())
    bundle = next(operator.all_bundles())
    assert {x.reason for x in check_semver(bundle)} == expected_results

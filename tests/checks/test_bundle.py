from pathlib import Path
from typing import Any, Union

import pytest

from operator_repo import Repo
from operator_repo.checks import Fail, Warn
from operator_repo.checks.bundle import check_image, check_operator_name, check_semver
from tests import bundle_files, create_files, make_nested_dict


@pytest.mark.parametrize(
    "files, bundle_to_check, expected_results",
    [
        (
            [
                bundle_files("hello", "0.0.1"),
            ],
            ("hello", "0.0.1"),
            set(),
        ),
        (
            [
                bundle_files(
                    "hello",
                    "0.0.1",
                    annotations={
                        "operators.operatorframework.io.bundle.package.v1": "foo"
                    },
                ),
            ],
            ("hello", "0.0.1"),
            {
                (
                    Fail,
                    "Operator name from annotations.yaml (foo) does not match"
                    " the operator's directory name (hello)",
                ),
                (
                    Fail,
                    "Operator name from annotations.yaml (foo) does not match"
                    " the name defined in the CSV (hello)",
                ),
            },
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                {
                    "operators/hello/0.0.1/metadata/annotations.yaml": {
                        "annotations": {}
                    }
                },
            ],
            ("hello", "0.0.1"),
            {
                (Fail, "Bundle does not define the operator name in annotations.yaml"),
            },
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                bundle_files(
                    "hello",
                    "0.0.2",
                    annotations={
                        "operators.operatorframework.io.bundle.package.v1": "foo"
                    },
                ),
                bundle_files(
                    "hello",
                    "0.0.3",
                    annotations={
                        "operators.operatorframework.io.bundle.package.v1": "foo"
                    },
                ),
            ],
            ("hello", "0.0.3"),
            {
                (
                    Warn,
                    "Operator name from annotations.yaml (foo) does not match"
                    " the operator's directory name (hello)",
                ),
                (
                    Warn,
                    "Operator name from annotations.yaml (foo) does not match"
                    " the name defined in the CSV (hello)",
                ),
                (
                    Warn,
                    "Operator name from annotations.yaml is not consistent"
                    " across bundles: ['foo', 'hello']",
                ),
            },
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                bundle_files(
                    "hello",
                    "0.0.2",
                    csv={"metadata": {"name": "foo.v0.0.2"}},
                ),
                bundle_files(
                    "hello",
                    "0.0.3",
                    csv={"metadata": {"name": "foo.v0.0.3"}},
                ),
            ],
            ("hello", "0.0.3"),
            {
                (
                    Warn,
                    "Operator name from annotations.yaml (hello) does not match"
                    " the name defined in the CSV (foo)",
                ),
                (
                    Warn,
                    "Operator name from the CSV is not consistent across bundles: ['foo', 'hello']",
                ),
            },
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                bundle_files("hello", "0.0.2"),
                bundle_files(
                    "hello",
                    "0.0.3",
                    annotations={
                        "operators.operatorframework.io.bundle.package.v1": "foo"
                    },
                ),
            ],
            ("hello", "0.0.3"),
            {
                (
                    Fail,
                    "Operator name from annotations.yaml (foo) does not match"
                    " the operator's directory name (hello)",
                ),
                (
                    Fail,
                    "Operator name from annotations.yaml (foo) does not match"
                    " the name defined in the CSV (hello)",
                ),
                (
                    Fail,
                    "Operator name from annotations.yaml (foo) does not match"
                    " the name defined in other bundles (hello)",
                ),
            },
        ),
        (
            [
                bundle_files("hello", "0.0.1"),
                bundle_files("hello", "0.0.2"),
                bundle_files(
                    "hello",
                    "0.0.3",
                    csv={"metadata": {"name": "foo.v0.0.3"}},
                ),
            ],
            ("hello", "0.0.3"),
            {
                (
                    Fail,
                    "Operator name from annotations.yaml (hello) does not match"
                    " the name defined in the CSV (foo)",
                ),
                (
                    Fail,
                    "Operator name from the CSV (foo) does not match the name"
                    " defined in other bundles (hello)",
                ),
            },
        ),
    ],
    indirect=False,
    ids=[
        "Names ok",
        "Wrong annotations.yaml name",
        "Empty annotations.yaml",
        "Wrong annotations.yaml name, inconsistent bundles",
        "Wrong CSV name, inconsistent bundles",
        "Wrong annotations.yaml name, consistent bundles",
        "Wrong CSV name, consistent bundles",
    ],
)
def test_operator_name(
    tmp_path: Path,
    files: list[dict[str, Any]],
    bundle_to_check: tuple[str, str],
    expected_results: set[tuple[type, str]],
) -> None:
    create_files(tmp_path, *files)
    repo = Repo(tmp_path)
    operator_name, bundle_version = bundle_to_check
    operator = repo.operator(operator_name)
    bundle = operator.bundle(bundle_version)
    assert {
        (x.__class__, x.reason) for x in check_operator_name(bundle)
    } == expected_results


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

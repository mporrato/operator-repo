from collections.abc import Iterator

from semver import Version

from .. import Bundle
from ..utils import lookup_dict
from . import CheckResult, Fail, Warn


def check_operator_name(bundle: Bundle) -> Iterator[CheckResult]:
    """Check if the operator names used in CSV, metadata and filesystem are consistent"""
    name = bundle.annotations.get("operators.operatorframework.io.bundle.package.v1")
    if name is None:
        yield Fail(
            bundle, f"Bundle does not define the operator name in annotations.yaml"
        )
        return
    if name != bundle.csv_operator_name:
        yield Fail(
            bundle,
            f"Operator name from annotations.yaml ({name}) does not match the name defined in the CSV ({bundle.csv_operator_name})",
        )
    if name != bundle.operator_name:
        yield Fail(
            bundle,
            f"Operator name from annotations.yaml ({name}) does not match the operator's directory name ({bundle.operator_name})",
        )


def check_image(bundle: Bundle) -> Iterator[CheckResult]:
    """Check if containerImage is properly defined and used in a deployment"""
    try:
        container_image = lookup_dict(bundle.csv, "metadata.annotations.containerImage")
        if container_image is None:
            yield Fail(
                bundle, f"CSV doesn't define .metadata.annotations.containerImage"
            )
            return
        deployments = lookup_dict(bundle.csv, "spec.install.spec.deployments")
        if deployments is None:
            yield Fail(bundle, f"CSV doesn't define .spec.install.spec.deployments")
            return
        for deployment in deployments:
            containers = lookup_dict(deployment, "spec.template.spec.containers", [])
            if any(container_image == x.get("image") for x in containers):
                return
        yield Fail(
            bundle, f"container image {container_image} not used by any deployment"
        )
    except Exception as e:
        yield Fail(bundle, str(e))


def check_semver(bundle: Bundle) -> Iterator[CheckResult]:
    """Check that the bundle version is semver compliant"""
    try:
        _ = Version.parse(bundle.operator_version)
    except ValueError:
        yield Warn(
            bundle,
            f"Version from filesystem ({bundle.operator_version}) is not valid semver",
        )
    try:
        _ = Version.parse(bundle.csv_operator_version)
    except ValueError:
        yield Warn(
            bundle,
            f"Version from CSV ({bundle.csv_operator_version}) is not valid semver",
        )

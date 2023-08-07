from typing import Iterator, Tuple

from semver import Version

from .. import Bundle
from ..utils import lookup_dict


def check_operator_name(bundle: Bundle) -> Iterator[Tuple[str, str]]:
    name = bundle.annotations.get("operators.operatorframework.io.bundle.package.v1")
    if name is None:
        yield "fail", "Bundle does not define the operator name in annotations.yaml"
        return
    if name != bundle.csv_operator_name:
        yield "fail", f"Operator name from annotations.yaml ({name}) does not match the name defined in the CSV ({bundle.csv_operator_name})"
    if name != bundle.operator_name:
        yield "warn", f"Operator name from annotations.yaml ({name}) does not match the operator's directory name ({bundle.operator_name})"


def check_image(bundle: Bundle) -> Iterator[Tuple[str, str]]:
    try:
        container_image = lookup_dict(bundle.csv, "metadata.annotations.containerImage")
        if container_image is None:
            yield "fail", "CSV doesn't define .metadata.annotations.containerImage"
            return
        deployments = lookup_dict(bundle.csv, "spec.install.spec.deployments")
        if deployments is None:
            yield "fail", "CSV doesn't define .spec.install.spec.deployments"
            return
        for deployment in deployments:
            containers = lookup_dict(deployment, "spec.template.spec.containers", [])
            if any(container_image == x.get("image") for x in containers):
                return
        yield "fail", f"container image {container_image} not used by any deployment"
    except Exception as e:
        yield "fail", str(e)


def check_semver(bundle: Bundle) -> Iterator[Tuple[str, str]]:
    try:
        _ = Version.parse(bundle.operator_version)
    except ValueError:
        yield "warn", f"Version from filesystem ({bundle.operator_version}) is not valid semver"
    try:
        _ = Version.parse(bundle.csv_operator_version)
    except ValueError:
        yield "warn", f"Version from CSV ({bundle.csv_operator_version}) is not valid semver"

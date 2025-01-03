from collections.abc import Iterator

from semantic_version import Version

from .. import Bundle
from ..utils import lookup_dict
from . import CheckResult, Fail, Warn


def _check_consistency(
    my_name: str, all_names: set[str], other_names: set[str], result_description: str
) -> Iterator[CheckResult]:
    """Helper function for check_operator_name"""
    if len(other_names) == 1:
        # Operator names are consistent across other bundles
        common_name = other_names.pop()
        if common_name != my_name:
            # The new bundle has a different operator name
            msg = (
                f"Operator name {result_description} ({my_name})"
                f" does not match the name defined in other"
                f" bundles ({common_name})"
            )
            yield Fail(msg)
    else:
        # Other bundles have inconsistent operator names: let's just issue a warning
        msg = (
            f"Operator name {result_description} is not consistent across bundles:"
            f" {sorted(all_names)}"
        )
        yield Warn(msg)


def check_operator_name(bundle: Bundle) -> Iterator[CheckResult]:
    """Check if the operator names used in CSV, metadata and filesystem are consistent
    in the bundle and across other operator's bundles"""
    if not bundle.metadata_operator_name:
        yield Fail("Bundle does not define the operator name in annotations.yaml")
        return
    all_bundles = set(bundle.operator.all_bundles())
    all_metadata_operator_names = {x.metadata_operator_name for x in all_bundles}
    all_csv_operator_names = {x.csv_operator_name for x in all_bundles}
    other_bundles = all_bundles - {bundle}
    other_metadata_operator_names = {x.metadata_operator_name for x in other_bundles}
    other_csv_operator_names = {x.csv_operator_name for x in other_bundles}
    # Count how many unique names are in use in the CSV and annotations.yaml across
    # all other bundles. Naming is consistent if the count is zero (when the bundle
    # under test is the only bundle for its operator) or one
    consistent_metadata_names = len(other_metadata_operator_names) < 2
    consistent_csv_names = len(other_csv_operator_names) < 2
    if other_bundles:
        yield from _check_consistency(
            bundle.metadata_operator_name,
            all_metadata_operator_names,
            other_metadata_operator_names,
            "from annotations.yaml",
        )
        yield from _check_consistency(
            bundle.csv_operator_name,
            all_csv_operator_names,
            other_csv_operator_names,
            "from the CSV",
        )
    if bundle.metadata_operator_name != bundle.csv_operator_name:
        msg = (
            f"Operator name from annotations.yaml ({bundle.metadata_operator_name})"
            f" does not match the name defined in the CSV ({bundle.csv_operator_name})"
        )
        if consistent_metadata_names and consistent_csv_names:
            yield Fail(msg)
        else:
            yield Warn(msg)
    if bundle.metadata_operator_name != bundle.operator_name:
        msg = (
            f"Operator name from annotations.yaml ({bundle.metadata_operator_name})"
            f" does not match the operator's directory name ({bundle.operator_name})"
        )
        if consistent_metadata_names:
            yield Fail(msg)
        else:
            yield Warn(msg)


def check_image(bundle: Bundle) -> Iterator[CheckResult]:
    """Check if containerImage is properly defined and used in a deployment"""
    try:
        container_image = lookup_dict(bundle.csv, "metadata.annotations.containerImage")
        if container_image is None:
            yield Fail("CSV doesn't define .metadata.annotations.containerImage")
            return
        deployments = lookup_dict(bundle.csv, "spec.install.spec.deployments")
        if deployments is None:
            yield Fail("CSV doesn't define .spec.install.spec.deployments")
            return
        for deployment in deployments:
            containers = lookup_dict(deployment, "spec.template.spec.containers", [])
            if any(container_image == x.get("image") for x in containers):
                return
        yield Fail(f"container image {container_image} not used by any deployment")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        yield Fail(str(exc))


def check_semver(bundle: Bundle) -> Iterator[CheckResult]:
    """Check that the bundle version is semver compliant"""
    try:
        _ = Version.parse(bundle.operator_version)
    except ValueError:
        yield Warn(
            f"Version from filesystem ({bundle.operator_version}) is not valid semver"
        )
    try:
        _ = Version.parse(bundle.csv_operator_version)
    except ValueError:
        yield Warn(
            f"Version from CSV ({bundle.csv_operator_version}) is not valid semver"
        )

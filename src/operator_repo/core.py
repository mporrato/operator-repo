"""
    Definition of Repo, Operator and Bundle classes
"""

import logging
from collections.abc import Iterator
from functools import cached_property, total_ordering
from pathlib import Path
from typing import Any, Optional, Union

from semver import Version

from .exceptions import (
    InvalidBundleException,
    InvalidOperatorException,
    InvalidRepoException,
)
from .utils import load_yaml

log = logging.getLogger(__name__)


@total_ordering
class Bundle:
    """
    An operator bundle as specified in
    https://github.com/operator-framework/operator-registry/blob/master/docs/design/operator-bundle.md
    """

    METADATA_DIR = "metadata"
    MANIFESTS_DIR = "manifests"

    def __init__(
        self, bundle_path: Union[str, Path], operator: Optional["Operator"] = None
    ):
        log.debug("Loading bundle at %s", bundle_path)
        self._bundle_path = Path(bundle_path).resolve()
        if not self.probe(self._bundle_path):
            raise InvalidBundleException(f"Not a valid bundle: {self._bundle_path}")
        self.operator_version = self._bundle_path.name
        self.operator_name = self._bundle_path.parent.name
        self._manifests_path = self._bundle_path / self.MANIFESTS_DIR
        self._metadata_path = self._bundle_path / self.METADATA_DIR
        self._parent = operator

    @cached_property
    def annotations(self) -> dict[str, Any]:
        """
        :return: The content of the "annotations" field in metadata/annotations.yaml
        """
        return self.load_metadata("annotations.yaml").get("annotations", {}) or {}

    @cached_property
    def dependencies(self) -> list[Any]:
        """
        :return: The content of the "dependencies" field in metadata/dependencies.yaml
        """
        return self.load_metadata("dependencies.yaml").get("dependencies", []) or []

    @cached_property
    def csv(self) -> dict[str, Any]:
        """
        :return: The content of the CSV file for the bundle
        """
        csv = load_yaml(self.csv_file_name)
        if not isinstance(csv, dict):
            raise InvalidBundleException(f"Invalid CSV contents ({self.csv_file_name})")
        return csv

    @cached_property
    def csv_full_name(self) -> tuple[str, str]:
        """
        :return: A tuple containging operator name and bundle version
        extracted from the bundle's csv file
        """
        try:
            csv_full_name = self.csv["metadata"]["name"]
            name, version = csv_full_name.split(".", 1)
            return name, version.lstrip("v")
        except (KeyError, ValueError) as exc:
            raise InvalidBundleException(
                f"CSV for {self} has invalid .metadata.name"
            ) from exc

    @property
    def csv_operator_name(self) -> str:
        """
        :return: The operator name from the csv file
        """
        name, _ = self.csv_full_name
        return name

    @property
    def csv_operator_version(self) -> str:
        """
        :return: The bundle version from the csv file
        """
        _, version = self.csv_full_name
        return version

    @classmethod
    def probe(cls, path: Path) -> bool:
        """
        :return: True if path looks like a bundle
        """
        return (
            path.is_dir()
            and (path / cls.MANIFESTS_DIR).is_dir()
            and (path / cls.METADATA_DIR).is_dir()
        )

    @property
    def root(self) -> Path:
        """
        :return: The path to the root of the bundle
        """
        return self._bundle_path

    @property
    def operator(self) -> "Operator":
        """
        :return: The Operator object the bundle belongs to
        """
        if self._parent is None:
            self._parent = Operator(self._bundle_path.parent)
        return self._parent

    def load_metadata(self, filename: str) -> dict[str, Any]:
        """
        Load and parse a yaml file from the metadata directory of the bundle
        :param filename: Name of the file
        :return: The parsed content of the file
        """
        try:
            content = load_yaml(self._metadata_path / filename)
            if not isinstance(content, dict):
                if content is None:
                    return {}
                raise InvalidBundleException(f"Invalid {filename} contents")
            return content
        except FileNotFoundError:
            return {}

    @cached_property
    def csv_file_name(self) -> Path:
        """
        :return: The path of the CSV file for the bundle
        """
        for suffix in ["yaml", "yml"]:
            try:
                return next(
                    self._manifests_path.glob(f"*.clusterserviceversion.{suffix}")
                )
            except StopIteration:
                continue
        raise InvalidBundleException(
            f"CSV file for {self.operator_name}/{self.operator_version} not found"
        )

    @property
    def channels(self) -> set[str]:
        """
        :return: Set of channels the bundle belongs to
        """
        try:
            return {
                x.strip()
                for x in self.annotations[
                    "operators.operatorframework.io.bundle.channels.v1"
                ].split(",")
            }
        except KeyError:
            return set()

    @property
    def default_channel(self) -> Optional[str]:
        """
        :return: Default channel for the bundle
        """
        try:
            return str(
                self.annotations[
                    "operators.operatorframework.io.bundle.channel.default.v1"
                ]
            ).strip()
        except KeyError:
            return None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.csv_operator_name != other.csv_operator_name:
            return False
        try:
            return Version.parse(
                self.csv_operator_version.lstrip("v")
            ) == Version.parse(other.csv_operator_version.lstrip("v"))
        except ValueError:
            log.warning(
                "Can't compare bundle versions %s and %s as semver: using lexical order instead",
                self,
                other,
            )
            return self.csv_operator_version == other.csv_operator_version

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError(
                f"< not supported between instances of '{self.__class__.__name__}'"
                f" and '{other.__class__.__name__}"
            )
        if self.csv_operator_name != other.csv_operator_name:
            return self.csv_operator_name < other.csv_operator_name
        try:
            return Version.parse(self.csv_operator_version.lstrip("v")) < Version.parse(
                other.csv_operator_version.lstrip("v")
            )
        except ValueError:
            log.warning(
                "Can't compare bundle versions %s and %s as semver: using lexical order instead",
                self,
                other,
            )
            return self.csv_operator_version < other.csv_operator_version

    def __hash__(self) -> int:
        return hash((self.operator_name, self.operator_version))

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.operator_name}/{self.operator_version})"
        )


@total_ordering
class Operator:
    """An operator containing a collection of bundles"""

    _bundle_cache: dict[str, Bundle]

    def __init__(self, operator_path: Union[str, Path], repo: Optional["Repo"] = None):
        log.debug("Loading operator at %s", operator_path)
        self._operator_path = Path(operator_path).resolve()
        if not self.probe(self._operator_path):
            raise InvalidOperatorException(
                f"Not a valid operator: {self._operator_path}"
            )
        self.operator_name = self._operator_path.name
        self._parent = repo
        self._bundle_cache = {}

    @cached_property
    def config(self) -> Any:
        """
        :return: The contents of the ci.yaml for the operator
        """
        try:
            return load_yaml(self._operator_path / "ci.yaml")
        except FileNotFoundError:
            log.info("No ci.yaml found for %s", self)
            return {}

    @classmethod
    def probe(cls, path: Path) -> bool:
        """
        :return: True if path looks like an operator
        """
        return path.is_dir() and any(Bundle.probe(x) for x in path.iterdir())

    @property
    def root(self) -> Path:
        """
        :return: The path to the root of the operator
        """
        return self._operator_path

    @property
    def repo(self) -> "Repo":
        """
        :return: The Repo object the operator belongs to
        """
        if self._parent is None:
            self._parent = Repo(self._operator_path.parent.parent)
        return self._parent

    def all_bundles(self) -> Iterator[Bundle]:
        """
        :return: All the bundles for the operator
        """
        for version_path in self._operator_path.iterdir():
            try:
                yield self._bundle_cache[version_path.name]
            except KeyError:
                if Bundle.probe(version_path):
                    yield self.bundle(version_path.name)

    def bundle_path(self, operator_version: str) -> Path:
        """
        Return the path where a bundle for the given version
        would be located
        :param operator_version: Version of the bundle
        :return: Path to the bundle
        """
        return self._operator_path / operator_version

    def bundle(self, operator_version: str) -> Bundle:
        """
        Load the bundle for the given version
        :param operator_version: Version of the bundle
        :return: The loaded bundle
        """
        try:
            return self._bundle_cache[operator_version]
        except KeyError:
            bundle = Bundle(self.bundle_path(operator_version), self)
            self._bundle_cache[operator_version] = bundle
            return bundle

    def has(self, operator_version: str) -> bool:
        """
        Check if the operator has a bundle for the given version
        :param operator_version: Version to check for
        :return: True if the operator contains a bundle for such version
        """
        return operator_version in self._bundle_cache or Bundle.probe(
            self.bundle_path(operator_version)
        )

    @cached_property
    def channels(self) -> set[str]:
        """
        :return: All channels defined by the operator bundles
        """
        return {x for y in self.all_bundles() for x in y.channels}

    @cached_property
    def default_channel(self) -> Optional[str]:
        """
        :return: Default channel as defined in
        https://github.com/operator-framework/operator-registry/blob/master/docs/design/opm-tooling.md
        """
        # The default channel for an operator is defined as the default
        # channel of the highest bundle version
        try:
            version_channel_pairs: list[tuple[Union[str, Version], str]] = [
                (
                    Version.parse(x.csv_operator_version),
                    x.default_channel,
                )
                for x in self.all_bundles()
                if x.default_channel is not None
            ]
        except ValueError:
            log.warning(
                "%s has bundles with non-semver compliant version:"
                " using lexical order to determine default channel",
                self,
            )
            version_channel_pairs = [
                (
                    x.csv_operator_version,
                    x.default_channel,
                )
                for x in self.all_bundles()
                if x.default_channel is not None
            ]
        try:
            return sorted(version_channel_pairs)[-1][1]
        except IndexError:
            return None

    def channel_bundles(self, channel: str) -> list[Bundle]:
        """
        :param channel: Name of the channel
        :return: List of bundles in the given channel
        """
        return sorted({x for x in self.all_bundles() if channel in x.channels})

    def head(self, channel: str) -> Bundle:
        """
        :param channel: Name of the channel
        :return: Head of the channel
        """
        return self.channel_bundles(channel)[-1]

    @staticmethod
    def _replaces_graph(
        channel: str, bundles: list[Bundle]
    ) -> dict[Bundle, set[Bundle]]:
        edges: dict[Bundle, set[Bundle]] = {}
        all_bundles_set = set(bundles)
        version_to_bundle = {x.csv_operator_version: x for x in all_bundles_set}
        for bundle in all_bundles_set:
            spec = bundle.csv.get("spec", {})
            replaces = spec.get("replaces")
            skips = spec.get("skips", [])
            previous = set(skips) | {replaces}
            for replaced_bundle_name in previous:
                if replaced_bundle_name is None:
                    continue
                if "." not in replaced_bundle_name:
                    raise ValueError(
                        f"{bundle} has invalid 'replaces' field: '{replaced_bundle_name}'"
                    )
                (
                    replaced_bundle_operator,
                    replaced_bundle_version,
                ) = replaced_bundle_name.split(".", 1)
                if replaced_bundle_operator != bundle.csv_operator_name:
                    raise ValueError(
                        f"{bundle} replaces a bundle from a different operator"
                    )
                try:
                    replaced_bundle = version_to_bundle[
                        replaced_bundle_version.lstrip("v")
                    ]
                    if (
                        channel in bundle.channels
                        and channel in replaced_bundle.channels
                    ):
                        edges.setdefault(replaced_bundle, set()).add(bundle)
                except KeyError:
                    pass
        return edges

    def update_graph(self, channel: str) -> dict[Bundle, set[Bundle]]:
        """
        Return the update graph for the given channel
        :param channel: Name of the channel
        :return: Update graph edges in the form of a dictionary mapping each bundle
            to a set of bundles that can replace it
        """
        all_bundles = self.channel_bundles(channel)
        update_strategy = self.config.get("updateGraph", "replaces-mode")
        operator_names = {x.csv_operator_name for x in all_bundles}
        if len(operator_names) > 1:
            raise ValueError(
                f"{self} has bundles with different operator names: {operator_names}"
            )
        if update_strategy == "semver-mode":
            return {x: {y} for x, y in zip(all_bundles, all_bundles[1:])}
        if update_strategy == "replaces-mode":
            return self._replaces_graph(channel, all_bundles)
        raise NotImplementedError(
            f"{self}: unsupported updateGraph value: {update_strategy}"
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.operator_name == other.operator_name

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError(
                f"Can't compare {self.__class__.__name__} to {other.__class__.__name__}"
            )
        return self.operator_name < other.operator_name

    def __iter__(self) -> Iterator[Bundle]:
        yield from self.all_bundles()

    def __hash__(self) -> int:
        return hash((self.operator_name,))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.operator_name})"


class Repo:
    """A repository containing a collection of operators"""

    _operator_cache: dict[str, Operator]

    OPERATORS_DIR = "operators"

    def __init__(self, repo_path: Union[str, Path]):
        log.debug("Loading repo at %s", repo_path)
        self._repo_path = Path(repo_path).resolve()
        if not self.probe(self._repo_path):
            raise InvalidRepoException(
                f"Not a valid operator repository: {self._repo_path}"
            )
        self._operators_path = self._repo_path / self.OPERATORS_DIR
        self._operator_cache = {}

    @cached_property
    def config(self) -> Any:
        """
        :return: The contents of the ci/pipeline-config.yaml for the repo
        """
        try:
            return load_yaml(self._repo_path / "ci" / "pipeline-config.yaml")
        except FileNotFoundError:
            log.warning("No ci/pipeline-config.yaml found for %s", self)
            return {}

    @classmethod
    def probe(cls, path: Path) -> bool:
        """
        :return: True if path looks like an operator repo
        """
        return path.is_dir() and (path / cls.OPERATORS_DIR).is_dir()

    @property
    def root(self) -> Path:
        """
        :return: The path to the root of the repository
        """
        return self._repo_path

    def all_operators(self) -> Iterator[Operator]:
        """
        :return: All the operators in the repo
        """
        for operator_path in self._operators_path.iterdir():
            try:
                yield self._operator_cache[operator_path.name]
            except KeyError:
                if Operator.probe(operator_path):
                    yield self.operator(operator_path.name)

    def operator_path(self, operator_name: str) -> Path:
        """
        Return the path where an operator with the given
        name would be located
        :param operator_name: Name of the operator
        :return: Path to the operator
        """
        return self._operators_path / operator_name

    def operator(self, operator_name: str) -> Operator:
        """
        Load the operator with the given name
        :param operator_name: Name of the operator
        :return: The loaded operator
        """
        try:
            return self._operator_cache[operator_name]
        except KeyError:
            operator = Operator(self.operator_path(operator_name), self)
            self._operator_cache[operator_name] = operator
            return operator

    def has(self, operator_name: str) -> bool:
        """
        Check if the repo contains an operator with the given name
        :param operator_name: Name of the operator to look for
        :return: True if the repo contains an operator with the given name
        """
        return operator_name in self._operator_cache or Operator.probe(
            self.operator_path(operator_name)
        )

    def __iter__(self) -> Iterator[Operator]:
        yield from self.all_operators()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self._repo_path == other._repo_path

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._repo_path})"

"""
    Definition of Repo, Operator and Bundle classes
"""

import logging
from functools import cached_property, total_ordering
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Union, Tuple

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

    def __init__(self, bundle_path: Union[str, Path]):
        log.debug("Loading bundle at %s", bundle_path)
        self._bundle_path = Path(bundle_path).resolve()
        if not self.probe(self._bundle_path):
            raise InvalidBundleException(f"Not a valid bundle: {self._bundle_path}")
        self.operator_version = self._bundle_path.name
        self.operator_name = self._bundle_path.parent.name
        self._manifests_path = self._bundle_path / self.MANIFESTS_DIR
        self._metadata_path = self._bundle_path / self.METADATA_DIR

    @cached_property
    def annotations(self) -> Dict[str, Any]:
        """
        :return: The content of the "annotations" field in metadata/annotations.yaml
        """
        return self.load_metadata("annotations.yaml").get("annotations", {})

    @cached_property
    def dependencies(self) -> List[Any]:
        """
        :return: The content of the "dependencies" field in metadata/dependencies.yaml
        """
        return self.load_metadata("dependencies.yaml").get("dependencies", [])

    @cached_property
    def csv(self) -> Dict[str, Any]:
        """
        :return: The content of the CSV file for the bundle
        """
        csv = load_yaml(self.csv_file_name)
        if not isinstance(csv, dict):
            raise InvalidBundleException(f"Invalid CSV contents ({self.csv_file_name})")
        return csv

    @cached_property
    def csv_full_name(self) -> Tuple[str, str]:
        try:
            csv_full_name = self.csv["metadata"]["name"]
            name, version = csv_full_name.split(".", 1)
            return name, version.lstrip('v')
        except (KeyError, ValueError) as exc:
            raise InvalidBundleException(
                f"CSV for {self} has invalid .metadata.name"
            ) from exc

    @property
    def csv_operator_name(self) -> str:
        name, _ = self.csv_full_name
        return name

    @property
    def csv_operator_version(self) -> str:
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
    def root(self) -> str:
        """
        :return: The path to the root of the bundle
        """
        return str(self._bundle_path)

    def operator(self) -> "Operator":
        """
        :return: The operator the bundle belongs to
        """
        return Operator(self._bundle_path.parent)

    def load_metadata(self, filename: str) -> Dict[str, Any]:
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
                return next(self._manifests_path.glob(f"*.clusterserviceversion.{suffix}"))
            except StopIteration:
                continue
        raise InvalidBundleException(
            f"CSV file for {self.operator_name}/{self.operator_version} not found"
        )

    @property
    def channels(self) -> Set[str]:
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
            return self.annotations[
                "operators.operatorframework.io.bundle.channel.default.v1"
            ].strip()
        except KeyError:
            return None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError(
                f"== not supported between instances of '{self.__class__.__name__}'"
                f" and '{other.__class__.__name__}"
            )
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

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError(
                f"< not supported between instances of '{self.__class__.__name__}'"
                f" and '{other.__class__.__name__}"
            )
        if self.csv_operator_name != other.csv_operator_name:
            raise ValueError("Can't compare bundles from different operators")
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

    def __hash__(self):
        return hash((self.operator_name, self.operator_version))

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.operator_name}/{self.operator_version})"
        )


@total_ordering
class Operator:
    """An operator containing a collection of bundles"""

    def __init__(self, operator_path: Union[str, Path]):
        log.debug("Loading operator at %s", operator_path)
        self._operator_path = Path(operator_path).resolve()
        if not self.probe(self._operator_path):
            raise InvalidOperatorException(
                f"Not a valid operator: {self._operator_path}"
            )
        self.operator_name = self._operator_path.name

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
    def root(self) -> str:
        """
        :return: The path to the root of the operator
        """
        return str(self._operator_path)

    def all_bundles(self) -> Iterator[Bundle]:
        """
        :return: All the bundles for the operator
        """
        for version_path in self._operator_path.iterdir():
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
        return Bundle(self.bundle_path(operator_version))

    def has(self, operator_version: str) -> bool:
        """
        Check if the operator has a bundle for the given version
        :param operator_version: Version to check for
        :return: True if the operator contains a bundle for such version
        """
        return Bundle.probe(self.bundle_path(operator_version))

    @cached_property
    def channels(self) -> Set[str]:
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
        try:
            return sorted(
                [
                    (
                        Version.parse(x.csv_operator_version.lstrip("v")),
                        x.default_channel,
                    )
                    for x in self.all_bundles()
                    if x.default_channel is not None
                ]
            )[-1][1]
        except IndexError:
            return None

    def channel_bundles(self, channel: str) -> List[Bundle]:
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

    def update_graph(self, channel: str) -> Dict[Bundle, Set[Bundle]]:
        """
        Return the update graph for the given channel
        :param channel: Name of the channel
        :return: Update graph edges in the form of a dictionary mapping each bundle
            to a set of bundles that can replace it
        """
        all_bundles = self.channel_bundles(channel)
        update_strategy = self.config.get("updateGraph", "replaces-mode")
        if update_strategy == "semver-mode":
            return {x: {y} for x, y in zip(all_bundles, all_bundles[1:])}
        if update_strategy == "semver-skippatch":
            # TODO: implement semver-skippatch
            raise NotImplementedError("%s: semver-skippatch is not implemented yet")
        if update_strategy == "replaces-mode":
            edges: Dict[Bundle, set[Bundle]] = {}
            all_bundles_set = set(all_bundles)
            for bundle in all_bundles_set:
                try:
                    spec = bundle.csv["spec"]
                except KeyError:
                    continue
                replaces = spec.get("replaces")
                skips = spec.get("skips", [])
                for replaced_bundle_name in skips + [replaces]:
                    if replaced_bundle_name is None:
                        continue
                    if ".v" not in replaced_bundle_name:
                        raise ValueError(
                            f"{bundle} has invalid 'replaces' field: '{replaced_bundle_name}'"
                        )
                    (
                        replaced_bundle_operator,
                        replaced_bundle_version,
                    ) = replaced_bundle_name.split(".", 1)
                    if replaced_bundle_operator != bundle.csv_operator_name:
                        raise ValueError(
                            f"{self}: {bundle} replaces a bundle from a different operator"
                        )
                    try:
                        replaced_bundle = self.bundle(
                            replaced_bundle_version.lstrip("v")
                        )
                        edges.setdefault(replaced_bundle, set()).add(bundle)
                    except InvalidBundleException:
                        pass
            return edges
        raise ValueError(f"{self}: unknown updateGraph value: {update_strategy}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            raise NotImplementedError(
                f"Can't compare {self.__class__.__name__} to {other.__class__.__name__}"
            )
        return self.operator_name == other.operator_name

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            raise NotImplementedError(
                f"Can't compare {self.__class__.__name__} to {other.__class__.__name__}"
            )
        return self.operator_name != other.operator_name

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            raise NotImplementedError(
                f"Can't compare {self.__class__.__name__} to {other.__class__.__name__}"
            )
        return self.operator_name < other.operator_name

    def __iter__(self) -> Iterator[Bundle]:
        yield from self.all_bundles()

    def __hash__(self):
        return hash((self.operator_name,))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.operator_name})"


class Repo:
    """A repository containing a collection of operators"""

    OPERATORS_DIR = "operators"

    def __init__(self, repo_path: Union[str, Path]):
        log.debug("Loading repo at %s", repo_path)
        self._repo_path = Path(repo_path).resolve()
        if not self.probe(self._repo_path):
            raise InvalidRepoException(
                f"Not a valid operator repository: {self._repo_path}"
            )
        self._operators_path = self._repo_path / self.OPERATORS_DIR

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
    def root(self) -> str:
        """
        :return: The path to the root of the repository
        """
        return str(self._repo_path)

    def all_operators(self) -> Iterator[Operator]:
        """
        :return: All the operators in the repo
        """
        for operator_path in self._operators_path.iterdir():
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
        return Operator(self.operator_path(operator_name))

    def has(self, operator_name: str) -> bool:
        """
        Check if the repo contains an operator with the given name
        :param operator_name: Name of the operator to look for
        :return: True if the repo contains an operator with the given name
        """
        return Operator.probe(self.operator_path(operator_name))

    def __iter__(self) -> Iterator[Operator]:
        yield from self.all_operators()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._repo_path})"

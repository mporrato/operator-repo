#!/usr/bin/env python3
"""
    CLI tool to handle operator repositories
"""

import argparse
import logging
from itertools import chain
from pathlib import Path
from typing import Union, Dict, Any, Iterator, Tuple

from operator_repo.classes import Bundle, Operator, Repo


def parse_target(repo: Repo, target: str) -> Union[Operator, Bundle]:
    if "/" in target:
        operator_name, operator_version = target.split("/", 1)
        return repo.operator(operator_name).bundle(operator_version)
    return repo.operator(target)


def indent(depth: int) -> str:
    return "  " * depth


def _list(
    target: Union[Repo, Operator, Bundle], recursive: bool = False, depth: int = 0
) -> None:
    if isinstance(target, Repo):
        print(indent(depth) + str(target))
        for operator in target:
            if recursive:
                _list(operator, True, depth + 1)
            else:
                print(indent(depth + 1) + str(operator))
    elif isinstance(target, Operator):
        print(indent(depth) + str(target))
        for bundle in target:
            if recursive:
                _list(bundle, True, depth + 1)
            else:
                print(indent(depth + 1) + str(bundle))
    elif isinstance(target, Bundle):
        print(indent(depth) + str(target))
        csv_annotations = target.csv.get("metadata", {}).get("annotations", {})
        info = [
            ("Description", csv_annotations.get("description", "")),
            ("Name", f"{target.csv_operator_name}.{target.csv_operator_version}"),
            ("Channels", ", ".join(target.channels)),
            ("Default channel", target.default_channel),
            ("Container image", csv_annotations.get("containerImage", "")),
        ]
        max_width = max([len(key) for key, _ in info])
        for key, value in info:
            message = f"{key.ljust(max_width+1)}: {value}"
            print(indent(depth + 1) + message)


def action_list(repo_path, *what: str, recursive: bool = False) -> None:
    repo = Repo(repo_path)
    if not what:
        _list(repo, recursive)
    else:
        for target in what:
            _list(parse_target(repo, target), recursive)


def lookup_dict(data: Dict[str, Any], path: str, default: Any = None, separator: str = '.') -> Any:
    keys = path.split(separator)
    subtree = data
    for key in keys:
        if key not in subtree:
            return default
        subtree = subtree[key]
    return subtree


def do_check_bundle_operator_name(bundle: Bundle) -> Iterator[Tuple[str, str]]:
    name = bundle.annotations.get("operators.operatorframework.io.bundle.package.v1")
    if name is None:
        yield "fail", "Bundle does not define the operator name in annotations.yaml"
        return
    if name != bundle.csv_operator_name:
        yield "fail", "Operator name from annotations.yaml does not match the name defined in the CSV"
    if name != bundle.operator_name:
        yield "fail", "Operator name from annotations.yaml does not match the operator's directory name"


def do_check_bundle_image(bundle: Bundle) -> Iterator[Tuple[str, str]]:
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


def action_check_bundles(repo_path: Path, *what: str) -> None:
    for bundle_name in what:
        print(f"Checking {bundle_name}")
        bundle = parse_target(Repo(repo_path), bundle_name)
        for result, message in chain(do_check_bundle_image(bundle), do_check_bundle_operator_name(bundle)):
            print(f"{result.upper()}: {message}")


def main() -> None:
    main_parser = argparse.ArgumentParser(
        description="Operator repository manipulation tool",
    )
    main_parser.add_argument(
        "-r", "--repo", help="path to the root of the operator repository", type=Path
    )
    main_parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="increase log verbosity"
    )
    main_subparsers = main_parser.add_subparsers(dest="action")

    # list
    list_parser = main_subparsers.add_parser(
        "list", help="list contents of repo, operators or bundles"
    )
    list_parser.add_argument(
        "-R", "--recursive", action="store_true", help="descend the tree"
    )
    list_parser.add_argument(
        "target",
        nargs="*",
        help="name of the repos or bundles to list; if omitted, list the contents of the repo",
    )

    # check_bundle
    check_bundle_parser = main_subparsers.add_parser(
        "check-bundle", help="check validity of a bundle"
    )
    check_bundle_parser.add_argument(
        "target",
        nargs="*",
        help="name of the bundles to check",
    )

    args = main_parser.parse_args()
    # print(args)

    verbosity = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO}
    log = logging.getLogger(__package__)
    log.setLevel(verbosity.get(args.verbose, logging.DEBUG))
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    )
    log.addHandler(handler)

    if args.action == "list":
        action_list(args.repo or Path.cwd(), *args.target, recursive=args.recursive)
    elif args.action == "check-bundle":
        action_check_bundles(args.repo or Path.cwd(), *args.target)
    else:
        main_parser.print_help()


if __name__ == "__main__":
    main()

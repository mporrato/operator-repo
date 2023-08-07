#!/usr/bin/env python3
"""
    CLI tool to handle operator repositories
"""

import argparse
import logging
from inspect import getmembers, isfunction
from pathlib import Path
from typing import Union

from .checks import bundle as bundle_checks
from .checks import operator as operator_checks
from .classes import Bundle, Operator, Repo


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
            ("Replaces", target.csv.get("spec", {}).get("replaces", "")),
            ("Skips", target.csv.get("spec", {}).get("skips", [])),
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


def action_check_bundle(bundle: Bundle) -> None:
    print(f"Checking {bundle}")
    for check_name, check in getmembers(bundle_checks, isfunction):
        if check_name.startswith("check_"):
            for result, message in check(bundle):
                print(f"{result.upper()}: {bundle}: {message}")


def action_check_operator(operator: Operator) -> None:
    print(f"Checking {operator}")
    for check_name, check in getmembers(operator_checks, isfunction):
        if check_name.startswith("check_"):
            for result, message in check(operator):
                print(f"{result.upper()}: {operator}: {message}")


def action_check(repo_path: Path, *what: str, recursive: bool = False) -> None:
    repo = Repo(repo_path)
    for target in [parse_target(repo, x) for x in what] or sorted(repo):
        if isinstance(target, Operator):
            action_check_operator(target)
            if recursive:
                for bundle in sorted(target):
                    action_check_bundle(bundle)
        elif isinstance(target, Bundle):
            action_check_bundle(target)


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
        "list", aliases=["ls"], help="list contents of repo, operators or bundles"
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
    check_parser = main_subparsers.add_parser(
        "check",
        help="check validity of an operator or bundle",
    )
    check_parser.add_argument(
        "-R", "--recursive", action="store_true", help="descend the tree"
    )
    check_parser.add_argument(
        "target",
        nargs="*",
        help="name of the operators or bundles to check",
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

    if args.action in ("list", "ls"):
        action_list(args.repo or Path.cwd(), *args.target, recursive=args.recursive)
    elif args.action == "check":
        action_check(args.repo or Path.cwd(), *args.target, recursive=args.recursive)
    else:
        main_parser.print_help()


if __name__ == "__main__":
    main()

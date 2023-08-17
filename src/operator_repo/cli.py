#!/usr/bin/env python3
"""
    CLI tool to handle operator repositories
"""

import argparse
import logging
import sys
from collections.abc import Iterator
from itertools import chain
from pathlib import Path
from typing import Optional, Union

from .checks import get_checks, run_suite
from .classes import Bundle, Operator, Repo
from .exceptions import OperatorRepoException


def parse_target(repo: Repo, target: str) -> Union[Operator, Bundle]:
    if "/" in target:
        operator_name, operator_version = target.split("/", 1)
        return repo.operator(operator_name).bundle(operator_version)
    return repo.operator(target)


def indent(depth: int) -> str:
    return "  " * depth


def show_repo(repo: Repo, recursive: bool = False, depth: int = 0) -> None:
    print(indent(depth) + str(repo))
    for operator in repo:
        if recursive:
            show_operator(operator, recursive, depth + 1)
        else:
            print(indent(depth + 1) + str(operator))


def show_operator(operator: Operator, recursive: bool = False, depth: int = 0) -> None:
    print(indent(depth) + str(operator))
    for bundle in operator:
        if recursive:
            show_bundle(bundle, recursive, depth + 1)
        else:
            print(indent(depth + 1) + str(bundle))


def show_bundle(bundle: Bundle, recursive: bool = False, depth: int = 0) -> None:
    print(indent(depth) + str(bundle))
    csv_annotations = bundle.csv.get("metadata", {}).get("annotations", {})
    info = [
        ("Description", csv_annotations.get("description", "")),
        ("Name", f"{bundle.csv_operator_name}.v{bundle.csv_operator_version}"),
        ("Channels", ", ".join(bundle.channels)),
        ("Default channel", bundle.default_channel),
        ("Container image", csv_annotations.get("containerImage", "")),
        ("Replaces", bundle.csv.get("spec", {}).get("replaces", "")),
        ("Skips", bundle.csv.get("spec", {}).get("skips", [])),
    ]
    max_width = max(len(key) for key, _ in info)
    for key, value in info:
        message = f"{key.ljust(max_width + 1)}: {value}"
        print(indent(depth + 1) + message)


def show(target: Union[Repo, Operator, Bundle], recursive: bool = False) -> None:
    if isinstance(target, Repo):
        show_repo(target, recursive, 0)
    elif isinstance(target, Operator):
        show_operator(target, recursive, 1 * recursive)
    elif isinstance(target, Bundle):
        show_bundle(target, recursive, 2 * recursive)


def action_list(repo_path, *what: str, recursive: bool = False) -> None:
    repo = Repo(repo_path)
    if what:
        targets = (parse_target(repo, x) for x in what)
    else:
        targets = [repo]
    for target in targets:
        show(target, recursive)


def _walk(
    target: Union[Repo, Operator, Bundle]
) -> Iterator[Union[Repo, Operator, Bundle]]:
    yield target
    if isinstance(target, Repo):
        for operator in target:
            yield from _walk(operator)
    elif isinstance(target, Operator):
        yield from target.all_bundles()


def action_check(
    repo_path: Path, suite: str, *what: str, recursive: bool = False
) -> None:
    repo = Repo(repo_path)
    if what:
        targets = [parse_target(repo, x) for x in what]
    else:
        targets = repo.all_operators()
    if recursive:
        all_targets = chain.from_iterable(_walk(x) for x in targets)
    else:
        all_targets = targets
    for result in run_suite(all_targets, suite_name=suite):
        print(result)


def action_check_list(suite: str) -> None:
    for check_type_name, checks in get_checks(suite).items():
        print(f"{check_type_name} checks:")
        for check in checks:
            display_name = check.__name__.removeprefix("check_")
            print(f" - {display_name}: {check.__doc__}")


def _get_repo(path: Optional[Path]) -> Repo:
    if not path:
        path = Path.cwd()
    try:
        return Repo(path)
    except OperatorRepoException:
        print(f"{path} is not a valid operator repository")
        sys.exit(1)


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
        "-s", "--suite", default="operator_repo.checks", help="check suite to use"
    )
    check_parser.add_argument(
        "-l", "--list", action="store_true", help="list available checks"
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

    verbosity = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO}
    log = logging.getLogger(__package__)
    log.setLevel(verbosity.get(args.verbose, logging.DEBUG))
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    )
    log.addHandler(handler)

    if args.action in ("list", "ls"):
        action_list(_get_repo(args.repo), *args.target, recursive=args.recursive)
    elif args.action == "check":
        if args.list:
            action_check_list(args.suite)
        else:
            action_check(
                _get_repo(args.repo),
                args.suite,
                *args.target,
                recursive=args.recursive,
            )
    else:
        main_parser.print_help()


if __name__ == "__main__":
    main()

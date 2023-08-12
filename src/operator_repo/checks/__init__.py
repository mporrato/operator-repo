import importlib
from collections.abc import Callable, Iterable, Mapping
from inspect import getmembers, isfunction
from typing import Union

from .. import Bundle, Operator, Repo


class CheckResult:
    severity: int = 0
    display: str = "UNKNOWN"
    origin: Union[Repo, Operator, Bundle, None]
    reason: str

    def __init__(self, origin, reason: str):
        self.origin = origin
        self.reason = reason

    def __str__(self):
        return f"{self.display}: {self.origin}: {self.reason}"

    def __repr__(self):
        return f"{self.display}({self.origin}, {self.reason})"

    def __int__(self):
        return self.severity

    def __lt__(self, other):
        return int(self) < int(other)


class Warn(CheckResult):
    severity = 40
    display = "WARNING"


class Fail(CheckResult):
    severity = 90
    display = "FAILURE"


SUPPORTED_TYPES = [("operator", Operator), ("bundle", Bundle)]
Check = Callable[[Operator | Bundle], Iterable[CheckResult]]


def get_checks(
    suite_name: str = "operator_repo.checks",
) -> Mapping[str, Iterable[Check]]:
    result = {}
    for module_name, _ in SUPPORTED_TYPES:
        result[module_name] = []
        try:
            module = importlib.import_module(f"{suite_name}.{module_name}")
            for check_name, check in getmembers(module, isfunction):
                if check_name.startswith("check_"):
                    result[module_name].append(check)
        except ModuleNotFoundError:
            pass
    return result


def run_suite(
    targets: Iterable[Repo | Operator | Bundle],
    suite_name: str = "operator_repo.checks",
) -> Iterable[CheckResult]:
    checks = get_checks(suite_name)
    for target in targets:
        for target_type_name, target_type in SUPPORTED_TYPES:
            if isinstance(target, target_type):
                for check in checks.get(target_type_name, []):
                    yield from check(target)

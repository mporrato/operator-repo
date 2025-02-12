from unittest.mock import MagicMock, call, patch

from operator_repo import Bundle
from operator_repo.checks import Fail, Warn, get_checks, run_check, run_suite


def test_check_result() -> None:
    result1 = Warn("foo")
    result2 = Fail("bar")
    result3 = Warn("foo")
    assert result1 != result2
    assert result1 < result2
    assert result1 == result3
    assert "foo" in str(result1)
    assert "bar" in str(result2)
    assert "foo" in repr(result1)
    assert "bar" in repr(result2)
    assert "warning" in str(result1)
    assert "error" in str(result2)
    assert {result1, result2, result3} == {result1, result2}


@patch("importlib.import_module")
def test_get_checks(mock_import_module: MagicMock) -> None:
    def check_fake(_something):  # type: ignore
        pass

    fake_module = MagicMock()
    fake_module.check_fake = check_fake
    fake_module.non_check_bar = lambda x: None
    mock_import_module.return_value = fake_module
    assert get_checks("suite.name") == {
        "operator": [check_fake],
        "bundle": [check_fake],
        "operator_catalogs": [check_fake],
    }
    mock_import_module.assert_has_calls(
        [call("suite.name.operator"), call("suite.name.bundle")], any_order=True
    )


@patch("importlib.import_module")
def test_get_checks_skip_check(mock_import_module: MagicMock) -> None:
    def check_fake(_something):  # type: ignore
        pass

    fake_module = MagicMock()
    fake_module.check_fake = check_fake
    fake_module.check_ignore = lambda x: None
    mock_import_module.return_value = fake_module
    assert get_checks("suite.name", skip_tests=["check_ignore"]) == {
        "operator": [check_fake],
        "bundle": [check_fake],
        "operator_catalogs": [check_fake],
    }
    mock_import_module.assert_has_calls(
        [call("suite.name.operator"), call("suite.name.bundle")], any_order=True
    )


@patch("importlib.import_module")
def test_get_checks_missing_modules(mock_import_module: MagicMock) -> None:
    mock_import_module.side_effect = ModuleNotFoundError()
    assert get_checks("suite.name") == {
        "operator": [],
        "bundle": [],
        "operator_catalogs": [],
    }
    mock_import_module.assert_has_calls(
        [call("suite.name.operator"), call("suite.name.bundle")], any_order=True
    )


def test_run_check(mock_bundle: Bundle) -> None:
    def check_fake(_something):  # type: ignore
        yield Warn("foo")

    exception = Exception("bar")

    def check_with_exception(_something):  # type: ignore
        raise exception

    assert list(run_check(check_fake, mock_bundle, "fake-suite")) == [
        Warn("foo", "check_fake", mock_bundle, "fake-suite")
    ]

    results = list(run_check(check_with_exception, mock_bundle, "fake-suite"))
    assert results == [
        Fail(
            # Copying the exception message
            results[0].reason,
            "check_with_exception",
            mock_bundle,
            "fake-suite",
        )
    ]


@patch("operator_repo.checks.get_checks")
def test_run_suite(mock_get_checks: MagicMock, mock_bundle: Bundle) -> None:
    def check_fake(_something):  # type: ignore
        yield Warn("foo")

    def check_fake_err(_something):  # type: ignore
        yield Fail("foo")

    mock_get_checks.return_value = {
        "bundle": [check_fake, check_fake_err],
        "operator": [],
    }
    assert sorted(list(run_suite([mock_bundle], "fake.suite"))) == sorted(
        [
            Fail("foo", "check_fake_err", mock_bundle, "fake.suite"),
            Warn("foo", "check_fake", mock_bundle, "fake.suite"),
        ]
    )

    assert list(run_suite([mock_bundle], "fake.suite", ["check_fake_err"])) == [
        Fail("foo", "check_fake_err", mock_bundle, "fake.suite")
    ]

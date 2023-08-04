from pathlib import Path

from operator_repo import Repo
from tests import bundle_files, create_files


def test_operator_empty(tmp_path: Path) -> None:
    create_files(tmp_path, {"operators/hello/readme.txt": "hello"})
    repo = Repo(tmp_path)
    assert not repo.has("hello")


def test_operator_one_bundle(tmp_path: Path) -> None:
    create_files(tmp_path, bundle_files("hello", "0.0.1"))
    repo = Repo(tmp_path)
    operator = repo.operator("hello")
    bundle = operator.bundle("0.0.1")
    assert len(list(operator)) == 1
    assert set(operator) == {bundle}
    assert operator.has("0.0.1")
    assert bundle.operator() == operator
    assert operator.config == {}
    assert bundle.dependencies == []
    assert operator.root == repo.root / "operators" / "hello"
    assert "hello" in repr(operator)


def test_channels(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        bundle_files("hello", "0.0.1"),
        bundle_files("hello", "0.0.2", csv={"spec": {"replaces": "hello.v0.0.1"}}),
        bundle_files(
            "hello",
            "0.1.0",
            csv={"spec": {"replaces": "hello.v0.0.2"}},
            annotations={
                "operators.operatorframework.io.bundle.channel.default.v1": "candidate",
                "operators.operatorframework.io.bundle.channels.v1": "beta,candidate",
            },
        ),
        bundle_files(
            "hello",
            "1.0.0",
            csv={"spec": {"replaces": "hello.v0.1.0"}},
            annotations={
                "operators.operatorframework.io.bundle.channel.default.v1": "stable",
                "operators.operatorframework.io.bundle.channels.v1": "beta,candidate,stable",
            },
        ),
        bundle_files("hello", "1.0.1", csv={"spec": {"replaces": "hello.v1.0.0"}}),
    )
    repo = Repo(tmp_path)
    operator = repo.operator("hello")
    bundle001 = operator.bundle("0.0.1")
    bundle002 = operator.bundle("0.0.2")
    bundle010 = operator.bundle("0.1.0")
    bundle100 = operator.bundle("1.0.0")
    bundle101 = operator.bundle("1.0.1")
    assert operator.channels == {"beta", "candidate", "stable"}
    assert operator.default_channel == "beta"
    assert operator.channel_bundles("beta") == [
        bundle001,
        bundle002,
        bundle010,
        bundle100,
        bundle101,
    ]
    assert operator.channel_bundles("candidate") == [bundle010, bundle100]
    assert operator.channel_bundles("stable") == [bundle100]
    assert operator.head("beta") == bundle101
    assert operator.head("candidate") == bundle100
    assert operator.head("stable") == bundle100


def test_update_graph(tmp_path: Path) -> None:
    create_files(
        tmp_path,
        bundle_files("hello", "0.0.1"),
        bundle_files("hello", "0.0.2", csv={"spec": {"replaces": "hello.v0.0.1"}}),
        bundle_files("hello", "0.0.3", csv={"spec": {"replaces": "hello.v0.0.2"}}),
        bundle_files(
            "hello",
            "0.0.4",
            csv={"spec": {"replaces": "hello.v0.0.2", "skips": ["hello.v0.0.3"]}},
        ),
    )
    repo = Repo(tmp_path)
    operator = repo.operator("hello")
    bundle1 = operator.bundle("0.0.1")
    bundle2 = operator.bundle("0.0.2")
    bundle3 = operator.bundle("0.0.3")
    bundle4 = operator.bundle("0.0.4")
    assert operator.head("beta") == bundle4
    update = operator.update_graph("beta")
    assert update[bundle1] == {bundle2}
    assert update[bundle2] == {bundle3, bundle4}
    assert update[bundle3] == {bundle4}

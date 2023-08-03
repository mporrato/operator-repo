from pathlib import Path

import yaml


def merge(a, b, path=None):
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            else:
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a


def create_files(path, *contents):
    root = Path(path)
    for element in contents:
        for file_name, content in element.items():
            full_path = root / file_name
            if content is None:
                full_path.mkdir(parents=True, exist_ok=True)
            else:
                full_path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(content, (str, bytes)):
                    full_path.write_text(content)
                else:
                    full_path.write_text(yaml.safe_dump(content))


def bundle_files(
    operator_name, bundle_version, annotations=None, csv=None, other_files=None
):
    bundle_path = f"operators/{operator_name}/{bundle_version}"
    base_annotations = {
        "operators.operatorframework.io.bundle.mediatype.v1": "registry+v1",
        "operators.operatorframework.io.bundle.manifests.v1": "manifests/",
        "operators.operatorframework.io.bundle.metadata.v1": "metadata/",
        "operators.operatorframework.io.bundle.package.v1": operator_name,
        "operators.operatorframework.io.bundle.channel.default.v1": "beta",
        "operators.operatorframework.io.bundle.channels.v1": "beta",
    }
    base_csv = {
        "metadata": {
            "name": f"{operator_name}.v{bundle_version}",
            "spec": {"version": bundle_version},
        }
    }
    return merge(
        {
            f"{bundle_path}/metadata/annotations.yaml": {
                "annotations": merge(base_annotations, annotations or {})
            },
            f"{bundle_path}/manifests/{operator_name}.clusterserviceversion.yaml": merge(
                base_csv, csv or {}
            ),
        },
        other_files or {},
    )

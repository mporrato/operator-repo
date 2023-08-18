# operator-repo

This project is a library and utility for managing repositories of kubernetes operator packages as used by the
Operator Lifecycle Manager (OLM) and OperatorHub.
It provides functionality to load and parse YAML files, validate repository structures, operators and bundles, and
perform upgrade graph verifications.

The old and deprecated package manifest format is not supported.

## Getting Started

### Installation

pip install git+https://github.com/mporrato/operator-repo.git

### Using the CLI

The CLI tool is called `optool` accepts an optional `--repo` or `-r` argument pointing to the root directory of a
repository of operators. If one is not provided, the current directory will be used as the repository root.

#### list (or its alias ls)

Shows the contents of the resources given on the command line.

The format is either `operator_name` to indicate an operator or `operator_name/bundle_version` to
indicate a bundle. If no resource is given, then it will show the contents of the repo.

It accepts an optional `--recursive` or `-R` flag to recursively walk the tree below the given resources
when listing the contents.

##### Examples

```commandline
$ optool ls etcd
Operator(etcd)
  Bundle(etcd/0.6.1)
  Bundle(etcd/0.9.0)
  Bundle(etcd/0.9.2)
  Bundle(etcd/0.9.4)
```
```commandline
$ optool -r ~/operator-repo ls etcd/0.9.4
Bundle(etcd/0.9.4)
  Description     : Create and maintain highly-available etcd clusters on Kubernetes
  Name            : etcdoperator.v0.9.4
  Channels        : singlenamespace-alpha
  Default channel : singlenamespace-alpha
  Container image : quay.io/coreos/etcd-operator@sha256:66a37fd61a06a43969854ee6d3e21087a98b93838e284a6086b13917f96b0d9b
  Replaces        : etcdoperator.v0.9.2
  Skips           : []
```

#### check

Runs a suite of formal checks on the given operators or bundles.

Just like the `list` command, it supports the `--recursive` (`-R`) flag.

If no suite is specified with the `--suite` (`-s`) option, the built-in `operator_repo.checks` suite will be used.

The `--list` (`-l`) option can be used to list the checks contained in the selected suite.


##### Examples

```commandline
$ optool check --list
operator checks:
 - upgrade: Validate upgrade graphs for all channels
bundle checks:
 - image: Check if containerImage is properly defined and used in a deployment
 - operator_name: Check if the operator names used in CSV, metadata and filesystem are consistent
 - semver: Check that the bundle version is semver compliant
```
```commandline
$ optool check etcd/0.9.4
failure: check_operator_name(Bundle(etcd/0.9.4)): Operator name from annotations.yaml (etcd) does not match the name defined in the CSV (etcdoperator)
```
```commandline
$ optool check -R datadog-operator
failure: check_image(Bundle(datadog-operator/0.1.3)): container image datadog/operator:v0.1.3 not used by any deployment
failure: check_image(Bundle(datadog-operator/0.3.1)): container image datadog/operator:0.2.0 not used by any deployment
failure: check_image(Bundle(datadog-operator/0.3.2)): container image datadog/operator:0.2.0 not used by any deployment
failure: check_image(Bundle(datadog-operator/0.4.0)): CSV doesn't define .metadata.annotations.containerImage
failure: check_image(Bundle(datadog-operator/0.5.0)): CSV doesn't define .metadata.annotations.containerImage
failure: check_image(Bundle(datadog-operator/0.6.0)): CSV doesn't define .metadata.annotations.containerImage
```

## Creating a custom check suite

A check suite is a python package containing two modules, `operator` and `bundle`. All functions in those modules with
a name starting with `check_` will be used as a check for operator or bundle resources respectively.
A check function must take a single argument, either an `Operator` or a `Bundle` object, and return a generator of
`CheckResult` objects (either `Fail` or `Warn`).

# Charmed MongoDB on Kubernetes
[![CharmHub Badge](https://charmhub.io/mongodb-k8s/badge.svg)](https://charmhub.io/mongodb-k8s)
[![Release to 6/edge](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/release.yaml/badge.svg)](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/release.yaml)
[![Tests](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/ci.yaml/badge.svg)](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/ci.yaml)
## Overview

The Charmed MongoDB K8s Operator delivers automated operations management from [day 0 to day 2](https://codilime.com/glossary/day-0-day-1-day-2/#:~:text=Day%200%2C%20Day%201%2C%20and,just%20a%20daily%20operations%20routine.) on the [MongoDB Community Edition](https://github.com/mongodb/mongo) document database. It is an open source, end-to-end, production ready data platform on top of cloud native technologies.

MongoDB is a popular NoSQL database application. It stores its data with JSON-like documents creating a flexible experience for users; with easy to use data aggregation for data analytics. It is a distributed database, so vertical and horizontal scaling come naturally.

This operator charm deploys and operates MongoDB on Kubernetes. It offers features such as replication, TLS, password rotation, and easy to use integration with applications. The Charmed MongoDB K8s Operator meets the need of deploying MongoDB in a structured and consistent manner while allowing the user flexibility in configuration. It simplifies deployment, scaling, configuration and management of MongoDB in production at scale in a reliable way.

## Requirements 
- at least 2GB of RAM.
- at least 2 CPU threads per host.
- For production deployment: at least 60GB of available storage on each host.
- Access to the internet for downloading the charm.
- Machine is running Ubuntu 24.04(noble) or later.

## Usage

### Basic Usage
To deploy a single unit of MongoDB using its default configuration
```shell
juju deploy ./mongodb-k8s_ubuntu-24.04-amd64.charm --resource mongodb-image=ghcr.io/canonical/charmed-mongodb@sha256:739243ea34dc453d7d13eba96980c1618ebbd9202a742fd1e052caa644c174e0
```

It is customary to use MongoDB with replication. Hence usually more than one unit (preferably an odd number to prohibit a "split-brain" scenario) is deployed. To deploy MongoDB with multiple replicas, specify the number of desired units with the `-n` option.
```shell
juju deploy ./mongodb-k8s_ubuntu-24.04-amd64.charm --resource mongodb-image=ghcr.io/canonical/charmed-mongodb@sha256:739243ea34dc453d7d13eba96980c1618ebbd9202a742fd1e052caa644c174e0 -n <number_of_replicas>
```

## Documentation

Check the Charmed MongoDB [documentation](https://canonical-charmed-mongodb.readthedocs-hosted.com/8/).

## Security
Security issues in the Charmed MongoDB K8s Operator can be reported through [LaunchPad](https://wiki.ubuntu.com/DebuggingSecurity#How%20to%20File). Please do not file GitHub issues about security issues.


## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](https://github.com/canonical/mongodb-k8s-operator/blob/main/CONTRIBUTING.md) for developer guidance.


## License
The Charmed MongoDB K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/mongodb-k8s-operator/blob/main/LICENSE) for more information.

The Charmed MongoDB K8s Operator is free software, distributed under the Apache Software License, version 2.0. It [installs/operates/depends on] [MongoDB Community Version](https://github.com/mongodb/mongo), which is licensed under the Server Side Public License (SSPL)

See [LICENSE](https://github.com/canonical/mongodb-k8s-operator/blob/main/LICENSE) for more information.

## Trademark notice
MongoDB' is a trademark or registered trademark of MongoDB Inc. Other trademarks are property of their respective owners.

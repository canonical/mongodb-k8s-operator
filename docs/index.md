# Charmed MongoDB K8s Documentation

## Overview
[MongoDB](https://github.com/mongodb/mongo) is a popular NoSQL database application. It stores its data with JSON-like documents creating a flexible user experience with easy-to-use data aggregation for data analytics. In addition, it is a distributed database, so vertical and horizontal scaling come naturally.

Applications like MongoDB must be managed and operated in production environments. This means that MongoDB application administrators and analysts who run workloads in various infrastructures should be able to automate tasks for repeatable operational work. Technologies such as software operators encapsulate the knowledge, wisdom and expertise of a real-world operations team and codify it into a computer program that helps to operate complex server applications like MongoDB and other databases. Canonical has developed an open-source operator called Charmed MongoDB for this purpose.

[The Charmed MongoDB Kubernetes (K8s)](https://charmhub.io/mongodb-k8s?channel=5/edge) operator deploys and operates MongoDB in multi-cloud environments using Kubernetes. Software operators are principally associated with Kubernetes, an open-source container orchestration system that facilitates the deployment and operation of complex server applications. As a concept, however, they can be applied equally to application and infrastructure operations on platforms beyond Kubernetes. They are especially popular on Kubernetes because they can help to reduce the complexity of operations on this powerful but complex platform.

Charmed MongoDB (K8s Operator) is an enhanced, open source and fully-compatible drop-in replacement for the MongoDB Community Edition with advanced MongoDB enterprise features. It simplifies the deployment, scaling, design and management of MongoDB in production in a reliable way. In addition, you can use the operator to manage your MongoDB clusters with automation capabilities. It also offers replication, TLS, password rotation, easy-to-use application integration, backup, restore, and monitoring.

Aside from a Kubernetes operator, Canonical developed another operator called [Charmed MongoDB (VM operator)](https://charmhub.io/mongodb?channel=5/edge) that operates on physical, Virtual Machines (VM) and a wide range of cloud and cloud-like environments, including AWS, Azure, OpenStack and VMWare.

## Software and releases

Charmed MongoDB (K8s Operator) uses the [Charmed MongoDB snap package](https://snapcraft.io/charmed-mongodb), which offers more features than the MongoDB Community version, such as backup and restores, monitoring and security features.

To see the Charmed MongoDB features and releases, visit our [Github Releases](https://github.com/canonical/mongodb-k8s-operator/releases) and [ Release Notes](https://discourse.charmhub.io/t/release-notes-charmed-mongodb-5-k8s-operator/10040) page.

## Charm version, environment and OS

A charm version is a combination of both the application version and / (slash) the channel, e.g. `5/stable`, `5/candidate`, `5/edge`. The channels are ordered from the most stable to the least stable, candidate, and edge. More risky channels like edge are always implicitly available. So, if the candidate is listed, you can pull the candidate and edge. When stable is listed, all three are available.

You can deploy the charm a stand-alone machine or cloud and cloud-like environments, including AWS, Azure, OpenStack and VMWare.

The upper portion of this page describes the Operating System (OS) where the charm can run. For example, 5/stable is compatible and should run on a machine with Ubuntu 22.04 OS.

#### Supported architectures

*The charm should be running on architectures that support AVX* 

To check your architecture please execute 
```bash
grep avx /proc/cpuinfo
```

or  
```bash 
grep avx2 /proc/cpuinfo
```

#### Supported Juju versions

At the current moment the MongoDB K8S charm supports Juju 2.9.43 and Juju 3.1.5

#### Security, Bugs and feature request

If you find a bug in this operator or want to request a specific feature, here are the useful links:

* Raise the issue or feature request in the [Canonical Github
](https://github.com/canonical/mongodb-operator/issues)
* Meet the community and chat with us if there are issues and feature requests in our [Mattermost Channel](https://chat.charmhub.io/charmhub/channels/data-platform).

#### Contributing

Please see the[ Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and[ CONTRIBUTING.md](https://github.com/canonical/mongodb-operator/blob/main/CONTRIBUTING.md) for developer guidance.

#### Licence statement

The Charmed MongoDB Operator is free software, distributed under the [Apache Software License, version 2.0](https://github.com/canonical/mongodb-operator/blob/main/LICENSE). It installs and operates Percona Server for MongoDB, which is licensed under the Server Side Public License (SSPL) version 1.

#### Trademark Notice

“MongoDB” is a trademark or registered trademark of MongoDB, Inc. Other trademarks are property of their respective owners. Charmed MongoDB is not sponsored, endorsed, or affiliated with MongoDB, Inc.

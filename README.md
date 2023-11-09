# Charmed MongoDB K8s Operator

## Overview

[MongoDB](https://github.com/mongodb/mongo) is a popular NoSQL database application. It stores its data with JSON-like documents creating a flexible user experience with easy-to-use data aggregation for data analytics. In addition, it is a distributed database, so vertical and horizontal scaling come naturally.

Applications like MongoDB must be managed and operated in the production environment. This means that MongoDB application administrators and analysts who run workloads in various infrastructures should be able to automate tasks for repeatable operational work. Technologies such as software operators encapsulate the knowledge, wisdom and expertise of a real-world operations team and codify it into a computer program that helps to operate complex server applications like MongoDB and other databases.

Canonical has developed an open-source operator called  Charmed MongoDB, making it easier to operate MongoDB. The Charmed MongoDB Virtual Machine (VM) operator deploys and operates MongoDB on physical,  Virtual Machines (VM) and other wide range of cloud and cloud-like environments, including AWS, Azure, OpenStack and VMWare.

Charmed MongoDB (VM Operator) is an enhanced, open source and fully-compatible drop-in replacement for the MongoDB Community Edition with advanced MongoDB enterprise features. It simplifies the deployment, scaling, design and management of MongoDB in production in a reliable way. In addition, you can use the operator to manage your MongoDB clusters with automation capabilities. It also offers replication, TLS, password rotation, easy-to-use application integration, backup, restore, and monitoring.  

Aside from a VM operator, Canonical also developed another operator called [Charmed MongoDB (K8s operator)](https://charmhub.io/mongodb-k8s?channel=5/edge) that operates in the Kubernetes environment, and you can see the documentation [here](https://charmhub.io/mongodb-k8s?channel=5/edge).


## Software and releases

Charmed MongoDB (VM Operator) is an enhanced, open source and fully-compatible drop-in replacement for the MongoDB Community Edition with advanced MongoDB enterprise features. This operator uses the [Charmed MongoDB snap package](https://snapcraft.io/charmed-mongodb), which offers more features than the MongoDB Community version, such as backup and restores, monitoring and security features.

As of now we offer two operators [Charmed MongoDB K8s 5](https://charmhub.io/mongodb-k8s?channel=5/edge) and [Charmed MongoDB K8s 6](https://charmhub.io/mongodb-k8s?channel=6/edge).

To see the Charmed MongoDB features and releases, visit our [Release Notes page](https://github.com/canonical/mongodb-k8s-operator/releases). Currently both charms support:
- Replication
- Password Rotation
- User management
- TLS
- Backup & Restore

## Charm channel, environment and OS

A charm version is a combination of both the application version and / (slash) the channel, e.g. x/stable, x/candidate, x/edge. The channels are ordered from the most stable to the least stable, candidate, and edge. More risky channels like edge are always implicitly available. So, if the candidate is listed, you can pull the candidate and edge. When stable is listed, all three are available. 

You can deploy the charm a stand-alone machine or cloud and cloud-like environments, including AWS, Azure, OpenStack and VMWare.

The upper portion of this page describes the Operating System (OS) where the charm can run e.g. 5/stable is compatible and should run in a machine with Ubuntu 22.04 OS.


## Security, Bugs and feature request

If you find a bug in this snap or want to request a specific feature, here are the useful links:

* Raise issues or feature requests in [Github](https://github.com/canonical/mongodb-operator/issues)

* Security issues in the Charmed MongoDB Operator can be reported through [LaunchPad](https://wiki.ubuntu.com/DebuggingSecurity#How%20to%20File). Please do not file GitHub issues about security issues.

* Meet the community and chat with us if there are issues and feature requests in our [Mattermost Channel](https://chat.charmhub.io/charmhub/channels/data-platform)

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](https://github.com/canonical/mongodb-operator/blob/main/CONTRIBUTING.md) for developer guidance.

## License

The Charmed MongoDB Operator is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/mongodb-operator/blob/main/LICENSE) for more information.

The Charmed MongoDB Operator is free software, distributed under the Apache Software License, version 2.0. It [installs/operates/depends on] [MongoDB Community Version](https://github.com/mongodb/mongo), which is licensed under the Server Side Public License (SSPL)

See [LICENSE](https://github.com/canonical/mongodb-operator/blob/main/LICENSE) for more information.

## Trademark notice
MongoDB' is a trademark or registered trademark of MongoDB Inc. Other trademarks are property of their respective owners.


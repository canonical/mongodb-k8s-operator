# Charmed MongoDB on Kubernetes
[![CharmHub Badge](https://charmhub.io/mongodb-k8s/badge.svg)](https://charmhub.io/mongodb-k8s)
[![Release](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/release.yaml/badge.svg)](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/release.yaml)
[![Tests](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/ci.yaml?query=branch%3Amain)
[![Docs](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/sync_docs.yaml/badge.svg)](https://github.com/canonical/mongodb-k8s-operator/actions/workflows/sync_docs.yaml)
## Overview

The Charmed MongoDB Operator delivers automated operations management from [day 0 to day 2](https://codilime.com/glossary/day-0-day-1-day-2/#:~:text=Day%200%2C%20Day%201%2C%20and,just%20a%20daily%20operations%20routine.) on the [MongoDB Community Edition](https://github.com/mongodb/mongo) document database. It is an open source, end-to-end, production ready data platform on top of cloud native technologies.

MongoDB is a popular NoSQL database application. It stores its data with JSON-like documents creating a flexible experience for users; with easy to use data aggregation for data analytics. It is a distributed database, so vertical and horizontal scaling come naturally.

This operator charm deploys and operates MongoDB on Kubernetes. It offers features such as replication, TLS, password rotation, and easy to use integration with applications. The Charmed MongoDB Operator meets the need of deploying MongoDB in a structured and consistent manner while allowing the user flexibility in configuration. It simplifies deployment, scaling, configuration and management of MongoDB in production at scale in a reliable way.

## Requirements 
- at least 2GB of RAM.
- at least 2 CPU threads per host.
- For production deployment: at least 60GB of available storage on each host.
- Access to the internet for downloading the charm.
- Machine is running Ubuntu 22.04(jammy) or later.

## Config options
auto-delete - `boolean`; When a relation is removed, auto-delete ensures that any relevant databases
associated with the relation are also removed. Set with `juju config mongodb-k8s auto-delete=<bool>`.

admin-password - `string`; The password for the database admin user. Set with `juju run-action mongodb-k8s/leader set-admin-password --wait`

tls external key - `string`; TLS external key for encryption outside the cluster. Set with `juju run-action mongodb-k8s/0 set-tls-private-key "external-key=$(base64 -w0 external-key-0.pem)" --wait`

tls internal key - `string`;  TLS external key for encryption inside the cluster. Set with `juju run-action mongodb-k8s/0 set-tls-private-key "internal-key=$(base64 -w0 internal-key.pem)"  --wait`

## Usage

### Basic Usage
To deploy a single unit of MongoDB using its default configuration
```shell
juju deploy ./mongodb-k8s_ubuntu-22.04-amd64.charm --resource mongodb-image=dataplatformoci/mongodb:5.0
```

It is customary to use MongoDB with replication. Hence usually more than one unit (preferably an odd number to prohibit a "split-brain" scenario) is deployed. To deploy MongoDB with multiple replicas, specify the number of desired units with the `-n` option.
```shell
juju deploy ./mongodb-k8s_ubuntu-22.04-amd64.charm --resource mongodb-image=dataplatformoci/mongodb:5.0 -n <number_of_replicas>
```

### Replication
#### Adding Replicas
To add more replicas one can use the `juju scale-application` functionality i.e.
```shell
juju scale-application mongodb-k8s -n <num_of_replicas_to_add>
```
The implementation of `add-unit` allows the operator to add more than one unit, but functions internally by adding one replica at a time, as specified by the [constraints](https://www.mongodb.com/docs/manual/reference/command/replSetReconfig/#reconfiguration-can-add-or-remove-no-more-than-one-voting-member-at-a-time) of MongoDB.


#### Removing Replicas 
Similarly to scale down the number of replicas the `juju scale-application` functionality may be used i.e.
```shell
juju scale-application mongodb-k8s -n <desired number of units>
```
The implementation of `remove-unit` allows the operator to remove more than one replica so long has the operator **does not remove a majority of the replicas**. The functionality of `remove-unit` functions by removing one replica at a time, as specified by the [constraints](https://www.mongodb.com/docs/manual/reference/command/replSetReconfig/#reconfiguration-can-add-or-remove-no-more-than-one-voting-member-at-a-time) of MongoDB.


## Relations

Supported [relations](https://juju.is/docs/olm/relations):

#### `mongodb_client` interface:

Relations to applications are supported via the `mongodb_client` interface. To create a relation: 

```shell
juju relate mongodb-k8s application
```

To remove a relation:
```shell
juju remove-relation mongodb-k8s application
```

#### `tls` interface:

We have also supported TLS for the MongoDB k8s charm. To enable TLS:

```shell
# deploy the TLS charm 
juju deploy tls-certificates-operator --channel=stable
# add the necessary configurations for TLS
juju config tls-certificates-operator generate-self-signed-certificates="true" ca-common-name="Test CA" 
# to enable TLS relate the two applications 
juju relate tls-certificates-operator mongodb-k8s 
```

Updates to private keys for certificate signing requests (CSR) can be made via the `set-tls-private-key` action. Note passing keys to external/internal keys should *only be done with* `base64 -w0` *not* `cat`. With three replicas this schema should be followed:
```shell
# generate shared internal key
openssl genrsa -out internal-key.pem 3072
# generate external keys for each unit
openssl genrsa -out external-key-0.pem 3072
openssl genrsa -out external-key-1.pem 3072
openssl genrsa -out external-key-2.pem 3072
# apply both private keys on each unit, shared internal key will be allied only on juju leader
juju run-action mongodb-k8s /0 set-tls-private-key "external-key=$(base64 -w0 external-key-0.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"  --wait
juju run-action mongodb-k8s /1 set-tls-private-key "external-key=$(base64 -w0 external-key-1.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"  --wait
juju run-action mongodb-k8s /2 set-tls-private-key "external-key=$(base64 -w0 external-key-2.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"  --wait

# updates can also be done with auto-generated keys with
juju run-action mongodb-k8s /0 set-tls-private-key --wait
juju run-action mongodb-k8s /1 set-tls-private-key --wait
juju run-action mongodb-k8s /2 set-tls-private-key --wait
```

To disable TLS remove the relation
```shell
juju remove-relation mongodb-k8s  tls-certificates-operator
```

Note: The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the `tls-certificates-operator` charm offers a variety of configurations, read more on the TLS charm [here](https://charmhub.io/tls-certificates-operator)

### Password rotation
#### Internal admin user
The admin user is used internally by the Charmed MongoDB Operator, the `set-password` action can be used to rotate its password.
```shell
# to set a specific password for the admin user
juju run-action mongodb-k8s/leader set-password password=<password> --wait

# to randomly generate a password for the admin user
juju run-action mongodb-k8s/leader set-password --wait
```

#### Related applications users
To rotate the passwords of users created for related applications, the relation should be removed and related again. That process will generate a new user and password for the application.
```shell
juju remove-relation application mongodb-k8s 
juju add-relation application mongodb-k8s 
```

## Security
Security issues in the Charmed MongoDB Operator can be reported through [LaunchPad](https://wiki.ubuntu.com/DebuggingSecurity#How%20to%20File). Please do not file GitHub issues about security issues.


## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](https://github.com/canonical/mongodb-operator/blob/main/CONTRIBUTING.md) for developer guidance.


## License
The Charmed MongoDB Operator is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/mongodb-operator/blob/main/LICENSE) for more information.

The Charmed MongoDB Operator is free software, distributed under the Apache Software License, version 2.0. It [installs/operates/depends on] [MongoDB Community Version](https://github.com/mongodb/mongo), which is licensed under the Server Side Public License (SSPL)

See [LICENSE](https://github.com/canonical/mongodb-operator/blob/main/LICENSE) for more information.

## Trademark notice
MongoDB' is a trademark or registered trademark of MongoDB Inc. Other trademarks are property of their respective owners.

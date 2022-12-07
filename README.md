# Charmed MongoDB on Kubernetes
## Overview

The Charmed MongoDB Operator delivers automated operations management from [day 0 to day 2](https://codilime.com/glossary/day-0-day-1-day-2/#:~:text=Day%200%2C%20Day%201%2C%20and,just%20a%20daily%20operations%20routine.) on the [MongoDB Community Edition](https://github.com/mongodb/mongo) document database. It is an open source, end-to-end, production ready data platform on top of cloud native technologies.

MongoDB is a popular NoSQL database application. It stores its data with JSON-like documents creating a flexible experience for users; with easy to use data aggregation for data analytics. It is a distributed database, so vertical and horizontal scaling come naturally.

This operator charm deploys and operates MongoDB on Kubernetes. It offers features such as replication, TLS, password rotation, and easy to use integration with applications. The Charmed MongoDB Operator meets the need of deploying MongoDB in a structured and consistent manner while allowing the user flexibility in configuration. It simplifies deployment, scaling, configuration and management of MongoDB in production at scale in a reliable way.

## Requirements 
- at least 2GB of RAM.
- at least 2 CPU threads per host.
- For production deployment: at least 60GB of available storage on each host.
- Access to the internet for downloading the charm.
- Machine is running Ubuntu 20.04(focal) or later.

## Config options
auto-delete - `boolean`; When a relation is removed, auto-delete ensures that any relevant databases
associated with the relation are also removed. Set with `juju config mongodb auto-delete=<bool>`.

admin-password - `string`; The password for the database admin user. Set with `juju run-action mongodb/leader set-admin-password --wait`

tls external key - `string`; TLS external key for encryption outside the cluster. Set with `juju run-action mongodb/0 set-tls-private-key "external-key=$(base64 -w0 external-key-0.pem)" --wait`

tls internal key - `string`;  TLS external key for encryption inside the cluster. Set with `juju run-action mongodb/0 set-tls-private-key "internal-key=$(base64 -w0 internal-key.pem)"  --wait`


## Usage

Create a Juju model for your operators, say "lma"

    juju add-model lma

Deploy a single unit of MongoDB using its default configuration

    juju deploy ./mongodb-k8s_ubuntu-20.04-amd64.charm --resource mongodb-image=mongo:4.4

It is customary to use MongoDB with replication. Hence usually more
than one unit (preferably and odd number) is deployed. Additionally
units (say two more) may be deployed using

    juju add-unit -n 2 mongodb

Alternatively multiple MongoDB units may be deployed at the
outset. This is usually faster

    juju deploy -n 3 ./mongodb-k8s_ubuntu-20.04-amd64.charm --resource mongodb-image=mongo:4.4

If required, remove the deployed monitoring model completely

    juju destroy-model -y lma --no-wait --force --destroy-storage

Note the `--destroy-storage` will delete any data stored by MongoDB in
its persistent store.

## Relations

Supported [relations](https://juju.is/docs/olm/relations):

#### `mongodb_client` interface:

Relations to new applications are supported via the `mongodb_client` interface. To create a relation: 

```shell
juju relate mongodb application
```

To remove a relation:
```shell
juju remove-relation mongodb application
```

#### `tls` interface:

We have also supported TLS for the MongoDB k8s charm. To enable TLS:

```shell
# deploy the TLS charm 
juju deploy tls-certificates-operator --channel=edge
# add the necessary configurations for TLS
juju config tls-certificates-operator generate-self-signed-certificates="true" ca-common-name="Test CA" 
# enable TLS via relation
juju relate tls-certificates-operator mongodb
# disable TLS by removing relation
juju remove-relation mongodb tls-certificates-operator
```

Note: The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the `tls-certificates-operator` charm offers a variety of configurations, read more on the TLS charm [here](https://charmhub.io/tls-certificates-operator)


## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this
charm following best practice guidelines, and [CONTRIBUTING.md](./CONTRIBUTING.md) for developer guidance.


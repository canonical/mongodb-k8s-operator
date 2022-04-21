# Charm for MongoDB on Kubernetes

## Description

Fully supported solution for production-grade MongoDB on Kubernetes from Canonical.

Run the developer's favourite document database - MongoDB! Charm for MongoDB is a
fully supported, automated solution from Canonical for running production-grade
MongoDB on Kubernetes. It offers simple, secure and highly available setup with
automatic recovery on failover. The solution includes scaling and other capabilities.

Charm for MongoDB on Kubernetes is built on Juju, the Charmed Operator Framework
to provide day-0 to day-2 operations of MongoDB.

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

Currently supported relations are

- Peer relations for replication
- Provides a `mongodb-client` database interface.

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this
charm following best practice guidelines, and [CONTRIBUTING.md](./CONTRIBUTING.md) for developer guidance.

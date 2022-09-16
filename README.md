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


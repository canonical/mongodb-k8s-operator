# Charmed Operator for MongoDB

## Description

The charmed operator for [MongoDB](https://www.mongodb.com/) provides a general
purpose distributed document database. This repository contains a
[Juju](https://jaas.ai/) Charm for deploying MongoDB on Kubernetes
clusters.


## Setup

A typical setup using [snaps](https://snapcraft.io/), for deployments
to a [microk8s](https://microk8s.io/) cluster can be done using the
following commands

    sudo snap install microk8s --classic
    microk8s.enable dns storage registry dashboard
    sudo snap install juju --classic
    juju bootstrap microk8s microk8s
    juju create-storage-pool operator-storage kubernetes storage-class=microk8s-hostpath

## Build

Install the charmcraft tool

    sudo snap install charmcraft --classic

Build the charm in this git repository

    charmcraft pack

## Usage

Create a Juju model for your operators, say "lma"

    juju add-model lma

Deploy a single unit of MongoDB using its default configuration

    juju deploy ./mongodb-k8s_ubuntu-20.04-amd64.charm --resource mongodb-image=mongo:4.4.1

It is customary to use MongoDB with replication. Hence usually more
than one unit (preferably and odd number) is deployed. Additionally
units (say two more) may be deployed using

    juju add-unit -n 2 mongodb

Alternatively multiple MongoDB units may be deployed at the
outset. This is usually faster

    juju deploy -n 3 ./mongodb-k8s_ubuntu-20.04-amd64.charm --resource mongodb-image=mongo:4.4.1

If required, remove the deployed monitoring model completely

    juju destroy-model -y lma --no-wait --force --destroy-storage

Note the `--destroy-storage` will delete any data stored by MongoDB in
its persistent store.

## Relations

Currently supported relations are

- Peer relations for replication
- Provides a `mongodb` database interface.

## Developing

Use your existing Python 3 development environment or create and
activate a Python 3 virtualenv

    virtualenv -p python3 venv
    source venv/bin/activate

Install the development requirements

    pip install -r requirements-dev.txt

## Testing

Just run `run_tests`:

    ./run_tests

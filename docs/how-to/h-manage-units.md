# How to manage related applications

## New `mongodb_client` interface:

Relations to new applications are supported via the `mongodb_client` interface. To create a relation: 

```shell
juju relate mongodb-k8s <application>
```
where <application> is the name of your application

To remove a relation:

```shell
juju remove-relation mongodb-k8s <application>
```
where <application> is the name of your application
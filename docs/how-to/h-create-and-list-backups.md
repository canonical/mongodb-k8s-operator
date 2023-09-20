Creating and listing backups requires that you:
* [Have a replica set with at least three-nodes deployed](/t/charmed-mongodb-k8s-tutorial-managing-your-units/10611)
* Access to S3 storage
* [Have configured settings for S3 storage](/t/charmed-mongodb-k8s-how-to-configuring-settings-for-s3/11692)

Once you have a three-node replicaset that has configurations set for S3 storage, check that Charmed MongoDB is `active` and `idle` with `juju status`. Once Charmed MongoDB is `active` and `idle`, you can create your first backup with the `create-backup` command:
```
juju run mongodb-k8s/leader create-backup
```

You can list your available, failed, and in progress backups by running the `list-backups` command:
```
juju run mongodb-k8s/leader list-backups
```
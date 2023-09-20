This is a How-To for restoring a backup that was made from the a *different* cluster, (i.e. cluster migration via restore). To perform a basic restore please reference the [Restore How-To](/t/charmed-mongodb-k8s-how-to-restore-a-backup/11694)

Restoring a backup from a previous cluster to a current cluster requires that you:
- Have a replica set with [at least three-nodes deployed](/t/charmed-mongodb-tutorial-managing-units/8620)
- Access to S3 storage
- [Have configured settings for S3 storage](/t/charmed-mongodb-k8s-tutorial-managing-your-units/10611)
- Have the backups from the previous cluster in your S3-storage
- Have the password from your previous cluster

When you restore a backup from an old cluster, it will restore the password from the previous cluster to your current cluster. Set the password of your current cluster to the previous cluster’s password:
```shell
juju run mongodb-k8s/leader set-password password=<previous cluster password>
```

To view the available backups to restore you can enter the command `list-backups`: 
```shell
juju run mongodb-k8s/leader list-backups
```

This shows a list of the available backups (it is up to you to identify which `backup-id` corresponds to the previous-cluster):
```shell
    backups: |-
      backup-id             | backup-type  | backup-status
      ----------------------------------------------------
      YYYY-MM-DDTHH:MM:SSZ  | logical      | finished
```

To restore your current cluster to the state of the previous cluster, run the `restore` command and pass the correct `backup-id` to the command:
 ```shell
juju run mongodb-k8s/leader restore backup-id=YYYY-MM-DDTHH:MM:SSZ
```

Your restore will then be in progress, once it is complete your cluster will represent the state of the previous cluster.
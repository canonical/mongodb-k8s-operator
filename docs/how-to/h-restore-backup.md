This is a How-To for performing a basic restore. To restore a backup that was made from the a *different* cluster, (i.e. cluster migration via restore), please reference the [Cluster Migration via Restore How-To K8s](/t/charmed-mongodb-k8s-how-to-cluster-migration-via-restore/11697):

Restoring from a backup requires that you:
- [Have a replica set with at least three-nodes deployed](/t/charmed-mongodb-k8s-tutorial-managing-your-units/10611)
- Access to S3 storage
- [Have configured settings for S3 storage](/t/charmed-mongodb-k8s-how-to-configuring-settings-for-s3/11692) 
- [Have existing backups in your S3-storage](/t/charmed-mongodb-k8s-how-to-how-to-create-and-list-backups/11689)

To view the available backups to restore you can enter the command `list-backups`: 
```shell
juju run mongodb-k8s/leader list-backups
```

This should show your available backups 
```shell
    backups: |-
      backup-id             | backup-type  | backup-status
      ----------------------------------------------------
      YYYY-MM-DDTHH:MM:SSZ  | logical      | finished 
```

To restore a backup from that list, run the `restore` command and pass the `backup-id` to restore:
 ```shell
juju action mongodb-k8s/leader restore backup-id=YYYY-MM-DDTHH:MM:SSZ
```

Your restore will then be in progress.
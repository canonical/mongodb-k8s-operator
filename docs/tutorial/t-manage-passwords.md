# Manage Passwords

This is part of the [Charmed MongoDB K8s Tutorial](/t/charmed-mongodb-k8s-tutorial/10592). Please refer to this page for more information and the overview of the content.

## Passwords

When we accessed MongoDB earlier in this tutorial, we needed to include a password in the URI. Passwords help to secure our database and are essential for security. Over time it is a good practice to change the password frequently. Here we will go through setting and changing the password for the admin user.

### Retrieve the admin password
As previously mentioned, the admin password can be retrieved by running the `get-password` action on the Charmed MongoDB application:
```shell
juju run-action mongodb-k8s/leader get-password --wait
```
Running the command should output:
```yaml
unit-mongodb-k8s-0:
  UnitId: mongodb-k8s/0
  id: "4"
  results:
    password: Jx1WvwM95Tn14Vz98YVogBQqYLHPmvpf
  status: completed
  timing:
    completed: 2023-05-13 16:27:44 +0000 UTC
    enqueued: 2023-05-13 16:27:41 +0000 UTC
    started: 2023-05-13 16:27:43 +0000 UT
```
The admin password is under the result: `password`.


### Rotate the admin password
You can change the admin password to a new random password by entering:
```shell
juju run-action mongodb-k8s/leader set-password --wait
```
Running the command should output:
```yaml
unit-mongodb-k8s-0:
  UnitId: mongodb-k8s/0
  id: "6"
  results:
    password: kt58Lnw2nS57oC9lyG2F8OkBtsU9CIq4
  status: completed
  timing:
    completed: 2023-05-13 16:28:24 +0000 UTC
    enqueued: 2023-05-13 16:28:20 +0000 UTC
    started: 2023-05-13 16:28:22 +0000 UTC
```
The admin password is under the result: `password`. It should be different from your previous password.

*Note: when you change the admin password you will also need to update the admin password the in MongoDB URI; as the old password will no longer be valid.* Update the DB password used in the URI and update the URI:
```shell
export DB_PASSWORD=$(juju run-action mongodb-k8s/leader get-password --wait | grep password|  awk '{print $2}')
export URI=mongodb://$DB_USERNAME:$DB_PASSWORD@$HOST_IP:27017/$DB_NAME?replicaSet=$REPL_SET_NAME
```

### Set the admin password
You can change the admin password to a specific password by entering:
```shell
juju run-action mongodb-k8s/leader set-password password=<password> --wait
```
Running the command should output:
```yaml
unit-mongodb-k8s-0:
  UnitId: mongodb-k8s/0
  id: "10"
  results:
    password: <password>
  status: completed
  timing:
    completed: 2023-05-13 16:30:40 +0000 UTC
    enqueued: 2023-05-13 16:30:34 +0000 UTC
    started: 2023-05-13 16:30:39 +0000 UTC
```
The admin password under the result: `password` should match whatever you passed in when you entered the command.

*Note: that when you change the admin password you will also need to update the admin password in the MongoDB URI, as the old password will no longer be valid.* To update the DB password used in the URI:
```shell
export DB_PASSWORD=$(juju run-action mongodb-k8s/leader get-password --wait | grep password|  awk '{print $2}')
export URI=mongodb://$DB_USERNAME:$DB_PASSWORD@$HOST_IP:27017/$DB_NAME?replicaSet=$REPL_SET_NAME
```

## Next Steps
[Charmed MongoDB K8s Tutorial - Relate your MongoDB to other applications](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-relate-your-mongodb-deployment/10613)
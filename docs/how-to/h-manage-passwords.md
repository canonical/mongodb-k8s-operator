# How to manage passwords
## Internal operator user

The operator user is used internally by the Charmed MongoDB Operator, the `set-password` action can be used to rotate its password.

* To set a specific password for the operator user

```shell
juju run-action mongodb-k8s/leader set-password password=<password> --wait
```
where <password> is the password you want to set

* To randomly generate a password for the operator user

```shell
juju run-action mongodb-k8s/leader set-password --wait
```

## Rotate application passwords

To rotate the passwords of users created for related applications, the relation should be removed and related again. That process will generate a new user and password for the application.

```shell
juju remove-relation <application> mongodb-k8s
juju add-relation <application> mongodb-k8s
```
where <application> is the name of your application
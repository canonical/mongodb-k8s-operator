# Get a Charmed MongoDB up and running

This is part of the [Charmed MongoDB K8s Tutorial](/t/charmed-mongodb-k8s-tutorial/10592). Please refer to this page for more information and the overview of the content. 

## Deploy

To deploy [Charmed MongoDB K8s](https://charmhub.io/mongodb?channel=5/edge),  all you need to do is run the following command, which will fetch the charm from Charmhub and deploy it to your model:
```shell
juju deploy mongodb-k8s --channel 5/edge
```

Juju will now fetch Charmed MongoDB and begin deploying it to the Kubernetes cloud. This process can take several minutes depending on how provisioned (RAM, CPU,etc) your machine is. You can track the progress by running:
```shell
watch -n 1 -c juju status
```

This command is useful for checking the status of Charmed MongoDB and gathering information about the pods hosting Charmed MongoDB. Some of the helpful information it displays include IP addresses, ports, state, etc. The command updates the status of Charmed MongoDB every second and as the application starts you can watch the status and messages of Charmed MongoDB change. Wait until the application is ready - when it is ready, `watch -n 1 -c juju status` will show:
```shell
Model     Controller  Cloud/Region        Version  SLA          Timestamp
tutorial  overlord    microk8s/localhost  2.9.42   unsupported  15:33:40Z

App          Version  Status  Scale  Charm        Channel  Rev  Address        Exposed  Message
mongodb-k8s           active      1  mongodb-k8s  5/edge    27  10.152.183.20  no

Unit            Workload  Agent  Address      Ports  Message
mongodb-k8s/0*  active    idle   10.1.42.137
Model     Controller  Cloud/Region        Version  SLA          Timestamp
tutorial  overlord    microk8s/localhost  2.9.42   unsupported  15:33:41Z
```

To exit the screen with `watch -n 1 -c juju status`, run `Ctrl+c`.
You can also add `--color` parameter to the command. `watch -n 1 -c juju status --color`

## Access MongoDB
> **!** *Disclaimer: this part of the tutorial accesses MongoDB via the `operator` user. **Do not** directly interface with the admin user in a production environment. In a production environment [always create a separate user](https://www.mongodb.com/docs/manual/tutorial/create-users/) and connect to MongoDB with that user instead. Later in the section covering Relations we will cover how to access MongoDB without the admin user.*

The first action most users take after installing MongoDB is accessing MongoDB. The easiest way to do this is via the MongoDB shell, with `mongo`. You can read more about the MongoDB shell [here](https://www.mongodb.com/docs/mongodb-shell/). For this part of the Tutorial we will access MongoDB via  `mongo`. Fortunately there is no need to install the Mongo shell, as `mongo` is already installed on the units hosting the Charmed MongoDB application as `charmed-mongodb.mongo`. 

### MongoDB URI
Connecting to the database requires a Uniform Resource Identifier (URI), MongoDB expects a [MongoDB specific URI](https://www.mongodb.com/docs/manual/reference/connection-string/). The URI for MongoDB contains information which is used to authenticate us to the database. We use a URI of the format:
```shell
mongodb://<username>:<password>@<hosts>/<database name>?replicaSet=<replica set name>
```

Connecting via the URI requires that you know the values for `username`, `password`, `hosts`, `database name`, and the `replica set name`. We will show you how to retrieve the necessary fields and set them to environment variables. 

**Retrieving the username:** In this case, we are using the `admin` user to connect to MongoDB. Use `admin` as the username:
```shell
export DB_USERNAME="operator"
```

**Retrieving the password:** The password can be retrieved by running the `get-password` action on the Charmed MongoDB application:
```shell
juju run-action mongodb-k8s/leader get-password --wait
```
Running the command should output:
```yaml
unit-mongodb-k8s-0:
  UnitId: mongodb-k8s/0
  id: "4"
  results:
    password: hXgkYOkErQEEPGTaTug7ZnSjKxNDXPen
  status: completed
  timing:
    completed: 2023-05-12 12:42:07 +0000 UTC
    enqueued: 2023-05-12 12:42:04 +0000 UTC
    started: 2023-05-12 12:42:06 +0000 UTC
```
Use the value of password under the result: `password`:
```shell
export DB_PASSWORD=$(juju run-action mongodb-k8s/leader get-password --wait | grep password|  awk '{print $2}')
```

**Retrieving the hosts:** In this case we are connecting to mongo inside mongodb-k8s/0

Set the variable `HOST_IP` to the IP address to `mongodb-k8s-0.mongodb-k8s-endpoints`:
```shell
export HOST_IP="mongodb-k8s-0.mongodb-k8s-endpoints"
```

**Retrieving the database name:** In this case we are connecting to the `admin` database. Use `admin` as the database name. Once we access the database via the MongoDB URI, we will create a `test-db` database to store data.
```shell
export DB_NAME="admin"
```

**Retrieving the replica set name:** The replica set name is the name of the application on Juju hosting MongoDB. The application name in this tutorial is `mongodb-k8s`. Use `mongodb-k8s` as the replica set name. 
```shell
export REPL_SET_NAME="mongodb-k8s"
```

### Generate the MongoDB URI
Now that we have the necessary fields to connect to the URI, we can connect to MongoDB with `charmed-mongodb.mongo` via the URI. We can create the URI with:
```shell
export URI=mongodb://$DB_USERNAME:$DB_PASSWORD@$HOST_IP:27017/$DB_NAME?replicaSet=$REPL_SET_NAME
```
Now view and save the output of the URI:
```shell
echo $URI
```

### Connect via MongoDB URI
As said earlier, `mongo` is already installed in Charmed MongoDB K8s as `mongo`. To access the unit hosting Charmed MongoDB K8S, ssh into it pod:
```shell
juju ssh --container=mongod mongodb-k8s/0
```
*Note if at any point you'd like to leave the unit hosting Charmed MongoDB K8S, type:* `exit`.

While `ssh`'d into `mongodb-k8s/0` unit, we can access `mongo`, using the URI that we saved in the step [Generate the MongoDB URI](#generate-the-mongodb-uri).

```shell
mongo <URI>
```

You should now see:
```shell
Percona Server for MongoDB shell version v5.0.15-13
connecting to: mongodb://mongodb-k8s-0.mongodb-k8s-endpoints:27017/admin?compressors=disabled&gssapiServiceName=mongodb&replicaSet=mongodb-k8s
Implicit session: session { "id" : UUID("c695ac7d-9da6-4693-920d-bca4f2925feb") }
Percona Server for MongoDB server version: v5.0.15-13
================
Warning: the "mongo" shell has been superseded by "mongosh",
which delivers improved usability and compatibility.The "mongo" shell has been deprecated and will be removed in
an upcoming release.
For installation instructions, see
https://docs.mongodb.com/mongodb-shell/install/
================
---
The server generated these startup warnings when booting:
        2023-05-12T12:31:55.774+00:00: Using the XFS filesystem is strongly recommended with the WiredTiger storage engine. See http://dochub.mongodb.org/core/prodnotes-filesystem
---
mongodb-k8s:PRIMARY>
```

You can now interact with MongoDB directly using any [MongoDB commands](https://www.mongodb.com/docs/manual/reference/command/). For example entering `show dbs` should output something like:
```
admin   0.000GB
config  0.000GB
local   0.000GB
```
Now that we have access to MongoDB we can create a database named `test-db`. To create this database type:
```shell
use test-db
```
Now lets create a user called `testUser` with read/write access to the database `test-db` that we just created. Type:
```shell
db.createUser({
  user: "testUser",
  pwd: "password",
  roles: [
    { role: "readWrite", db: "test-db" }
  ]
})
```
You can verify that you added the user correctly by running the command `show users` into the mongo shell. This will output:
```json
Successfully added user: {
	"user" : "testUser",
	"roles" : [
		{
			"role" : "readWrite",
			"db" : "test-db"
		}
	]
}
```
Feel free to test out any other MongoDB commands. When you’re ready to leave the MongoDB shell you can just type `exit`. Once you've typed `exit` you will be back in the host of the `mongodb-k8s/0` unit.   
Exit this host by once again typing `exit`. Now you will be in your original shell where you first started the tutorial; here you can interact with Juju and Kubernetes. 

*Note: if you accidentally exit one more time you will leave your terminal session and all of the environment variables used in the URI will be removed. If this happens redefine these variables as described in the section that describes how to [create the MongoDB URI](#mongodb-uri).*

## Next Steps
[Charmed MongoDB K8s Tutorial - Managing your units](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-managing-your-units/10611)
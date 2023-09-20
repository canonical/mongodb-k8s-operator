# Scale your Charmed MongoDB

This is part of the [Charmed MongoDB K8s Tutorial](/t/charmed-mongodb-k8s-tutorial/10592). Please refer to this page for more information and the overview of the content. 

## Adding and Removing units

Replication is a popular feature of MongoDB; replicas copy data making a database highly available. This means the application can provide self-healing capabilities in case one MongoDB replica fails. 

> **!** *Disclaimer: this tutorial hosts all replicas are on the same machine, this should not be done in a production environment. To enable high availability in a production environment, replicas should be hosted on different servers to [maintain isolation](https://canonical.com/blog/database-high-availability).*


### Add replicas
You can add two replicas to your deployed MongoDB application with:
```shell
juju add-unit mongodb-k8s -n 2
```

You can now watch the replica set add these replicas with: `watch -n 1 -c juju status`. It usually takes several minutes for the replicas to be added to the replica set. You’ll know that all three replicas are ready when `watch -n 1 -c juju status --color ` reports:
```shell
Model     Controller  Cloud/Region        Version  SLA          Timestamp
tutorial  overlord    microk8s/localhost  2.9.42   unsupported  15:58:05Z

App          Version  Status  Scale  Charm        Channel  Rev  Address        Exposed  Message
mongodb-k8s           active      3  mongodb-k8s  5/edge    27  10.152.183.20  no

Unit            Workload  Agent  Address      Ports  Message
mongodb-k8s/0*  active    idle   10.1.42.137
mongodb-k8s/1   active    idle   10.1.42.140
mongodb-k8s/2   active    idle   10.1.42.141
```

You can trust that Charmed MongoDB K8S added these replicas correctly. But if you wanted to verify the replicas got added correctly you could connect to MongoDB via `mongo` command in the pod. Since your replica set has 2 additional hosts you will need to update the hosts in your URI. You can retrieve these host IPs with:

```shell
export HOST_IP_1="mongodb-k8s-1.mongodb-k8s-endpoints"
export HOST_IP_2="mongodb-k8s-2.mongodb-k8s-endpoints"
```

Then recreate the URI using your new hosts and reuse the `username`, `password`, `database name`, and `replica set name` that you previously used when you *first* connected to MongoDB:
```shell
export URI=mongodb://$DB_USERNAME:$DB_PASSWORD@$HOST_IP,$HOST_IP_1,$HOST_IP_2:27017/$DB_NAME?replicaSet=$REPL_SET_NAME
```

Now view and save the output of the URI:
```shell
echo $URI
```

Like earlier we access `mongo` by `ssh`ing into one of the Charmed MongoDB K8s hosts:
```shell
juju ssh --container=mongod mongodb-k8s/0
```

While `ssh`'d into `mongodb-k8s/0` unit , we can access `mongo` using our new URI that we saved above.
```shell
mongo <saved URI>
```

Now type `rs.status()` and you should see your replica set configuration. It should look something like this:
```json
{
	"set" : "mongodb-k8s",
	"date" : ISODate("2023-05-13T16:01:53.297Z"),
	"myState" : 1,
	"term" : NumberLong(1),
	"syncSourceHost" : "",
	"syncSourceId" : -1,
	"heartbeatIntervalMillis" : NumberLong(2000),
	"majorityVoteCount" : 2,
	"writeMajorityCount" : 2,
	"votingMembersCount" : 3,
	"writableVotingMembersCount" : 3,
	"optimes" : {
		"lastCommittedOpTime" : {
			"ts" : Timestamp(1683993711, 1),
			"t" : NumberLong(1)
		},
		"lastCommittedWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
		"readConcernMajorityOpTime" : {
			"ts" : Timestamp(1683993711, 1),
			"t" : NumberLong(1)
		},
		"appliedOpTime" : {
			"ts" : Timestamp(1683993711, 1),
			"t" : NumberLong(1)
		},
		"durableOpTime" : {
			"ts" : Timestamp(1683993711, 1),
			"t" : NumberLong(1)
		},
		"lastAppliedWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
		"lastDurableWallTime" : ISODate("2023-05-13T16:01:51.536Z")
	},
	"lastStableRecoveryTimestamp" : Timestamp(1683993671, 1),
	"electionCandidateMetrics" : {
		"lastElectionReason" : "electionTimeout",
		"lastElectionDate" : ISODate("2023-05-13T15:32:11.406Z"),
		"electionTerm" : NumberLong(1),
		"lastCommittedOpTimeAtElection" : {
			"ts" : Timestamp(1683991931, 1),
			"t" : NumberLong(-1)
		},
		"lastSeenOpTimeAtElection" : {
			"ts" : Timestamp(1683991931, 1),
			"t" : NumberLong(-1)
		},
		"numVotesNeeded" : 1,
		"priorityAtElection" : 1,
		"electionTimeoutMillis" : NumberLong(10000),
		"newTermStartDate" : ISODate("2023-05-13T15:32:11.448Z"),
		"wMajorityWriteAvailabilityDate" : ISODate("2023-05-13T15:32:11.469Z")
	},
	"members" : [
		{
			"_id" : 0,
			"name" : "mongodb-k8s-0.mongodb-k8s-endpoints:27017",
			"health" : 1,
			"state" : 1,
			"stateStr" : "PRIMARY",
			"uptime" : 1862,
			"optime" : {
				"ts" : Timestamp(1683993711, 1),
				"t" : NumberLong(1)
			},
			"optimeDate" : ISODate("2023-05-13T16:01:51Z"),
			"lastAppliedWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
			"lastDurableWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
			"syncSourceHost" : "",
			"syncSourceId" : -1,
			"infoMessage" : "",
			"electionTime" : Timestamp(1683991931, 2),
			"electionDate" : ISODate("2023-05-13T15:32:11Z"),
			"configVersion" : 5,
			"configTerm" : 1,
			"self" : true,
			"lastHeartbeatMessage" : ""
		},
		{
			"_id" : 1,
			"name" : "mongodb-k8s-1.mongodb-k8s-endpoints:27017",
			"health" : 1,
			"state" : 2,
			"stateStr" : "SECONDARY",
			"uptime" : 269,
			"optime" : {
				"ts" : Timestamp(1683993711, 1),
				"t" : NumberLong(1)
			},
			"optimeDurable" : {
				"ts" : Timestamp(1683993711, 1),
				"t" : NumberLong(1)
			},
			"optimeDate" : ISODate("2023-05-13T16:01:51Z"),
			"optimeDurableDate" : ISODate("2023-05-13T16:01:51Z"),
			"lastAppliedWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
			"lastDurableWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
			"lastHeartbeat" : ISODate("2023-05-13T16:01:52.993Z"),
			"lastHeartbeatRecv" : ISODate("2023-05-13T16:01:52.993Z"),
			"pingMs" : NumberLong(0),
			"lastHeartbeatMessage" : "",
			"syncSourceHost" : "mongodb-k8s-0.mongodb-k8s-endpoints:27017",
			"syncSourceId" : 0,
			"infoMessage" : "",
			"configVersion" : 5,
			"configTerm" : 1
		},
		{
			"_id" : 2,
			"name" : "mongodb-k8s-2.mongodb-k8s-endpoints:27017",
			"health" : 1,
			"state" : 2,
			"stateStr" : "SECONDARY",
			"uptime" : 86,
			"optime" : {
				"ts" : Timestamp(1683993711, 1),
				"t" : NumberLong(1)
			},
			"optimeDurable" : {
				"ts" : Timestamp(1683993711, 1),
				"t" : NumberLong(1)
			},
			"optimeDate" : ISODate("2023-05-13T16:01:51Z"),
			"optimeDurableDate" : ISODate("2023-05-13T16:01:51Z"),
			"lastAppliedWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
			"lastDurableWallTime" : ISODate("2023-05-13T16:01:51.536Z"),
			"lastHeartbeat" : ISODate("2023-05-13T16:01:52.992Z"),
			"lastHeartbeatRecv" : ISODate("2023-05-13T16:01:51.978Z"),
			"pingMs" : NumberLong(0),
			"lastHeartbeatMessage" : "",
			"syncSourceHost" : "mongodb-k8s-1.mongodb-k8s-endpoints:27017",
			"syncSourceId" : 1,
			"infoMessage" : "",
			"configVersion" : 5,
			"configTerm" : 1
		}
	],
	"ok" : 1,
	"$clusterTime" : {
		"clusterTime" : Timestamp(1683993711, 1),
		"signature" : {
			"hash" : BinData(0,"DhUrEN4lw0ixgUSIb76By1yU2BA="),
			"keyId" : NumberLong("7232690270372888581")
		}
	},
	"operationTime" : Timestamp(1683993711, 1)
}
```

Now exit the MongoDB shell by typing:
```shell
exit
```
Now you should be back in the host of the Charmed MongoDB K8s' unit `mongodb-k8s/0`. To exit this host type:
```shell
exit
```
You should now be in the shell you started where you can interact with Juju and Kubernetes (MicroK8s).

### Remove replicas
Removing a unit from the application, scales the replicas down. Before we scale down the replicas, list all the units with `juju status`, here you will see three units `mongodb-k8s/0`, `mongodb-k8s/1`, and `mongodb-k8s/2`. Each of these units hosts a MongoDB replica.  
Because Kubernetes models does not support removing named units, we will specify number of units for application using `--num-units`
and we will specify the number of units to be removed.

```shell
juju remove-unit mongodb-k8s --num-units 1
```

You’ll know that the replica was successfully removed when `juju status --watch 1s` reports:
```
Model     Controller  Cloud/Region        Version  SLA          Timestamp
tutorial  overlord    microk8s/localhost  2.9.42   unsupported  16:12:55Z

App          Version  Status  Scale  Charm        Channel  Rev  Address        Exposed  Message
mongodb-k8s           active      2  mongodb-k8s  5/edge    27  10.152.183.20  no

Unit            Workload    Agent  Address      Ports  Message
mongodb-k8s/0*  active      idle   10.1.42.137
mongodb-k8s/1   active      idle   10.1.42.140
mongodb-k8s/2   terminated  lost   10.1.42.141         unit stopped by the cloud
```

You can check the status of pod in K8s to see that a pod was removed by using command `microk8s.kubectl get pods --namespace=tutorial`
```shell
NAME                             READY   STATUS    RESTARTS   AGE
modeloperator-69977f8c44-zh7pc   1/1     Running   0          52m
mongodb-k8s-0                    2/2     Running   0          49m
mongodb-k8s-1                    2/2     Running   0          19m
```


As previously mentioned you can trust that Charmed MongoDB K8s removed this replica correctly. This can be checked by verifying that the new URI where the removed host has been excluded works properly.

## Next Steps
[Charmed MongoDB K8s Tutorial - Manage Passwords](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-manage-passwords/10612)
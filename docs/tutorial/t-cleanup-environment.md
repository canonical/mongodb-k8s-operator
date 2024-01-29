# Cleanup and extra info

This is part of the [Charmed MongoDB K8s Tutorial](/t/charmed-mongodb-k8s-tutorial/10592). Please refer to this page for more information and the overview of the content. 

## Remove Charmed MongoDB K8s and Juju
If you're done using Charmed MongoDB K8s and Juju and would like to free up resources on your machine, you can remove Charmed MongoDB and Juju.    

 *Warning: when you remove Charmed MongoDB K8s as shown below you will lose all the data in MongoDB. Further, when you remove Juju as shown below you will lose access to any other applications you have hosted on Juju.*. 


To remove Charmed MongoDB K8s and the model it is hosted on run the command:
```shell
juju destroy-model tutorial --destroy-storage --force
```

Next step is to remove the Juju controller. You can see all of the available controllers by entering `juju controllers`. To remove the controller enter:
```shell
juju destroy-controller overlord
```

Finally, to remove Juju altogether, enter:
```shell
sudo snap remove juju --purge
```

## Next Steps

In this tutorial we've successfully deployed MongoDB, added/removed replicas, added/removed users to/from the database, and even enabled and disabled TLS. You may now keep your Charmed MongoDB K8s deployment running and write to the database or remove it entirely using the steps in [Remove Charmed MongoDB K8s and Juju](#remove-charmed-mongodb-and-juju). If you're looking for what to do next you can:
- Run [Charmed MongoDB on VM](https://github.com/canonical/mongodb-operator).
- Check out our Charmed offerings of [PostGres](https://charmhub.io/postgresql?channel=edge) and [Kafka](https://charmhub.io/kafka?channel=edge).
- Read about [High Availability Best Practices](https://canonical.com/blog/database-high-availability)
- [Report](https://github.com/canonical/mongodb-operator/issues) any problems you encountered.
- [Give us your feedback](https://chat.charmhub.io/charmhub/channels/data-platform).
- [Contribute to the code base](https://github.com/canonical/mongodb-operator)
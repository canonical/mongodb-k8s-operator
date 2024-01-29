The Charmed MongoDB K8S Operator delivers automated operations management from [day 0 to day 2](https://codilime.com/blog/day-0-day-1-day-2-the-software-lifecycle-in-the-cloud-age/) on the [MongoDB Community Edition](https://github.com/mongodb/mongo) document database.   

   It is an open source, end-to-end, production-ready data platform product [on top of Juju](https://juju.is/) and Kubernetes.  
As a first step, this tutorial shows you how to get Charmed MongoDB on Kubernetes up and running, but the tutorial does not stop there.   
Through this tutorial you will learn a variety of operations, everything from adding replicas to advanced operations such as enabling Transcript Layer Security (TLS). In this tutorial we will walk through how to:

* Set up your environment using [Microk8s](https://microk8s.io/) and [Juju](https://juju.is/)
* Deploy MongoDB using a single command.
* Access the admin database directly.
* Add high availability with replication.
* Change the admin password.
* Automatically create MongoDB users via Juju relations.
* Enable secure transactions with TLS.

While this tutorial intends to guide and teach you as you deploy Charmed MongoDB K8S, it will be most beneficial if you already have a familiarity with:

* Basic terminal commands.
* MongoDB concepts such as replication and users.

## Important note about Juju version
In this tutorial we will use a juju installed from snap. 
The version of juju installed from snap is 3.x (3.1.5 at the moment of the last edit of this document).  
*If* you are using version of juju 2.9.x please be aware that a command format to run a charm action is changed. In juju version 2.9.x you would use
```shell
juju run-action mongodb-k8s/leader <action_name> <action_param_key>=<action_param_value>  --wait
```
while in juju version 3.x we will use the following command format

```shell
juju run mongodb-k8s/leader <action_name> <action_param_key>=<action_param_value>
```

Also, the format of result of action is changed.

## Step-by-step guide

Here’s an overview of the steps required with links to our separate tutorials that deal with each individual step:

* [Set up the environment](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-environment-setup/10596)
* [Deploy MongoDB on K8s ](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-deploy-mongodb/10608)
* [Managing your units](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-managing-your-units/10611)
* [Manage passwords](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-manage-passwords/10612)
* [Relate your MongoDB to other applications](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-relate-your-mongodb-deployment/10613)
* [Enable security](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-enable-security/10614)
* [Cleanup your environment](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-environment-cleanup/10615)
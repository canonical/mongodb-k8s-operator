# Environment Setup

This is part of the [Charmed MongoDB K8s Tutorial](/t/charmed-mongodb-k8s-tutorial/10592). Please refer to this page for more information and the overview of the content. 

## Minimum requirements

Before we start, make sure your machine meets the following requirements:
- Ubuntu 20.04 (focal) or later.
- at least 8GB of RAM.
- at least 2 CPU cores.
- At least 20GB of available storage. For production deployment: at least 60GB of available storage on each host.
- Access to the internet for downloading the required snaps and charms.


## Install and prepare MicroK8s
The fastest, simplest way to get started with Charmed MongoDB K8s is to set up a local Kubernetes using [MicroK8s](https://microk8s.io/).   
MicroK8s provides lightweight zero-ops, pure-upstream Kubernetes; Charmed MongoDB K8s will be run in Kubernetes pods  and managed by Juju.   
While this tutorial covers the basics of MicroK8s, you can [explore more MicroK8 here](https://microk8s.io/#install-microk8s).

### 1. Install MicroK8s

```shell 
sudo snap install microk8s --classic
```

### 2. Add a user to microk8s group
```shell
sudo usermod -a -G microk8s $(whoami)
```

### 3. Create ~/.kube folder and set permissions
```shell
mkdir ~/.kube
sudo chown -R $(whoami) ~/.kube
```

### 4. Reload user groups 
Reload the user groups either via a reboot or by running 
```shell
newgrp microk8s
```

### 5. Check Kubernetes status
```shell
microk8s status --wait-ready
```

### 6. Enable required plugins
```shell
microk8s enable dns hostpath-storage
```

### 7. (Optional) create an alias for  microk8s.kubectl
```shell
sudo snap alias microk8s.kubectl kubectl
```

## Install and prepare Juju
[Juju](https://juju.is/) is an Operator Lifecycle Manager (OLM) for clouds, bare metal, LXD or Kubernetes. We will be using it to deploy and manage Charmed MongoDB. As with Microk8s, Juju is installed from a snap package:
```shell
sudo snap install juju --classic
```

Juju already has a built-in knowledge of Kubernetes and how it works, so there is no additional setup or configuration needed.   
A controller will be used to deploy and control Charmed MongoDB K8s.  
All we need to do is run the following command to bootstrap a Juju controller named ‘overlord’ to Kubernetes. This bootstrapping processes can take several minutes depending on how provisioned (RAM, CPU, etc.) your machine is:
```shell
juju bootstrap microk8s overlord
```

The Juju controller should exist within a Kubernetes namespace. 
You can verify this by:

1. Typing the command `microk8s.kubectl get namespaces` and you should see the following:

```shell
microk8s.kubectl get namespaces
NAME                  STATUS   AGE
kube-system           Active   <age>
kube-public           Active   <age>
kube-node-lease       Active   <age>
default               Active   <age>
controller-overlord   Active   <age>
```

2. Typing command `microk8s.kubectl get pods --namespace=controller-overlord`

```shell
NAME                             READY   STATUS    RESTARTS      AGE
controller-0                     2/2     Running   1 (<restarted> ago)   <age>
modeloperator-6c58d95cfb-ktts7   1/1     Running   0             <age>
```

where `<age>` shows information on how long ago a namespace was created 
and `<restarted>` shows information when the pod was restarted.

The controller can work with different models; models host applications such as Charmed MongoDB K8s. Set up a specific model for Charmed MongoDB K8s named ‘tutorial’:

```shell
juju add-model tutorial
```

You can now view the model you created above by entering the command `juju status` into the command line. You should see the following:
```shell
Model     Controller  Cloud/Region        Version  SLA          Timestamp
tutorial  overlord    microk8s/localhost  2.9.42   unsupported  11:35:46Z

Model "admin/tutorial" is empty.
```
## Next Steps

[Charmed MongoDB K8s Tutorial - Deploy MongoDB on K8s ](https://discourse.charmhub.io/t/charmed-mongodb-k8s-tutorial-deploy-mongodb/10608)
# MongoDB operator Charm for Kubernetes

## Requirements

```bash
sudo snap install juju --classic
sudo snap install charmcraft --beta
# If you don't have a Kubernetes, install Microk8s
sudo snap install microk8s --classic
sudo microk8s.status --wait-ready
sudo microk8s.enable storage dns
```

## Bootstrap a Juju controller in K8s

- If you are using microk8s:

```bash
juju bootstrap microk8s
```

- If you are using another Kubernetes:

```bash
cat /path/to/kube/config | juju add-k8s my-k8s
juju bootstrap my-k8s
```

## Deploy MongoDB

```bash
juju add-model mongodb
charmcraft build
juju deploy ./mongodb.charm
```

## Configuration options

- standalone
- replica_set_name
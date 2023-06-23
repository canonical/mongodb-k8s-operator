# How to enable encryption
**Note**:

> The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the tls-certificates-operator charm offers a variety of configurations, read more on the TLS charm here.

## Enable TLS
```shell
# deploy the TLS charm 
juju deploy tls-certificates-operator --channel=edge
# add the necessary configurations for TLS
juju config tls-certificates-operator generate-self-signed-certificates="true" ca-common-name="Test CA" 
# to enable TLS relate the two applications 
juju relate mongodb-k8s tls-certificates-operator
```
## Manage keys
Updates to private keys for certificate signing requests (CSR) can be made via the set-tls-private-key action. Note passing keys to external/internal keys should only be done with base64 -w0 not cat. With three replicas this schema should be followed
* Generate a shared internal key
```shell
openssl genrsa -out internal-key.pem 3072
```
* generate external keys for each unit
```shell
openssl genrsa -out external-key-0.pem 3072
openssl genrsa -out external-key-1.pem 3072
openssl genrsa -out external-key-2.pem 3072
```
* apply both private keys on each unit, shared internal key will be allied only on juju leader
```shell
juju run-action mongodb-k8s/0 set-tls-private-key "external-key=$(base64 -w0 external-key-0.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"  --wait
juju run-action mongodb-k8s/1 set-tls-private-key "external-key=$(base64 -w0 external-key-1.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"  --wait
juju run-action mongodb-k8s/2 set-tls-private-key "external-key=$(base64 -w0 external-key-2.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"  --wait
```
* updates can also be done with auto-generated keys with
```shell
juju run-action mongodb/0 set-tls-private-key --wait
juju run-action mongodb/1 set-tls-private-key --wait
juju run-action mongodb/2 set-tls-private-key --wait
```

## Disable TLS remove the relation
```shell
juju remove-relation mongodb tls-certificates-operator
```
# Terraform module for MongoDB sharded cluster

This module deploys a MongoDB 6 sharded cluster with the Terraform Juju provider.

## Requirements

| Name          | Version |
| ------------- | ------- |
| Terraform     | >= 1.6  |
| Juju provider | ~> 2.0  |

## Modules

| Name               | Source                                                |
| ------------------ | ----------------------------------------------------- |
| config_and_routing | ../../components/config_and_routing                   |
| data_integrator    | canonical/data-integrator, `main`                     |
| shards             | ../../charms/mongodb                                  |
| s3_integrator      | canonical/object-storage-integrator S3 module, `main` |

## Inputs

| Name                                    | Description                                                                                 | Default         |
| --------------------------------------- | ------------------------------------------------------------------------------------------- | --------------- |
| `config_server`                         | MongoDB config-server definition. Uses Ubuntu 22.04 and channel `6/stable` by default.      | required        |
| `mongos`                                | Mongos definition. Uses Ubuntu 22.04 and channel `6/stable` by default.                     | required        |
| `shards`                                | MongoDB shard definitions. Each shard uses Ubuntu 22.04 and channel `6/stable` by default.  | `[]`            |
| `data_integrator`                       | Data-integrator definition. Uses Ubuntu 22.04 by default.                                   | required        |
| `backups_integrator`                    | Optional S3 integrator definition.                                                          | `null`          |
| `certificates_integration`              | Optional TLS certificates integration target.                                               | `null`          |
| `grafana_dashboard_integration`         | Optional Grafana dashboard integration target for the config server and shards.             | `null`          |
| `metrics_endpoint_integration`          | Optional metrics integration target for the config server and shards.                       | `null`          |
| `logging_integration`                   | Optional logging integration target for the config server and shards.                       | `null`          |
| `ldap_integration`                      | Optional LDAP integration target; must be set with `ldap_certificate_transfer_integration`. | `null`          |
| `ldap_certificate_transfer_integration` | Optional LDAP certificate-transfer target; must be set with `ldap_integration`.             | `null`          |
| `logging_config`                        | Logging configuration.                                                                      | `"<root>=INFO"` |
| `s3_access_key`                         | Optional S3 access key stored in a Juju secret.                                             | `null`          |
| `s3_secret_key`                         | Optional S3 secret key stored in a Juju secret.                                             | `null`          |

Optional integration targets use this shape:

```hcl
{
  name       = string
  endpoint   = string
  model_uuid = string
  url        = optional(string)
}
```

When an integration is cross-model, `url` must contain an offer URL created outside this module.

## Outputs

| Name         | Description                               |
| ------------ | ----------------------------------------- |
| `components` | All deployed applications.                |
| `app_names`  | Names of all deployed applications.       |
| `models`     | Components grouped by model UUID.         |
| `metadata`   | Product deployment metadata.              |
| `provides`   | Provided integration endpoints.           |
| `requires`   | Required integration endpoints.           |
| `offers`     | Cross-model offers created by the module. |

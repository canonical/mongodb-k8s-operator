## Requirements

| Name | Version |
|------|---------|
| `Terraform` | >= 1.6 |
| `Juju provider` | ~> 2.0 |

## Providers

| Name | Version |
| ---- | ------- |
| `juju` | ~> 2.0 |


## Module

| Name | Source | Version |
|------|--------|---------|
| `config_and_routing` | ../../components/sharded | n/a |
| `data_integrator` | git::https://github.com/canonical/data-integrator.git//terraform/charm/data_integrator | main |
| `gcs_integrator` | git::https://github.com/canonical/object-storage-integrator.git//gcs/terraform/charm/gcs_integrator | main |
| `shards` | ../../charms/mongodb | n/a |
| `s3_integrator` | git::https://github.com/canonical/object-storage-integrator.git//s3/terraform/charm/s3_integrator | main |

## Resources

| Name | Type | Description |
|------|------|-------------|
| `juju_integration.config_server_shards` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates shards to the config server, using a direct endpoint for same-model shards and an offer for cross-model shards. |
| `juju_integration.mongos_client` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates data-integrator to mongos in the config server model. |
| `juju_integration.s3_credentials` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates the config server to the optional S3 integrator. |
| `juju_integration.gcs_credentials` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates the config server to the optional GCS integrator. |
| `juju_integration.client_certificates` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates MongoDB applications to an optional client TLS certificates target. |
| `juju_integration.grafana_dashboard` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates config-server and shard Grafana dashboard endpoints to an optional target. |
| `juju_integration.metrics_endpoint` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates config-server and shard metrics endpoints to an optional target. |
| `juju_integration.logging` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates config-server and shard logging endpoints to an optional target. |
| `juju_integration.ldap` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates the config server and mongos to an optional LDAP target. |
| `juju_integration.ldap_certificate_transfer` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates the config server and mongos to an optional LDAP certificate transfer target. |
| `juju_integration.peer_certificates` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates MongoDB applications to an optional peer TLS certificates target. |
| `juju_integration.vault_kv` | [Juju integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | Relates MongoDB applications to an optional Vault KV target for encryption at rest. |
| `juju_offer.s3_credentials` | [Juju offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | Offers the S3 integrator credentials endpoint when S3 is cross-model. |
| `juju_secret.gcs_secret` | [Juju secret](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/secret) | Optionally stores the GCS service-account JSON key and passes its URI to the GCS integrator. |
| `juju_access_secret.gcs_secret_access` | [Juju secret access](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/access_secret) | Grants the optional GCS credentials secret to the GCS integrator application. |
| `juju_secret.tls_client_private_key` | [Juju secret](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/secret) | Optionally stores the client TLS private key and passes its URI to the config server. |
| `juju_access_secret.tls_client_private_key` | [Juju secret access](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/access_secret) | Grants the config server access to the optional client TLS private-key secret. |
| `juju_secret.tls_peer_private_key` | [Juju secret](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/secret) | Optionally stores the peer TLS private key and passes its URI to the config server. |
| `juju_access_secret.tls_peer_private_key` | [Juju secret access](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/access_secret) | Grants the config server access to the optional peer TLS private-key secret. |
| `terraform_data.validate_encryption_at_rest` | [Terraform data](https://developer.hashicorp.com/terraform/language/resources/terraform-data) | Requires Vault KV when any MongoDB application enables encryption at rest. |
| `terraform_data.validate_ldap_integrations` | [Terraform data](https://developer.hashicorp.com/terraform/language/resources/terraform-data) | Ensures LDAP and LDAP certificate transfer are configured together. |
| `terraform_data.validate_cross_model_integration_urls` | [Terraform data](https://developer.hashicorp.com/terraform/language/resources/terraform-data) | Ensures optional external integration targets provide an offer URL when cross-model relations are needed. |
| `terraform_data.deployed_at` | [Terraform data](https://developer.hashicorp.com/terraform/language/resources/terraform-data) | Stores the first deployment timestamp for product metadata. |



## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| `config_server` | Config server app definition | <pre>object({<br/>    app_name    = optional(string, "config-server")<br/>    base        = optional(string, "ubuntu@24.04")<br/>    channel     = optional(string, "8/stable")<br/>    config      = optional(map(string), { "role" : "config-server" })<br/>    constraints = optional(string, "arch=amd64")<br/>    expose = optional(list(object({<br/>      cidrs     = optional(string)<br/>      endpoints = optional(string)<br/>    })), [])<br/>    model_uuid         = string<br/>    revision           = optional(number, null)<br/>    storage_directives = optional(map(string), {})<br/>    units              = optional(number, 3)<br/>  })</pre> | n/a | yes |
| `data_integrator` | Configuration for the data-integrator | <pre>object({<br/>    app_name    = optional(string, "data-integrator")<br/>    base        = optional(string, "ubuntu@24.04")<br/>    channel     = optional(string, "latest/edge")<br/>    config      = optional(map(string), { "database-name" : "test", "extra-user-roles" : "admin" })<br/>    constraints = optional(string, "arch=amd64")<br/>    endpoint_bindings = optional(set(object({<br/>      space    = string<br/>      endpoint = optional(string)<br/>    })), [])<br/>    machines           = optional(set(string), null)<br/>    model_uuid         = string<br/>    revision           = optional(number, null)<br/>    storage_directives = optional(map(string), {})<br/>    units              = optional(number, 1)<br/>  })</pre> | n/a | yes |
| `mongos` | Configuration for mongos | <pre>object({<br/>    app_name    = optional(string, "mongos")<br/>    base        = optional(string, "ubuntu@24.04")<br/>    channel     = optional(string, "8/stable")<br/>    config      = optional(map(string), {})<br/>    constraints = optional(string, "arch=amd64")<br/>    model_uuid  = string<br/>    revision    = optional(number, null)<br/>    units       = optional(number, 3)<br/>  })</pre> | n/a | yes |
| `shards` | Shard apps | <pre>list(object({<br/>    app_name    = string<br/>    base        = optional(string, "ubuntu@24.04")<br/>    channel     = optional(string, "8/stable")<br/>    config      = optional(map(string), { "role" : "shard" })<br/>    constraints = optional(string, "arch=amd64")<br/>    expose = optional(list(object({<br/>      cidrs     = optional(string)<br/>      endpoints = optional(string)<br/>    })), [])<br/>    model_uuid         = string<br/>    revision           = optional(number, null)<br/>    storage_directives = optional(map(string), {})<br/>    units              = optional(number, 3)<br/>  }))</pre> | `[]` | no |
| `backups_integrator` | Optional configuration for the backup integrator, including the model in which it is deployed. `storage_type` selects either S3 or GCS. Cross-model relations use the integrator's Juju offer. When `channel` is omitted, S3 uses `2/stable` and GCS uses `1/stable`. | <pre>object({<br/>    storage_type = optional(string, "s3")<br/>    config       = map(string)<br/>    channel      = optional(string, null)<br/>    base         = optional(string, "ubuntu@24.04")<br/>    revision     = optional(number, null)<br/>    constraints  = optional(string, "arch=amd64")<br/>    machines     = optional(set(string), [])<br/>    model_uuid   = string<br/>  })</pre> | `null` | no |
| `client_certificates_integration` | Optional client TLS certificates integration target. | <pre>object({<br/>    name       = optional(string, null)<br/>    endpoint   = optional(string, null)<br/>    model_uuid = optional(string, null)<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |
| `grafana_dashboard_integration` | Optional Grafana dashboard integration target for the config server and shards. | <pre>object({<br/>    name       = string<br/>    endpoint   = string<br/>    model_uuid = string<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |
| `metrics_endpoint_integration` | Optional metrics endpoint integration target for the config server and shards. | <pre>object({<br/>    name       = string<br/>    endpoint   = string<br/>    model_uuid = string<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |
| `logging_integration` | Optional logging integration target for the config server and shards. | <pre>object({<br/>    name       = string<br/>    endpoint   = string<br/>    model_uuid = string<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |
| `ldap_integration` | Optional LDAP integration target. Must be configured together with ldap_certificate_transfer_integration. | <pre>object({<br/>    name       = optional(string, null)<br/>    endpoint   = optional(string, null)<br/>    model_uuid = optional(string, null)<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |
| `ldap_certificate_transfer_integration` | Optional LDAP certificate transfer integration target. Must be configured together with ldap_integration. | <pre>object({<br/>    name       = optional(string, null)<br/>    endpoint   = optional(string, null)<br/>    model_uuid = optional(string, null)<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |
| `gcs_secret_key` | GCP service-account JSON key for GCS credentials. When set, this module creates a Juju secret and passes its URI as `gcs_integrator.config.credentials`. | `string` | `null` | no |
| `logging_config` | Logging configuration to be used | `string` | `"<root>=INFO"` | no |
| `peer_certificates_integration` | Optional peer TLS certificates integration target. | <pre>object({<br/>    name       = optional(string, null)<br/>    endpoint   = optional(string, null)<br/>    model_uuid = optional(string, null)<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |
| `tls_client_private_key` | Private key for client-to-server TLS certificates on the config server. When set, the module stores it in a Juju secret and configures the config server with the secret URI. | `string` | `null` | no |
| `tls_peer_private_key` | Private key for peer-to-peer TLS certificates on the config server. When set, the module stores it in a Juju secret and configures the config server with the secret URI. | `string` | `null` | no |
| `vault_kv_integration` | Optional Vault KV integration target for encryption at rest. Must be provided when enable-encryption-at-rest is true for any config server or shard; only enabled applications are integrated. | <pre>object({<br/>    name       = optional(string, null)<br/>    endpoint   = optional(string, null)<br/>    model_uuid = optional(string, null)<br/>    url        = optional(string, null)<br/>  })</pre> | `null` | no |

Optional integration targets use this shape:

```hcl
{
  name       = string
  endpoint   = string
  model_uuid = string
  url        = optional(string)
}
```

When an optional integration is configured, `name`, `endpoint`, and `model_uuid` must be non-empty. If the target is cross-model from any MongoDB application that needs it, `url` must contain an offer URL created outside this module.

## Outputs

| Name | Description |
|------|-------------|
| `components` | Deployed applications. Optional integrators return `null` when omitted. |
| `app_names` | Names of all deployed applications. Optional integrators return `null` when omitted. |
| `models` | Models and deployed components managed by this module, keyed by model UUID. |
| `metadata` | Metadata of the product deployment. |
| `provides` | Provided endpoint pointers from the sharded control plane and shards. |
| `requires` | Required endpoint pointers from the sharded control plane and shards. |
| `offers` | Cross-model offer endpoints created for product-owned applications, or `null` when not needed. |

## Configure TLS private keys

The module can configure custom private keys for client-to-server and peer-to-peer TLS on the config server. Set them in a `.tfvars` file using heredocs:

```hcl
tls_client_private_key = <<-EOT
-----BEGIN PRIVATE KEY-----
<config-server client private key contents>
-----END PRIVATE KEY-----
EOT

tls_peer_private_key = <<-EOT
-----BEGIN PRIVATE KEY-----
<config-server peer private key contents>
-----END PRIVATE KEY-----
EOT
```

Each value must be a valid PEM-encoded private key. For example, generate separate RSA keys with:

```bash
openssl genrsa -out <private-key-name>.pem 3072
```

When a key is provided, the module creates a Juju secret containing a `private-key` field in the config-server model, grants only the config-server application access to it, and sets the corresponding `tls-client-private-key` or `tls-peer-private-key` charm configuration to the secret URI. These inputs do not configure mongos or shard applications. When an input is `null`, the module does not create that secret or override that configuration option.

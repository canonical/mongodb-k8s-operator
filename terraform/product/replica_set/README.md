## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.6 |
| <a name="requirement_juju"></a> [juju](#requirement\_juju) | >= 0.20.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_juju"></a> [juju](#provider\_juju) | >= 0.20.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_mongodb_k8s"></a> [mongodb\_k8s](#module\_mongodb\_k8s) | ../../charm/replica_set | n/a |

## Resources

| Name | Type |
|------|------|
| [juju_application.data_integrator](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_application.s3_integrator](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_application.self-signed-certificates](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_integration.mongodb_data_cross_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.mongodb_data_same_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.mongodb_s3_cross_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.mongodb_s3_same_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.mongodb_tls_cross_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.mongodb_tls_same_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_offer.mongodb_client_offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | resource |
| [juju_offer.s3_integrator_offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | resource |
| [juju_offer.tls_provider_offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_data_integrator"></a> [data\_integrator](#input\_data\_integrator) | Configuration for the data-integrator | <pre>object({<br/>    app_name          = optional(string, "data-integrator")<br/>    model             = string<br/>    config            = optional(map(string), { "database-name" : "test", "extra-user-roles" : "admin" })<br/>    channel           = optional(string, "latest/edge")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 1)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_mongodb_k8s"></a> [mongodb\_k8s](#input\_mongodb\_k8s) | MongoDB app definition | <pre>object({<br/>    app_name          = optional(string, "mongodb-k8s")<br/>    model             = string<br/>    config            = optional(map(string), { "role" : "replication" })<br/>    channel           = optional(string, "8/edge")<br/>    base              = optional(string, "ubuntu@24.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 3)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_s3_integrator"></a> [s3\_integrator](#input\_s3\_integrator) | Configuration for the backup integrator | <pre>object({<br/>    app_name          = optional(string, "s3-integrator")<br/>    model             = string<br/>    config            = map(string)<br/>    channel           = optional(string, "latest/edge")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 1)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_self_signed_certificates"></a> [self\_signed\_certificates](#input\_self\_signed\_certificates) | Configuration for the self-signed-certificates app | <pre>object({<br/>    app_name          = optional(string, "self-signed-certificates")<br/>    model             = string<br/>    config            = optional(map(string), { "ca-common-name" : "CA" })<br/>    channel           = optional(string, "latest/edge")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 1)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_app_names"></a> [app\_names](#output\_app\_names) | Names of of all deployed applications. |
| <a name="output_offers"></a> [offers](#output\_offers) | List of offers URLs. |
| <a name="output_provides"></a> [provides](#output\_provides) | Map of all "provides" endpoints |
| <a name="output_requires"></a> [requires](#output\_requires) | Map of all "requires" endpoints |

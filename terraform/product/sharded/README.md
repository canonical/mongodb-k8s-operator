## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.6 |
| <a name="requirement_juju"></a> [juju](#requirement\_juju) | >= 0.20.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_juju"></a> [juju](#provider\_juju) | 0.22.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_mongodb-k8s"></a> [mongodb-k8s](#module\_mongodb-k8s) | ../../charm/sharded | n/a |

## Resources

| Name | Type |
|------|------|
| [juju_application.data_integrator](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_application.mongos-k8s](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_application.s3_integrator](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_application.self-signed-certificates](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_integration.config_server_mongos_cross_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.config_server_mongos_same_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.mongos_data_integrator_same_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.s3_config_server_cross_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.s3_config_server_same_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.tls_mongo_cross_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.tls_mongo_same_model_integration](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_offer.config_server_mongos_offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | resource |
| [juju_offer.s3_integrator_offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | resource |
| [juju_offer.tls_provider_offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_config_server"></a> [config\_server](#input\_config\_server) | Config server app definition | <pre>object({<br/>    app_name          = optional(string, "config-server")<br/>    model             = string<br/>    config            = optional(map(string), { "role" : "config-server" })<br/>    channel           = optional(string, "6/stable")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 3)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_data_integrator"></a> [data\_integrator](#input\_data\_integrator) | Configuration for the data-integrator | <pre>object({<br/>    app_name          = optional(string, "data-integrator")<br/>    model             = string<br/>    config            = optional(map(string), { "database-name" : "test", "extra-user-roles" : "admin" })<br/>    channel           = optional(string, "latest/edge")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 1)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_mongos-k8s"></a> [mongos-k8s](#input\_mongos-k8s) | Configuration for mongos | <pre>object({<br/>    app_name = optional(string, "mongos-k8s")<br/>    model    = string<br/>    config   = optional(map(string), {})<br/>    channel  = optional(string, "6/stable")<br/>    base     = optional(string, "ubuntu@22.04")<br/>    revision = optional(string, null)<br/>  })</pre> | n/a | yes |
| <a name="input_s3_integrator"></a> [s3\_integrator](#input\_s3\_integrator) | Configuration for the backup integrator | <pre>object({<br/>    app_name          = optional(string, "s3-integrator")<br/>    model             = string<br/>    config            = map(string)<br/>    channel           = optional(string, "latest/edge")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 1)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_self_signed_certificates"></a> [self\_signed\_certificates](#input\_self\_signed\_certificates) | Configuration for the self-signed-certificates app | <pre>object({<br/>    app_name          = optional(string, "self-signed-certificates")<br/>    model             = string<br/>    config            = optional(map(string), { "ca-common-name" : "CA" })<br/>    channel           = optional(string, "latest/edge")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 1)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_shards"></a> [shards](#input\_shards) | Shard apps | <pre>list(object({<br/>    app_name          = string<br/>    model             = string<br/>    config            = optional(map(string), { "role" : "shard" })<br/>    channel           = optional(string, "6/stable")<br/>    base              = optional(string, "ubuntu@22.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 3)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  }))</pre> | `[]` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_app_names"></a> [app\_names](#output\_app\_names) | Names of of all deployed applications. |
| <a name="output_offers"></a> [offers](#output\_offers) | List of offers URLs. |
| <a name="output_provides"></a> [provides](#output\_provides) | Map of all "provides" endpoints |
| <a name="output_requires"></a> [requires](#output\_requires) | Map of all "requires" endpoints |

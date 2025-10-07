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
| <a name="module_mongodb_config_server"></a> [mongodb\_config\_server](#module\_mongodb\_config\_server) | ../replica_set | n/a |
| <a name="module_mongodb_shards"></a> [mongodb\_shards](#module\_mongodb\_shards) | ../replica_set | n/a |

## Resources

| Name | Type |
|------|------|
| [juju_integration.mongodb_config_server_cross_model_integrations](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.mongodb_config_server_same_model_integrations](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_offer.mongodb_config_server_offer](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/offer) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_config_server"></a> [config\_server](#input\_config\_server) | Config server app definition | <pre>object({<br/>    app_name          = string<br/>    model             = string<br/>    config            = optional(map(string), { "role" : "config-server" })<br/>    channel           = optional(string, "8-transition/edge")<br/>    base              = optional(string, "ubuntu@24.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 3)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  })</pre> | n/a | yes |
| <a name="input_shards"></a> [shards](#input\_shards) | Shard apps | <pre>list(object({<br/>    app_name          = string<br/>    model             = string<br/>    config            = optional(map(string), { "role" : "shard" })<br/>    channel           = optional(string, "8-transition/edge")<br/>    base              = optional(string, "ubuntu@24.04")<br/>    revision          = optional(string, null)<br/>    units             = optional(number, 3)<br/>    constraints       = optional(string, "arch=amd64")<br/>    machines          = optional(set(string), null)<br/>    storage           = optional(map(string), {})<br/>    endpoint_bindings = optional(set(map(string)), [])<br/>  }))</pre> | `[]` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_app_names"></a> [app\_names](#output\_app\_names) | Names of of all deployed applications. |
| <a name="output_offers"></a> [offers](#output\_offers) | List of offers URLs. |
| <a name="output_provides"></a> [provides](#output\_provides) | Map of all "provides" endpoints |
| <a name="output_requires"></a> [requires](#output\_requires) | Map of all "requires" endpoints |

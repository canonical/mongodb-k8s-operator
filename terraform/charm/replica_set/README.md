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

No modules.

## Resources

| Name | Type |
|------|------|
| [juju_application.mongodb-k8s](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_app_name"></a> [app\_name](#input\_app\_name) | Application name | `string` | `"mongodb-k8s"` | no |
| <a name="input_base"></a> [base](#input\_base) | Charm base (old name: series) | `string` | `"ubuntu@22.04"` | no |
| <a name="input_channel"></a> [channel](#input\_channel) | Charm channel | `string` | `"6/stable"` | no |
| <a name="input_config"></a> [config](#input\_config) | Map of charm configuration options | `map(string)` | `{}` | no |
| <a name="input_constraints"></a> [constraints](#input\_constraints) | String listing constraints for this application | `string` | `"arch=amd64"` | no |
| <a name="input_endpoint_bindings"></a> [endpoint\_bindings](#input\_endpoint\_bindings) | Map of endpoint bindings | `set(map(string))` | `[]` | no |
| <a name="input_machines"></a> [machines](#input\_machines) | List of machines for placement | `set(string)` | `null` | no |
| <a name="input_model"></a> [model](#input\_model) | Model name | `string` | n/a | yes |
| <a name="input_revision"></a> [revision](#input\_revision) | Charm revision | `number` | `null` | no |
| <a name="input_storage"></a> [storage](#input\_storage) | Map of storage used by the application | `map(string)` | `{}` | no |
| <a name="input_units"></a> [units](#input\_units) | Charm units | `number` | `3` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_app_names"></a> [app\_names](#output\_app\_names) | Names of of all deployed applications. |
| <a name="output_provides"></a> [provides](#output\_provides) | Map of all "provides" endpoints |
| <a name="output_requires"></a> [requires](#output\_requires) | Map of all "requires" endpoints |

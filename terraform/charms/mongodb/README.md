# Terraform module for mongodb-k8s-operator

This is a Terraform module facilitating the deployment of the MongoDB K8s charm with [Terraform juju provider](https://github.com/juju/terraform-provider-juju/). For more information, refer to the provider [documentation](https://registry.terraform.io/providers/juju/juju/latest/docs). 

## Requirements

| Name            | Version |
|-----------------|---------|
| `Terraform`     | >= 1.6  |
| `Juju provider` | ~> 2.0  |

## Providers

| Name   | Version |
|--------|---------|
| `juju` | ~> 2.0  |

## Modules

No modules.

## Resources

| Name                          | Type                                                                                                        |
|-------------------------------|-------------------------------------------------------------------------------------------------------------|
| `juju_application.mongodb_k8s` | [Juju application](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) |

## Inputs

| Name                 | Description                                                        | Type                                                                                                                                            | Default          | Required |
|----------------------|--------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|------------------|:--------:|
| `app_name`           | Application name                                                   | `string`                                                                                                                                        | `"mongodb-k8s"`  | no       |
| `base`               | The operating system on which to deploy. E.g. ubuntu@24.04.        | `string`                                                                                                                                        | `"ubuntu@24.04"` | no       |
| `channel`            | Charm channel                                                      | `string`                                                                                                                                        | `"8/stable"`     | no       |
| `config`             | Map of charm configuration options                                 | `map(string)`                                                                                                                                   | `{}`             | no       |
| `constraints`        | String listing constraints for this application                    | `string`                                                                                                                                        | `"arch=amd64"`   | no       |
| `expose`             | Expose the application for external access.                        | <pre>list(object({<br/>    cidrs     = optional(string)<br/>    endpoints = optional(string)<br/>    spaces    = optional(string)<br/>  }))</pre> | `[]`             | no       |
| `model_uuid`         | Model UUID                                                         | `string`                                                                                                                                        | n/a              | yes      |
| `revision`           | Charm revision                                                     | `number`                                                                                                                                        | `null`           | no       |
| `storage_directives` | Map of storage directives (constraints) for the Juju application   | `map(string)`                                                                                                                                   | `{}`             | no       |
| `units`              | Charm units                                                        | `number`                                                                                                                                        | `3`              | no       |

## Outputs

| Name          | Description                                                   |
|---------------|---------------------------------------------------------------|
| `application` | Object representing the deployed MongoDB application          |
| `offers`      | Map of all offers exposed by the single charm.                |
| `provides`    | Map of all "provides" endpoints                               |
| `requires`    | Map of all "requires" endpoints                               |

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

variable "mongodb_k8s" {
  description = "MongoDB app definition"
  type = object({
    app_name          = optional(string, "mongodb-k8s")
    model             = string
    config            = optional(map(string), { "role" : "replication" })
    channel           = optional(string, "8/edge")
    base              = optional(string, "ubuntu@24.04")
    revision          = optional(string, null)
    units             = optional(number, 3)
    constraints       = optional(string, "arch=amd64")
    machines          = optional(set(string), null)
    storage           = optional(map(string), {})
    endpoint_bindings = optional(set(map(string)), [])
  })
}

variable "self_signed_certificates" {
  description = "Configuration for the self-signed-certificates app"
  type = object({
    app_name          = optional(string, "self-signed-certificates")
    model             = string
    config            = optional(map(string), { "ca-common-name" : "CA" })
    channel           = optional(string, "latest/edge")
    base              = optional(string, "ubuntu@22.04")
    revision          = optional(string, null)
    units             = optional(number, 1)
    constraints       = optional(string, "arch=amd64")
    machines          = optional(set(string), null)
    storage           = optional(map(string), {})
    endpoint_bindings = optional(set(map(string)), [])
  })

  validation {
    condition     = var.self_signed_certificates == null || var.self_signed_certificates.machines == null || length(var.self_signed_certificates.machines) <= 1
    error_message = "Machine count should be at most 1"
  }
}

# Integrators
variable "s3_integrator" {
  description = "Configuration for the backup integrator"
  type = object({
    app_name          = optional(string, "s3-integrator")
    model             = string
    config            = map(string)
    channel           = optional(string, "latest/edge")
    base              = optional(string, "ubuntu@22.04")
    revision          = optional(string, null)
    units             = optional(number, 1)
    constraints       = optional(string, "arch=amd64")
    machines          = optional(set(string), null)
    storage           = optional(map(string), {})
    endpoint_bindings = optional(set(map(string)), [])
  })

  validation {
    condition     = var.s3_integrator.machines == null || length(var.s3_integrator.machines) <= 1
    error_message = "Machines count should be at most 1"
  }
  validation {
    condition     = var.s3_integrator.units == 1
    error_message = "Units count should be 1"
  }
}

variable "data_integrator" {
  description = "Configuration for the data-integrator"
  type = object({
    app_name          = optional(string, "data-integrator")
    model             = string
    config            = optional(map(string), { "database-name" : "test", "extra-user-roles" : "admin" })
    channel           = optional(string, "latest/edge")
    base              = optional(string, "ubuntu@22.04")
    revision          = optional(string, null)
    units             = optional(number, 1)
    constraints       = optional(string, "arch=amd64")
    machines          = optional(set(string), null)
    storage           = optional(map(string), {})
    endpoint_bindings = optional(set(map(string)), [])
  })

  validation {
    condition     = var.data_integrator.machines == null || length(var.data_integrator.machines) <= 1
    error_message = "Machine count should be at most 1"
  }
  validation {
    condition     = var.data_integrator.units == 1
    error_message = "Units count should be 1"
  }
  validation {
    condition = (
      lookup(var.data_integrator.config, "database-name", "") != ""
      && contains(["default", "admin"], lookup(var.data_integrator.config, "extra-user-roles", "admin"))
    )
    error_message = "data-integrator config must contain a non-empty 'database-name' and 'extra-user-roles' must be either 'default' or 'admin'."
  }
}

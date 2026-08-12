# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

variable "config_server" {
  description = "Config server app definition"
  type = object({
    app_name    = optional(string, "config-server")
    base        = optional(string, "ubuntu@24.04")
    channel     = optional(string, "8/stable")
    config      = optional(map(string), { "role" : "config-server" })
    constraints = optional(string, "arch=amd64")
    expose = optional(list(object({
      cidrs     = optional(string)
      endpoints = optional(string)
      spaces    = optional(string)
    })), [])
    model_uuid         = string
    revision           = optional(number, null)
    storage_directives = optional(map(string), {})
    units              = optional(number, 3)
  })

  validation {
    condition     = var.config_server.config["role"] == "config-server"
    error_message = "Config option: 'role' must be set to 'config-server'."
  }

  validation {
    condition     = var.config_server.base == "ubuntu@24.04"
    error_message = "Config server base must be 'ubuntu@24.04'."
  }
}

variable "mongos" {
  description = "Configuration for mongos"
  type = object({
    app_name    = optional(string, "mongos")
    base        = optional(string, "ubuntu@24.04")
    channel     = optional(string, "8/stable")
    config      = optional(map(string), {})
    constraints = optional(string, "arch=amd64")
    model_uuid  = string
    revision    = optional(number, null)
    units       = optional(number, 3)
  })

  validation {
    condition     = var.mongos.base == "ubuntu@24.04"
    error_message = "mongos base must be 'ubuntu@24.04'."
  }
}

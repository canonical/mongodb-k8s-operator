# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

variable "config_server" {
  description = "Config server app definition"
  type = object({
    app_name          = string
    model             = string
    config            = optional(map(string), { "role" : "config-server" })
    channel           = optional(string, "8/edge")
    base              = optional(string, "ubuntu@24.04")
    revision          = optional(string, null)
    units             = optional(number, 3)
    constraints       = optional(string, "arch=amd64")
    machines          = optional(set(string), null)
    storage           = optional(map(string), {})
    endpoint_bindings = optional(set(map(string)), [])
  })

  validation {
    condition     = var.config_server.config["role"] == "config-server"
    error_message = "Config option: 'role' must be set to 'config-server'."
  }
}

variable "shards" {
  description = "Shard apps"
  type = list(object({
    app_name          = string
    model             = string
    config            = optional(map(string), { "role" : "shard" })
    channel           = optional(string, "8/edge")
    base              = optional(string, "ubuntu@24.04")
    revision          = optional(string, null)
    units             = optional(number, 3)
    constraints       = optional(string, "arch=amd64")
    machines          = optional(set(string), null)
    storage           = optional(map(string), {})
    endpoint_bindings = optional(set(map(string)), [])
  }))
  default = []

  validation {
    condition     = alltrue([for shard in var.shards : (shard.config["role"] == "shard")])
    error_message = "Config option: 'role' must be set to 'shard' in all shard objects."
  }
}

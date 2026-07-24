# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# Applications
#--------------------------------------------------------

variable "backups_integrator" {
  description = "Optional configuration for the backup integrator, including the model in which it is deployed."
  type = object({
    storage_type = optional(string, "s3")
    config       = map(string)
    channel      = optional(string, null)
    base         = optional(string, "ubuntu@24.04")
    revision     = optional(number, null)
    constraints  = optional(string, "arch=amd64")
    machines     = optional(set(string), [])
    model_uuid   = string
  })
  default = null

  validation {
    condition     = try(contains(["s3", "gcs"], var.backups_integrator.storage_type), true)
    error_message = "backups_integrator allows one of the values: 's3', 'gcs' for storage_type."
  }

  validation {
    condition     = try(length(var.backups_integrator.machines) <= 1, true)
    error_message = "Machine count should be at most 1"
  }

  validation {
    condition     = try(var.backups_integrator.model_uuid != "", true)
    error_message = "backups_integrator.model_uuid must not be empty."
  }
}

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

variable "data_integrator" {
  description = "Configuration for the data-integrator"
  type = object({
    app_name    = optional(string, "data-integrator")
    base        = optional(string, "ubuntu@24.04")
    channel     = optional(string, "latest/edge")
    config      = optional(map(string), { "database-name" : "test", "extra-user-roles" : "admin" })
    constraints = optional(string, "arch=amd64")
    endpoint_bindings = optional(set(object({
      space    = string
      endpoint = optional(string)
    })), [])
    machines           = optional(set(string), [])
    model_uuid         = string
    revision           = optional(number, null)
    storage_directives = optional(map(string), {})
    units              = optional(number, 1)
  })

  validation {
    condition     = var.mongos.model_uuid == var.data_integrator.model_uuid
    error_message = "'mongos' and 'data_integrator' should have the same model_uuid."
  }

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

variable "shards" {
  description = "Shard apps"
  type = list(object({
    app_name    = string
    base        = optional(string, "ubuntu@24.04")
    channel     = optional(string, "8/stable")
    config      = optional(map(string), { "role" : "shard" })
    constraints = optional(string, "arch=amd64")
    expose = optional(list(object({
      cidrs     = optional(string)
      endpoints = optional(string)
    })), [])
    model_uuid         = string
    revision           = optional(number, null)
    storage_directives = optional(map(string), {})
    units              = optional(number, 3)
  }))
  default = []

  validation {
    condition     = alltrue([for shard in var.shards : (shard.config["role"] == "shard")])
    error_message = "Config option: 'role' must be set to 'shard' in all shard objects."
  }

  validation {
    condition     = alltrue([for shard in var.shards : shard.base == "ubuntu@24.04"])
    error_message = "All shard bases must be 'ubuntu@24.04'."
  }
}

#--------------------------------------------------------
# Integrations
#--------------------------------------------------------


variable "client_certificates_integration" {
  description = "Optional client TLS certificates integration target."
  type = object({
    name       = optional(string, null)
    endpoint   = optional(string, null)
    model_uuid = optional(string, null)
    url        = optional(string, null)
  })
  default = null

  validation {
    condition = (
      var.client_certificates_integration == null ? true : (
        var.client_certificates_integration.name != null && var.client_certificates_integration.name != "" &&
        var.client_certificates_integration.endpoint != null && var.client_certificates_integration.endpoint != "" &&
        var.client_certificates_integration.model_uuid != null && var.client_certificates_integration.model_uuid != ""
      )
    )
    error_message = "client_certificates_integration must include non-empty 'name', 'endpoint', and 'model_uuid' attributes."
  }
}

variable "grafana_dashboard_integration" {
  description = "Optional Grafana dashboard integration target for the config server and shards."
  type = object({
    name       = string
    endpoint   = string
    model_uuid = string
    url        = optional(string, null)
  })
  default = null
}

variable "metrics_endpoint_integration" {
  description = "Optional metrics endpoint integration target for the config server and shards."
  type = object({
    name       = string
    endpoint   = string
    model_uuid = string
    url        = optional(string, null)
  })
  default = null
}

variable "logging_integration" {
  description = "Optional logging integration target for the config server and shards."
  type = object({
    name       = string
    endpoint   = string
    model_uuid = string
    url        = optional(string, null)
  })
  default = null
}

variable "ldap_integration" {
  description = "Optional LDAP integration target. Must be configured together with ldap_certificate_transfer_integration."
  type = object({
    name       = optional(string, null)
    endpoint   = optional(string, null)
    model_uuid = optional(string, null)
    url        = optional(string, null)
  })
  default = null

  validation {
    condition = (
      var.ldap_integration == null ? true : (
        var.ldap_integration.name != null && var.ldap_integration.name != "" &&
        var.ldap_integration.endpoint != null && var.ldap_integration.endpoint != "" &&
        var.ldap_integration.model_uuid != null && var.ldap_integration.model_uuid != ""
      )
    )
    error_message = "ldap_integration must include non-empty 'name', 'endpoint', and 'model_uuid' attributes."
  }
}

variable "ldap_certificate_transfer_integration" {
  description = "Optional LDAP certificate transfer integration target. Must be configured together with ldap_integration."
  type = object({
    name       = optional(string, null)
    endpoint   = optional(string, null)
    model_uuid = optional(string, null)
    url        = optional(string, null)
  })
  default = null

  validation {
    condition = (
      var.ldap_certificate_transfer_integration == null ? true : (
        var.ldap_certificate_transfer_integration.name != null && var.ldap_certificate_transfer_integration.name != "" &&
        var.ldap_certificate_transfer_integration.endpoint != null && var.ldap_certificate_transfer_integration.endpoint != "" &&
        var.ldap_certificate_transfer_integration.model_uuid != null && var.ldap_certificate_transfer_integration.model_uuid != ""
      )
    )
    error_message = "ldap_certificate_transfer_integration must include non-empty 'name', 'endpoint', and 'model_uuid' attributes."
  }
}

variable "peer_certificates_integration" {
  description = "Optional peer TLS certificates integration target."
  type = object({
    name       = optional(string, null)
    endpoint   = optional(string, null)
    model_uuid = optional(string, null)
    url        = optional(string, null)
  })
  default = null

  validation {
    condition = (
      var.peer_certificates_integration == null ? true : (
        var.peer_certificates_integration.name != null && var.peer_certificates_integration.name != "" &&
        var.peer_certificates_integration.endpoint != null && var.peer_certificates_integration.endpoint != "" &&
        var.peer_certificates_integration.model_uuid != null && var.peer_certificates_integration.model_uuid != ""
      )
    )
    error_message = "peer_certificates_integration must include non-empty 'name', 'endpoint', and 'model_uuid' attributes."
  }
}

variable "vault_kv_integration" {
  description = "Optional Vault KV integration target for encryption at rest. Must be provided when enable-encryption-at-rest is true for any config server or shard; only enabled applications are integrated."
  type = object({
    name       = optional(string, null)
    endpoint   = optional(string, null)
    model_uuid = optional(string, null)
    url        = optional(string, null)
  })
  default = null

  validation {
    condition = (
      var.vault_kv_integration == null ? true : (
        var.vault_kv_integration.name != null && var.vault_kv_integration.name != "" &&
        var.vault_kv_integration.endpoint != null && var.vault_kv_integration.endpoint != "" &&
        var.vault_kv_integration.model_uuid != null && var.vault_kv_integration.model_uuid != ""
      )
    )
    error_message = "vault_kv_integration must include non-empty 'name', 'endpoint', and 'model_uuid' attributes."
  }
}


#--------------------------------------------------------
# Config
#--------------------------------------------------------

variable "logging_config" {
  description = "Logging configuration to be used"
  type        = string
  default     = "<root>=INFO"
}

variable "gcs_secret_key" {
  description = "GCP service-account JSON key for GCS credentials."
  type        = string
  sensitive   = true
  default     = null
}

variable "s3_access_key" {
  description = "S3 access key."
  type        = string
  sensitive   = true
  default     = null
}

variable "s3_secret_key" {
  description = "S3 secret key."
  type        = string
  sensitive   = true
  default     = null
}

variable "tls_client_private_key" {
  description = "Private key for client-to-server TLS certificates on the config server. When set, the module stores it in a Juju secret and configures the config server with the secret URI."
  type        = string
  sensitive   = true
  default     = null
}

variable "tls_peer_private_key" {
  description = "Private key for peer-to-peer TLS certificates on the config server. When set, the module stores it in a Juju secret and configures the config server with the secret URI."
  type        = string
  sensitive   = true
  default     = null
}

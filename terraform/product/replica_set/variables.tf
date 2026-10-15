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
}

variable "data_integrator" {
  description = "Configuration for the data-integrator"
  type = object({
    app_name    = optional(string, "data-integrator")
    base        = optional(string, "ubuntu@24.04")
    channel     = optional(string, "latest/stable")
    config      = optional(map(string), { "database-name" : "test", "extra-user-roles" : "admin" })
    constraints = optional(string, "arch=amd64")
    endpoint_bindings = optional(set(object({
      space    = string
      endpoint = optional(string)
    })), [])
    machines           = optional(set(string), null)
    model_uuid         = string
    revision           = optional(number, null)
    storage_directives = optional(map(string), {})
    units              = optional(number, 1)
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

variable "mongodb" {
  description = "MongoDB app definition"
  type = object({
    app_name    = optional(string, "mongodb-k8s")
    base        = optional(string, "ubuntu@24.04")
    channel     = optional(string, "8/stable")
    config      = optional(map(string), { "role" : "replication" })
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
}


#--------------------------------------------------------
# Integrations
#--------------------------------------------------------

variable "client_certificates_integration" {
  description = "Optional client TLS certificates integration target. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.client_certificates_integration == null || contains(["endpoint", "offer"], var.client_certificates_integration.kind)
    error_message = "client_certificates_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.client_certificates_integration == null ? true :
      var.client_certificates_integration.kind == "endpoint" ? (
        var.client_certificates_integration.name != null && var.client_certificates_integration.name != "" &&
        var.client_certificates_integration.endpoint != null && var.client_certificates_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.client_certificates_integration == null ? true :
      var.client_certificates_integration.kind == "offer" ? (
        var.client_certificates_integration.url != null && var.client_certificates_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
  }
}

variable "grafana_dashboard_integration" {
  description = "Optional Grafana dashboard integration target. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.grafana_dashboard_integration == null || contains(["endpoint", "offer"], var.grafana_dashboard_integration.kind)
    error_message = "grafana_dashboard_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.grafana_dashboard_integration == null ? true :
      var.grafana_dashboard_integration.kind == "endpoint" ? (
        var.grafana_dashboard_integration.name != null && var.grafana_dashboard_integration.name != "" &&
        var.grafana_dashboard_integration.endpoint != null && var.grafana_dashboard_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.grafana_dashboard_integration == null ? true :
      var.grafana_dashboard_integration.kind == "offer" ? (
        var.grafana_dashboard_integration.url != null && var.grafana_dashboard_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
  }
}

variable "metrics_endpoint_integration" {
  description = "Optional metrics endpoint integration target. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.metrics_endpoint_integration == null || contains(["endpoint", "offer"], var.metrics_endpoint_integration.kind)
    error_message = "metrics_endpoint_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.metrics_endpoint_integration == null ? true :
      var.metrics_endpoint_integration.kind == "endpoint" ? (
        var.metrics_endpoint_integration.name != null && var.metrics_endpoint_integration.name != "" &&
        var.metrics_endpoint_integration.endpoint != null && var.metrics_endpoint_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.metrics_endpoint_integration == null ? true :
      var.metrics_endpoint_integration.kind == "offer" ? (
        var.metrics_endpoint_integration.url != null && var.metrics_endpoint_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
  }
}

variable "logging_integration" {
  description = "Optional logging integration target. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.logging_integration == null || contains(["endpoint", "offer"], var.logging_integration.kind)
    error_message = "logging_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.logging_integration == null ? true :
      var.logging_integration.kind == "endpoint" ? (
        var.logging_integration.name != null && var.logging_integration.name != "" &&
        var.logging_integration.endpoint != null && var.logging_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.logging_integration == null ? true :
      var.logging_integration.kind == "offer" ? (
        var.logging_integration.url != null && var.logging_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
  }
}

variable "ldap_integration" {
  description = "Optional LDAP integration target. Must be configured together with ldap_certificate_transfer_integration. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.ldap_integration == null || contains(["endpoint", "offer"], var.ldap_integration.kind)
    error_message = "ldap_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.ldap_integration == null ? true :
      var.ldap_integration.kind == "endpoint" ? (
        var.ldap_integration.name != null && var.ldap_integration.name != "" &&
        var.ldap_integration.endpoint != null && var.ldap_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.ldap_integration == null ? true :
      var.ldap_integration.kind == "offer" ? (
        var.ldap_integration.url != null && var.ldap_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
  }
}

variable "ldap_certificate_transfer_integration" {
  description = "Optional LDAP certificate transfer integration target. Must be configured together with ldap_integration. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.ldap_certificate_transfer_integration == null || contains(["endpoint", "offer"], var.ldap_certificate_transfer_integration.kind)
    error_message = "ldap_certificate_transfer_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.ldap_certificate_transfer_integration == null ? true :
      var.ldap_certificate_transfer_integration.kind == "endpoint" ? (
        var.ldap_certificate_transfer_integration.name != null && var.ldap_certificate_transfer_integration.name != "" &&
        var.ldap_certificate_transfer_integration.endpoint != null && var.ldap_certificate_transfer_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.ldap_certificate_transfer_integration == null ? true :
      var.ldap_certificate_transfer_integration.kind == "offer" ? (
        var.ldap_certificate_transfer_integration.url != null && var.ldap_certificate_transfer_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
  }
}

variable "peer_certificates_integration" {
  description = "Optional peer TLS certificates integration target. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.peer_certificates_integration == null || contains(["endpoint", "offer"], var.peer_certificates_integration.kind)
    error_message = "peer_certificates_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.peer_certificates_integration == null ? true :
      var.peer_certificates_integration.kind == "endpoint" ? (
        var.peer_certificates_integration.name != null && var.peer_certificates_integration.name != "" &&
        var.peer_certificates_integration.endpoint != null && var.peer_certificates_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.peer_certificates_integration == null ? true :
      var.peer_certificates_integration.kind == "offer" ? (
        var.peer_certificates_integration.url != null && var.peer_certificates_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
  }
}

variable "vault_kv_integration" {
  description = "Optional Vault KV integration target for encryption at rest. Must be configured together with mongodb.config[\"enable-encryption-at-rest\"]. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.vault_kv_integration == null || contains(["endpoint", "offer"], var.vault_kv_integration.kind)
    error_message = "vault_kv_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.vault_kv_integration == null ? true :
      var.vault_kv_integration.kind == "endpoint" ? (
        var.vault_kv_integration.name != null && var.vault_kv_integration.name != "" &&
        var.vault_kv_integration.endpoint != null && var.vault_kv_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.vault_kv_integration == null ? true :
      var.vault_kv_integration.kind == "offer" ? (
        var.vault_kv_integration.url != null && var.vault_kv_integration.url != ""
      ) : true
    )
    error_message = "The 'url' attribute must be provided for a cross-model integration."
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
  description = "Gcs secret key. Supply through TF_VAR_gcs_secret_key or -var, at plan and at apply. Warning: passing credentials through -var may expose them in shell history."
  type        = string
  sensitive   = true
  ephemeral   = true
  default     = null
}

variable "gcs_secret_version" {
  description = "0 creates no secret. 1 creates it. Increment to rotate."
  type        = number
  default     = 0

  validation {
    condition     = var.gcs_secret_uri != null || var.gcs_secret_version == 0 || local.gcs_integrator_enabled
    error_message = "gcs_secret_version (unless gcs_secret_uri is set) requires backups_integrator.storage_type = \"gcs\"."
  }
}


variable "s3_access_key" {
  description = "S3 access key. Supply through TF_VAR_s3_access_key or -var, at plan and at apply. Warning: passing credentials through -var may expose them in shell history."
  type        = string
  sensitive   = true
  ephemeral   = true
  default     = null
}

variable "s3_secret_key" {
  description = "S3 secret key. Supply through TF_VAR_s3_secret_key or -var, at plan and at apply. Warning: passing credentials through -var may expose them in shell history."
  type        = string
  sensitive   = true
  ephemeral   = true
  default     = null
}

variable "s3_secret_version" {
  description = "0 creates no secret. 1 creates it. Increment to rotate."
  type        = number
  default     = 0

  validation {
    condition     = var.s3_secret_uri != null || var.s3_secret_version == 0 || local.s3_integrator_enabled
    error_message = "s3_secret_version (unless s3_secret_uri is set) requires backups_integrator.storage_type = \"s3\"."
  }
}


variable "tls_client_private_key" {
  description = "Tls client private key. Supply through TF_VAR_tls_client_private_key or -var, at plan and at apply. Warning: passing credentials through -var may expose them in shell history."
  type        = string
  sensitive   = true
  ephemeral   = true
  default     = null
}

variable "tls_client_private_key_version" {
  description = "0 creates no secret. 1 creates it. Increment to rotate."
  type        = number
  default     = 0
}


variable "tls_peer_private_key" {
  description = "Tls peer private key. Supply through TF_VAR_tls_peer_private_key or -var, at plan and at apply. Warning: passing credentials through -var may expose them in shell history."
  type        = string
  sensitive   = true
  ephemeral   = true
  default     = null
}

variable "tls_peer_private_key_version" {
  description = "0 creates no secret. 1 creates it. Increment to rotate."
  type        = number
  default     = 0
}

variable "tls_client_private_key_secret_uri" {
  description = "URI of an existing Juju secret in the consuming application's model. Takes priority over credential inputs and tls_client_private_key_version."
  type        = string
  default     = null

  validation {
    condition     = var.tls_client_private_key_secret_uri == null || startswith(coalesce(var.tls_client_private_key_secret_uri, "-"), "secret:")
    error_message = "tls_client_private_key_secret_uri must be a Juju secret URI starting with 'secret:'."
  }
}

variable "tls_peer_private_key_secret_uri" {
  description = "URI of an existing Juju secret in the consuming application's model. Takes priority over credential inputs and tls_peer_private_key_version."
  type        = string
  default     = null

  validation {
    condition     = var.tls_peer_private_key_secret_uri == null || startswith(coalesce(var.tls_peer_private_key_secret_uri, "-"), "secret:")
    error_message = "tls_peer_private_key_secret_uri must be a Juju secret URI starting with 'secret:'."
  }
}

variable "s3_secret_uri" {
  description = "URI of an existing Juju secret in the consuming application's model. Takes priority over credential inputs and s3_secret_version."
  type        = string
  default     = null

  validation {
    condition     = var.s3_secret_uri == null || startswith(coalesce(var.s3_secret_uri, "-"), "secret:")
    error_message = "s3_secret_uri must be a Juju secret URI starting with 'secret:'."
  }
}

variable "gcs_secret_uri" {
  description = "URI of an existing Juju secret in the consuming application's model. Takes priority over credential inputs and gcs_secret_version."
  type        = string
  default     = null

  validation {
    condition     = var.gcs_secret_uri == null || startswith(coalesce(var.gcs_secret_uri, "-"), "secret:")
    error_message = "gcs_secret_uri must be a Juju secret URI starting with 'secret:'."
  }
}

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
    channel      = optional(string, "2/stable")
    base         = optional(string, "ubuntu@24.04")
    revision     = optional(number, null)
    constraints  = optional(string, "arch=amd64")
    machines     = optional(set(string), [])
    model_uuid   = string
  })
  default = null

  validation {
    condition     = try(contains(["s3"], var.backups_integrator.storage_type), true)
    error_message = "backups_integrator only allows 's3' for storage_type."
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
    base        = optional(string, "ubuntu@22.04")
    channel     = optional(string, "6/stable")
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

variable "certificates_integration" {
  description = "Optional TLS certificates integration target. Use kind = \"endpoint\" with name/endpoint for same-model integrations, or kind = \"offer\" with url for cross-model integrations."
  type = object({
    kind     = string
    name     = optional(string, null)
    endpoint = optional(string, null)
    url      = optional(string, null)
  })
  default = null

  validation {
    condition     = var.certificates_integration == null || contains(["endpoint", "offer"], var.certificates_integration.kind)
    error_message = "certificates_integration.kind must be either \"endpoint\" or \"offer\"."
  }

  validation {
    condition = (
      var.certificates_integration == null ? true :
      var.certificates_integration.kind == "endpoint" ? (
        var.certificates_integration.name != null && var.certificates_integration.name != "" &&
        var.certificates_integration.endpoint != null && var.certificates_integration.endpoint != ""
      ) : true
    )
    error_message = "Both 'name' and 'endpoint' attributes must be provided for an in-model integration."
  }

  validation {
    condition = (
      var.certificates_integration == null ? true :
      var.certificates_integration.kind == "offer" ? (
        var.certificates_integration.url != null && var.certificates_integration.url != ""
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

#--------------------------------------------------------
# Config
#--------------------------------------------------------

variable "logging_config" {
  description = "Logging configuration to be used"
  type        = string
  default     = "<root>=INFO"
}

variable "s3_access_key" {
  description = "AWS S3 Access key."
  type        = string
  sensitive   = true
  default     = null
}

variable "s3_secret_key" {
  description = "AWS S3 Secret key."
  type        = string
  sensitive   = true
  default     = null
}

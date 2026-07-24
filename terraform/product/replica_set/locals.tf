# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  backups_integrator_channel    = var.backups_integrator != null ? coalesce(var.backups_integrator.channel, var.backups_integrator.storage_type == "s3" ? "2/stable" : "1/stable") : null
  gcs_integrator_enabled        = try(var.backups_integrator.storage_type == "gcs", false)
  s3_integrator_enabled         = try(var.backups_integrator.storage_type == "s3", false)
  encryption_at_rest_enabled    = var.vault_kv_integration != null ? true : false
  encryption_at_rest_configured = contains(keys(var.mongodb.config), "enable-encryption-at-rest")
  client_certificates_enabled   = var.client_certificates_integration != null ? true : false
  grafana_dashboard_enabled     = var.grafana_dashboard_integration != null ? true : false
  ldap_enabled                  = var.ldap_integration != null && var.ldap_certificate_transfer_integration != null ? true : false
  logging_enabled               = var.logging_integration != null ? true : false
  metrics_endpoint_enabled      = var.metrics_endpoint_integration != null ? true : false
  peer_certificates_enabled     = var.peer_certificates_integration != null ? true : false

  ldap_integrations = compact([
    var.ldap_integration != null ? "ldap_integration" : "",
    var.ldap_certificate_transfer_integration != null ? "ldap_certificate_transfer_integration" : "",
  ])

  model_components = concat(
    [
      {
        key        = "mongodb"
        model_uuid = module.mongodb.application.model_uuid
        value      = module.mongodb.application
      },
      {
        key        = "data_integrator"
        model_uuid = module.data_integrator.application.model_uuid
        value      = module.data_integrator.application
      },
    ],
    local.s3_integrator_enabled ? [
      {
        key        = "s3_integrator"
        model_uuid = module.s3_integrator[0].application.model_uuid
        value      = module.s3_integrator[0].application
      }
    ] : [],
    local.gcs_integrator_enabled ? [
      {
        key        = "gcs_integrator"
        model_uuid = module.gcs_integrator[0].application.model_uuid
        value      = module.gcs_integrator[0].application
      }
    ] : []
  )
}


resource "terraform_data" "deployed_at" {
  input = timestamp()

  lifecycle {
    ignore_changes = [input]
  }
}

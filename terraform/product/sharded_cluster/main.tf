# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 1. DEPLOYMENTS
#--------------------------------------------------------

module "config_and_routing" {
  source = "../../components/config_and_routing"

  config_server = merge(
    var.config_server,
    {
      config = merge(
        var.config_server.config,
        {
          "role" : "config-server"
        }
      )
    }
  )
  mongos = var.mongos
}

# shard apps
module "shards" {
  for_each = { for idx, app in local.shards : idx => app if app != null }
  source   = "../../charms/mongodb"

  app_name = each.value.app_name
  base     = each.value.base
  channel  = each.value.channel
  config = merge(
    each.value.config,
    { "role" : "shard" }
  )
  constraints        = each.value.constraints
  expose             = each.value.expose
  model_uuid         = each.value.model_uuid
  revision           = each.value.revision
  storage_directives = each.value.storage_directives
  units              = each.value.units
}

resource "terraform_data" "validate_ldap_integrations" {
  input = local.ldap_integrations

  lifecycle {
    precondition {
      condition     = length(local.ldap_integrations) == 0 || length(local.ldap_integrations) == 2
      error_message = "LDAP integrations must be configured together: set both ldap_integration and ldap_certificate_transfer_integration, or neither."
    }
  }
}

resource "terraform_data" "validate_cross_model_integration_urls" {
  input = {
    certificates_cross_model_apps              = local.certificates_cross_model_apps
    ldap_cross_model_apps                      = local.ldap_cross_model_apps
    ldap_certificate_transfer_cross_model_apps = local.ldap_certificate_transfer_cross_model_apps
    logging_cross_model_apps                   = local.logging_cross_model_apps
  }

  lifecycle {
    precondition {
      condition     = length(local.certificates_cross_model_apps) == 0 || try(var.certificates_integration.url != null && var.certificates_integration.url != "", false)
      error_message = "certificates_integration.url must be provided when certificates are cross-model from any MongoDB application."
    }
    precondition {
      condition     = length(local.logging_cross_model_apps) == 0 || try(var.logging_integration.url != null && var.logging_integration.url != "", false)
      error_message = "logging_integration.url must be provided when logging is cross-model from any MongoDB application."
    }
    precondition {
      condition     = length(local.ldap_cross_model_apps) == 0 || try(var.ldap_integration.url != null && var.ldap_integration.url != "", false)
      error_message = "ldap_integration.url must be provided when LDAP is cross-model from the config server or mongos."
    }
    precondition {
      condition     = length(local.ldap_certificate_transfer_cross_model_apps) == 0 || try(var.ldap_certificate_transfer_integration.url != null && var.ldap_certificate_transfer_integration.url != "", false)
      error_message = "ldap_certificate_transfer_integration.url must be provided when LDAP certificate transfer is cross-model from the config server or mongos."
    }
  }
}

# Integrator apps
module "data_integrator" {
  source = "git::https://github.com/canonical/data-integrator.git//terraform/charm/data_integrator?ref=main"

  app_name           = var.data_integrator.app_name
  base               = var.data_integrator.base
  channel            = var.data_integrator.channel
  config             = var.data_integrator.config
  constraints        = var.data_integrator.constraints
  endpoint_bindings  = var.data_integrator.endpoint_bindings
  machines           = var.data_integrator.machines
  model_uuid         = var.data_integrator.model_uuid
  revision           = var.data_integrator.revision
  storage_directives = var.data_integrator.storage_directives
  units              = var.data_integrator.units
}

resource "juju_secret" "s3_secret" {
  count      = local.s3_credentials_enabled && var.s3_access_key != null && var.s3_secret_key != null ? 1 : 0
  model_uuid = var.backups_integrator.model_uuid
  name       = "s3-integrator-credentials"
  value = {
    access-key = var.s3_access_key
    secret-key = var.s3_secret_key
  }
  info = "S3 credentials for s3-integrator"
}

module "s3_integrator" {
  depends_on = [juju_secret.s3_secret]
  count      = local.s3_credentials_enabled ? 1 : 0
  source     = "git::https://github.com/canonical/object-storage-integrator.git//s3/terraform/charm/s3_integrator?ref=main"

  app_name = "s3-integrator"
  base     = var.backups_integrator.base
  channel  = var.backups_integrator.channel
  config = merge(
    var.backups_integrator.config,
    length(juju_secret.s3_secret) > 0 ? {
      credentials = juju_secret.s3_secret[0].secret_uri
    } : {}
  )
  constraints = var.backups_integrator.constraints
  machines    = var.backups_integrator.machines
  model_uuid  = var.backups_integrator.model_uuid
  revision    = var.backups_integrator.revision
  units       = 1
}

resource "juju_access_secret" "s3_secret_access" {
  depends_on = [juju_secret.s3_secret, module.s3_integrator]
  count      = length(juju_secret.s3_secret) > 0 ? 1 : 0
  model_uuid = var.backups_integrator.model_uuid
  applications = [
    module.s3_integrator[0].application.name
  ]
  secret_id = juju_secret.s3_secret[0].secret_id
}

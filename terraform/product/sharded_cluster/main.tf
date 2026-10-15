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
        local.use_config_server_tls_client_private_key ? {
          "tls-client-private-key" = local.config_server_tls_client_private_key_secret_uri
        } : {},
        local.use_config_server_tls_peer_private_key ? {
          "tls-peer-private-key" = local.config_server_tls_peer_private_key_secret_uri
        } : {}
      )
    }
  )
  mongos = var.mongos
}

resource "juju_secret" "config_server_tls_client_private_key" {
  count            = local.create_config_server_tls_client_private_key ? 1 : 0
  model_uuid       = var.config_server.model_uuid
  name             = "${var.config_server.app_name}-tls-client-private-key"
  value_wo         = { private-key = var.config_server_tls_client_private_key }
  info             = "TLS client private key for ${var.config_server.app_name}"
  value_wo_version = var.config_server_tls_client_private_key_version
}

resource "juju_access_secret" "config_server_tls_client_private_key" {
  depends_on   = [module.config_and_routing]
  count        = local.use_config_server_tls_client_private_key ? 1 : 0
  model_uuid   = var.config_server.model_uuid
  applications = [module.config_and_routing.components["config_server"].name]
  secret_id    = regex("[^/:]+$", local.config_server_tls_client_private_key_secret_uri)
}

resource "juju_secret" "config_server_tls_peer_private_key" {
  count            = local.create_config_server_tls_peer_private_key ? 1 : 0
  model_uuid       = var.config_server.model_uuid
  name             = "${var.config_server.app_name}-tls-peer-private-key"
  value_wo         = { private-key = var.config_server_tls_peer_private_key }
  info             = "TLS peer private key for ${var.config_server.app_name}"
  value_wo_version = var.config_server_tls_peer_private_key_version
}

resource "juju_access_secret" "config_server_tls_peer_private_key" {
  depends_on   = [module.config_and_routing]
  count        = local.use_config_server_tls_peer_private_key ? 1 : 0
  model_uuid   = var.config_server.model_uuid
  applications = [module.config_and_routing.components["config_server"].name]
  secret_id    = regex("[^/:]+$", local.config_server_tls_peer_private_key_secret_uri)
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

resource "terraform_data" "validate_encryption_at_rest" {
  input = {
    encryption_at_rest_apps = local.encryption_at_rest_apps
    vault_kv_integration    = var.vault_kv_integration
  }

  lifecycle {
    precondition {
      condition     = (length(local.encryption_at_rest_apps) > 0) == (var.vault_kv_integration != null)
      error_message = "Encryption at rest must be configured together: provide vault_kv_integration when enable-encryption-at-rest is true for any MongoDB application, or configure neither."
    }
  }
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
    client_certificates_cross_model_apps       = local.client_certificates_cross_model_apps
    ldap_cross_model_apps                      = local.ldap_cross_model_apps
    ldap_certificate_transfer_cross_model_apps = local.ldap_certificate_transfer_cross_model_apps
    logging_cross_model_apps                   = local.logging_cross_model_apps
    peer_certificates_cross_model_apps         = local.peer_certificates_cross_model_apps
    vault_kv_cross_model_apps                  = local.vault_kv_cross_model_apps
  }

  lifecycle {
    precondition {
      condition     = length(local.client_certificates_cross_model_apps) == 0 || try(var.client_certificates_integration.url != null && var.client_certificates_integration.url != "", false)
      error_message = "client_certificates_integration.url must be provided when client certificates is cross-model from any MongoDB application."
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
    precondition {
      condition     = length(local.peer_certificates_cross_model_apps) == 0 || try(var.peer_certificates_integration.url != null && var.peer_certificates_integration.url != "", false)
      error_message = "peer_certificates_integration.url must be provided when peer certificates is cross-model from any MongoDB application."
    }
    precondition {
      condition     = length(local.vault_kv_cross_model_apps) == 0 || try(var.vault_kv_integration.url != null && var.vault_kv_integration.url != "", false)
      error_message = "vault_kv_integration.url must be provided when Vault KV is cross-model from any MongoDB application."
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

module "gcs_integrator" {
  count  = local.gcs_credentials_enabled ? 1 : 0
  source = "git::https://github.com/canonical/object-storage-integrator.git//gcs/terraform/charm/gcs_integrator?ref=main"

  app_name = "gcs-integrator"
  base     = var.backups_integrator.base
  channel  = local.backups_integrator_channel
  config = merge(
    var.backups_integrator.config,
    local.use_gcs_secret ? {
      credentials = local.gcs_secret_uri
    } : {}
  )
  constraints = var.backups_integrator.constraints
  machines    = var.backups_integrator.machines
  model_uuid  = var.backups_integrator.model_uuid
  revision    = var.backups_integrator.revision
  units       = 1
}

resource "juju_secret" "gcs_secret" {
  count            = local.create_gcs_secret ? 1 : 0
  model_uuid       = var.backups_integrator.model_uuid
  name             = "gcs-integrator-credentials"
  value_wo         = { secret-key = var.gcs_secret_key }
  info             = "GCS credentials for gcs-integrator"
  value_wo_version = var.gcs_secret_version
}

resource "juju_access_secret" "gcs_secret_access" {
  depends_on   = [module.gcs_integrator]
  count        = local.use_gcs_secret ? 1 : 0
  model_uuid   = var.backups_integrator.model_uuid
  applications = [module.gcs_integrator[0].application.name]
  secret_id    = regex("[^/:]+$", local.gcs_secret_uri)
}

resource "juju_secret" "s3_secret" {
  count            = local.create_s3_secret ? 1 : 0
  model_uuid       = var.backups_integrator.model_uuid
  name             = "s3-integrator-credentials"
  value_wo         = { access-key = var.s3_access_key, secret-key = var.s3_secret_key }
  info             = "S3 credentials for s3-integrator"
  value_wo_version = var.s3_secret_version
}

module "s3_integrator" {
  count  = local.s3_credentials_enabled ? 1 : 0
  source = "git::https://github.com/canonical/object-storage-integrator.git//s3/terraform/charm/s3_integrator?ref=main"

  app_name = "s3-integrator"
  base     = var.backups_integrator.base
  channel  = local.backups_integrator_channel
  config = merge(
    var.backups_integrator.config,
    local.use_s3_secret ? {
      credentials = local.s3_secret_uri
    } : {}
  )
  constraints = var.backups_integrator.constraints
  machines    = var.backups_integrator.machines
  model_uuid  = var.backups_integrator.model_uuid
  revision    = var.backups_integrator.revision
  units       = 1
}

resource "juju_access_secret" "s3_secret_access" {
  depends_on   = [module.s3_integrator]
  count        = local.use_s3_secret ? 1 : 0
  model_uuid   = var.backups_integrator.model_uuid
  applications = [module.s3_integrator[0].application.name]
  secret_id    = regex("[^/:]+$", local.s3_secret_uri)
}

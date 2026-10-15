# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 1. DEPLOYMENTS
#--------------------------------------------------------

# Replica set MongoDB app
module "mongodb" {
  source = "../../charms/mongodb"

  app_name = var.mongodb.app_name
  base     = var.mongodb.base
  channel  = var.mongodb.channel
  config = merge(
    var.mongodb.config,
    { "role" : "replication" },
    local.use_tls_client_private_key ? {
      "tls-client-private-key" = local.tls_client_private_key_secret_uri
    } : {},
    local.use_tls_peer_private_key ? {
      "tls-peer-private-key" = local.tls_peer_private_key_secret_uri
    } : {}
  )
  constraints        = var.mongodb.constraints
  expose             = var.mongodb.expose
  model_uuid         = var.mongodb.model_uuid
  revision           = var.mongodb.revision
  storage_directives = var.mongodb.storage_directives
  units              = var.mongodb.units
}

resource "juju_secret" "tls_client_private_key" {
  count            = local.create_tls_client_private_key ? 1 : 0
  model_uuid       = var.mongodb.model_uuid
  name             = "${var.mongodb.app_name}-tls-client-private-key"
  value_wo         = { private-key = var.tls_client_private_key }
  info             = "TLS client private key for ${var.mongodb.app_name}"
  value_wo_version = var.tls_client_private_key_version
}

resource "juju_access_secret" "tls_client_private_key" {
  depends_on   = [module.mongodb]
  count        = local.use_tls_client_private_key ? 1 : 0
  model_uuid   = var.mongodb.model_uuid
  applications = [module.mongodb.application.name]
  secret_id    = regex("[^/:]+$", local.tls_client_private_key_secret_uri)
}

resource "juju_secret" "tls_peer_private_key" {
  count            = local.create_tls_peer_private_key ? 1 : 0
  model_uuid       = var.mongodb.model_uuid
  name             = "${var.mongodb.app_name}-tls-peer-private-key"
  value_wo         = { private-key = var.tls_peer_private_key }
  info             = "TLS peer private key for ${var.mongodb.app_name}"
  value_wo_version = var.tls_peer_private_key_version
}

resource "juju_access_secret" "tls_peer_private_key" {
  depends_on   = [module.mongodb]
  count        = local.use_tls_peer_private_key ? 1 : 0
  model_uuid   = var.mongodb.model_uuid
  applications = [module.mongodb.application.name]
  secret_id    = regex("[^/:]+$", local.tls_peer_private_key_secret_uri)
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

resource "terraform_data" "validate_encryption_at_rest" {
  input = {
    enable_encryption_at_rest = local.encryption_at_rest_configured
    vault_kv_integration      = var.vault_kv_integration
  }

  lifecycle {
    precondition {
      condition     = local.encryption_at_rest_configured == (var.vault_kv_integration != null)
      error_message = "Encryption at rest must be configured together: set both mongodb.config[\"enable-encryption-at-rest\"] and vault_kv_integration, or neither."
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
  count  = local.gcs_integrator_enabled ? 1 : 0
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
  count  = local.s3_integrator_enabled ? 1 : 0
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

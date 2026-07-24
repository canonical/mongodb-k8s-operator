# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 1. DEPLOYMENTS
#--------------------------------------------------------

# Replica set MongoDB app
module "mongodb" {
  source = "../../charms/mongodb"

  depends_on = [juju_secret.tls_client_private_key, juju_secret.tls_peer_private_key]

  app_name = var.mongodb.app_name
  base     = var.mongodb.base
  channel  = var.mongodb.channel
  config = merge(
    var.mongodb.config,
    { "role" : "replication" },
    length(juju_secret.tls_client_private_key) > 0 ? {
      "tls-client-private-key" = juju_secret.tls_client_private_key[0].secret_uri
    } : {},
    length(juju_secret.tls_peer_private_key) > 0 ? {
      "tls-peer-private-key" = juju_secret.tls_peer_private_key[0].secret_uri
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
  count      = var.tls_client_private_key != null ? 1 : 0
  model_uuid = var.mongodb.model_uuid
  name       = "${var.mongodb.app_name}-tls-client-private-key"
  value = {
    private-key = var.tls_client_private_key
  }
  info = "TLS client private key for ${var.mongodb.app_name}"
}

resource "juju_access_secret" "tls_client_private_key" {
  depends_on = [juju_secret.tls_client_private_key, module.mongodb]
  count      = length(juju_secret.tls_client_private_key) > 0 ? 1 : 0
  model_uuid = var.mongodb.model_uuid
  applications = [
    module.mongodb.application.name
  ]
  secret_id = juju_secret.tls_client_private_key[0].secret_id
}

resource "juju_secret" "tls_peer_private_key" {
  count      = var.tls_peer_private_key != null ? 1 : 0
  model_uuid = var.mongodb.model_uuid
  name       = "${var.mongodb.app_name}-tls-peer-private-key"
  value = {
    private-key = var.tls_peer_private_key
  }
  info = "TLS peer private key for ${var.mongodb.app_name}"
}

resource "juju_access_secret" "tls_peer_private_key" {
  depends_on = [juju_secret.tls_peer_private_key, module.mongodb]
  count      = length(juju_secret.tls_peer_private_key) > 0 ? 1 : 0
  model_uuid = var.mongodb.model_uuid
  applications = [
    module.mongodb.application.name
  ]
  secret_id = juju_secret.tls_peer_private_key[0].secret_id
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
  depends_on = [juju_secret.gcs_secret]
  count      = local.gcs_integrator_enabled ? 1 : 0
  source     = "git::https://github.com/canonical/object-storage-integrator.git//gcs/terraform/charm/gcs_integrator?ref=main"

  app_name = "gcs-integrator"
  base     = var.backups_integrator.base
  channel  = local.backups_integrator_channel
  config = merge(
    var.backups_integrator.config,
    length(juju_secret.gcs_secret) > 0 ? {
      credentials = juju_secret.gcs_secret[0].secret_uri
    } : {}
  )
  constraints = var.backups_integrator.constraints
  machines    = var.backups_integrator.machines
  model_uuid  = var.backups_integrator.model_uuid
  revision    = var.backups_integrator.revision
  units       = 1
}

resource "juju_secret" "gcs_secret" {
  count      = local.gcs_integrator_enabled && var.gcs_secret_key != null ? 1 : 0
  model_uuid = var.backups_integrator.model_uuid
  name       = "gcs-integrator-credentials"
  value = {
    secret-key = var.gcs_secret_key
  }
  info = "GCS credentials for gcs-integrator"
}

resource "juju_access_secret" "gcs_secret_access" {
  depends_on = [juju_secret.gcs_secret, module.gcs_integrator]
  count      = length(juju_secret.gcs_secret) > 0 ? 1 : 0
  model_uuid = var.backups_integrator.model_uuid
  applications = [
    module.gcs_integrator[0].application.name
  ]
  secret_id = juju_secret.gcs_secret[0].secret_id
}

resource "juju_secret" "s3_secret" {
  count      = local.s3_integrator_enabled && var.s3_access_key != null && var.s3_secret_key != null ? 1 : 0
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
  count      = local.s3_integrator_enabled ? 1 : 0
  source     = "git::https://github.com/canonical/object-storage-integrator.git//s3/terraform/charm/s3_integrator?ref=main"

  app_name = "s3-integrator"
  base     = var.backups_integrator.base
  channel  = local.backups_integrator_channel
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

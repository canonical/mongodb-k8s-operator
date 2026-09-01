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
  )
  constraints        = var.mongodb.constraints
  expose             = var.mongodb.expose
  model_uuid         = var.mongodb.model_uuid
  revision           = var.mongodb.revision
  storage_directives = var.mongodb.storage_directives
  units              = var.mongodb.units
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

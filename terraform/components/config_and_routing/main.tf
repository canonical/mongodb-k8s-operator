# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 1. DEPLOYMENTS
#--------------------------------------------------------

module "config_server" {
  source = "../../charms/mongodb"

  app_name           = var.config_server.app_name
  base               = var.config_server.base
  channel            = var.config_server.channel
  config             = merge(var.config_server.config, { "role" : "config-server" })
  constraints        = var.config_server.constraints
  expose             = var.config_server.expose
  model_uuid         = var.config_server.model_uuid
  revision           = var.config_server.revision
  storage_directives = var.config_server.storage_directives
  units              = var.config_server.units
}

module "mongos" {
  source = "git::https://github.com/canonical/mongos-k8s-operator//terraform?ref=8-transition/edge"

  app_name    = var.mongos.app_name
  base        = var.mongos.base
  channel     = var.mongos.channel
  config      = var.mongos.config
  constraints = var.mongos.constraints
  model_uuid  = var.mongos.model_uuid
  revision    = var.mongos.revision
  units       = var.mongos.units
}

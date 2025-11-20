# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  shards = [
    for app in concat(var.shards != null ? var.shards : []) : app if app != null
  ]

  shards_in_config_server_model = [
    for shard in local.shards :
    shard if shard != null && shard.model_uuid == var.config_server.model_uuid
  ]

  shards_not_in_config_server_model = [
    for shard in local.shards :
    shard if shard != null && shard.model_uuid != var.config_server.model_uuid
  ]
}

#--------------------------------------------------------
# 1. DEPLOYMENTS
#--------------------------------------------------------

# config server mongodb app
module "mongodb_config_server" {
  source = "../replica_set"

  channel  = var.config_server.channel
  revision = var.config_server.revision
  base     = var.config_server.base

  app_name          = var.config_server.app_name
  units             = var.config_server.units
  machines          = var.config_server.machines
  config            = merge(var.config_server.config, { "role" : "config-server" })
  model_uuid        = var.config_server.model_uuid
  constraints       = var.config_server.constraints
  storage           = var.config_server.storage
  endpoint_bindings = var.config_server.endpoint_bindings
}

# shard apps
module "mongodb_shards" {
  for_each = { for idx, app in local.shards : idx => app if app != null }
  source   = "../replica_set"

  channel  = each.value.channel
  revision = each.value.revision
  base     = each.value.base

  app_name    = each.value.app_name
  units       = each.value.units
  machines    = each.value.machines
  config      = merge(each.value.config, { "role" : "shard" })
  model_uuid  = each.value.model_uuid
  constraints = each.value.constraints
  storage     = each.value.storage
}

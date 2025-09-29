# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  enable_tls   = var.self_signed_certificates != null
  mongodb_apps = concat([var.config_server], var.shards != null ? var.shards : [])
  mongo_apps   = concat(local.mongodb_apps, [merge({}, var.mongos_k8s)])

  tls_same_model_mongo_apps = [
    for app in local.mongo_apps :
    app if local.enable_tls && app.model == var.self_signed_certificates.model
  ]
  tls_cross_model_mongo_apps = [
    for app in local.mongo_apps :
    app if local.enable_tls && app.model != var.self_signed_certificates.model
  ]
}

#--------------------------------------------------------
# 1. DEPLOYMENTS
#--------------------------------------------------------

# replicaset mongodb-k8s app
module "mongodb_k8s" {
  source = "../../charm/sharded"

  config_server = var.config_server
  shards        = var.shards
}

# self-signed-certificates app
resource "juju_application" "self-signed-certificates" {
  for_each = local.enable_tls ? { "deployed" = true } : {}

  charm {
    name     = "self-signed-certificates"
    channel  = var.self_signed_certificates.channel
    revision = var.self_signed_certificates.revision
    base     = var.self_signed_certificates.base
  }

  name              = var.self_signed_certificates.app_name
  units             = (var.self_signed_certificates.machines == null || length(var.self_signed_certificates.machines) == 0) ? var.self_signed_certificates.units : null
  machines          = (var.self_signed_certificates.machines == null || length(var.self_signed_certificates.machines) == 0) ? null : var.self_signed_certificates.machines
  config            = var.self_signed_certificates.config
  model             = var.self_signed_certificates.model
  constraints       = var.self_signed_certificates.constraints
  endpoint_bindings = var.self_signed_certificates.endpoint_bindings
}

# mongos
resource "juju_application" "mongos_k8s" {
  charm {
    name     = "mongos-k8s"
    channel  = var.mongos_k8s.channel
    revision = var.mongos_k8s.revision
    base     = var.mongos_k8s.base
  }

  name   = var.mongos_k8s.app_name
  config = var.mongos_k8s.config
  model  = var.data_integrator.model
}

# Integrator apps
resource "juju_application" "data_integrator" {
  charm {
    name     = "data-integrator"
    channel  = var.data_integrator.channel
    revision = var.data_integrator.revision
    base     = var.data_integrator.base
  }

  name              = var.data_integrator.app_name
  units             = (var.data_integrator.machines == null || length(var.data_integrator.machines) == 0) ? var.data_integrator.units : null
  machines          = (var.data_integrator.machines == null || length(var.data_integrator.machines) == 0) ? null : var.data_integrator.machines
  config            = var.data_integrator.config
  model             = var.data_integrator.model
  constraints       = var.data_integrator.constraints
  endpoint_bindings = var.data_integrator.endpoint_bindings
}

resource "juju_application" "s3_integrator" {
  charm {
    name     = "s3-integrator"
    channel  = var.s3_integrator.channel
    revision = var.s3_integrator.revision
    base     = var.s3_integrator.base
  }

  name              = var.s3_integrator.app_name
  units             = (var.s3_integrator.machines == null || length(var.s3_integrator.machines) == 0) ? var.s3_integrator.units : null
  machines          = (var.s3_integrator.machines == null || length(var.s3_integrator.machines) == 0) ? null : var.s3_integrator.machines
  config            = var.s3_integrator.config
  model             = var.s3_integrator.model
  constraints       = var.s3_integrator.constraints
  endpoint_bindings = var.s3_integrator.endpoint_bindings
}

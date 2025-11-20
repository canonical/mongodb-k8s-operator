# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  enable_tls = var.self_signed_certificates != null
}

#--------------------------------------------------------
# 1. DEPLOYMENTS
#--------------------------------------------------------

# replicaset mongodb app
module "mongodb_k8s" {
  source = "../../charm/replica_set"

  channel  = var.mongodb_k8s.channel
  revision = var.mongodb_k8s.revision
  base     = var.mongodb_k8s.base

  app_name          = var.mongodb_k8s.app_name
  units             = var.mongodb_k8s.units
  machines          = var.mongodb_k8s.machines
  config            = merge(var.mongodb_k8s.config, { "role" : "replication" })
  model_uuid        = var.mongodb_k8s.model_uuid
  constraints       = var.mongodb_k8s.constraints
  storage           = var.mongodb_k8s.storage
  endpoint_bindings = var.mongodb_k8s.endpoint_bindings
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
  constraints       = var.self_signed_certificates.constraints
  endpoint_bindings = var.self_signed_certificates.endpoint_bindings
  model_uuid        = var.self_signed_certificates.model_uuid
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
  constraints       = var.data_integrator.constraints
  endpoint_bindings = var.data_integrator.endpoint_bindings
  model_uuid        = var.data_integrator.model
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
  constraints       = var.s3_integrator.constraints
  endpoint_bindings = var.s3_integrator.endpoint_bindings
  model_uuid        = var.s3_integrator.model
}

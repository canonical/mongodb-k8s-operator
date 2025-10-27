# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

terraform {
  required_providers {
    juju = {
      source  = "juju/juju"
      version = "~> 0.23.1"
    }
  }
}

resource "juju_application" "mongodb_k8s" {
  charm {
    name     = "mongodb-k8s"
    channel  = var.channel
    revision = var.revision
    base     = var.base
  }
  config             = var.config
  model              = var.model
  name               = var.app_name
  units              = (var.machines == null || length(var.machines) == 0) ? var.units : null
  machines           = (var.machines == null || length(var.machines) == 0) ? null : var.machines
  constraints        = var.constraints
  storage_directives = var.storage
  endpoint_bindings  = var.endpoint_bindings
  trust              = true

}

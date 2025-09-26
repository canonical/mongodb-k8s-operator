# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 2. Offers
#--------------------------------------------------------

resource "juju_offer" "config_server_mongos_offer" {
  for_each = var.config_server.model != var.mongos_k8s.model ? { "offered" = true } : {}

  model            = var.config_server.model
  application_name = var.config_server.app_name
  endpoints        = ["cluster"]
  depends_on       = [module.mongodb-k8s]
}

resource "juju_offer" "tls_provider_offer" {
  for_each = local.enable_tls && length(local.tls_cross_model_mongo_apps) > 0 ? { "offered" = true } : {}

  model            = var.self_signed_certificates.model
  application_name = var.self_signed_certificates.app_name
  endpoints        = ["certificates"]
  depends_on       = [juju_application.self-signed-certificates["deployed"]]
}

resource "juju_offer" "s3_integrator_offer" {
  for_each = var.s3_integrator.model != var.config_server.model ? { "offered" = true } : {}

  model            = var.s3_integrator.model
  application_name = var.s3_integrator.app_name
  endpoints        = ["s3-credentials"]
  depends_on       = [juju_application.s3_integrator]
}

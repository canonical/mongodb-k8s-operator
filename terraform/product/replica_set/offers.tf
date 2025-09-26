# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 2. Offers
#--------------------------------------------------------

resource "juju_offer" "mongodb_client_offer" {
  for_each = var.data_integrator.model != var.mongodb-k8s.model ? { "offered" = true } : {}

  model            = var.data_integrator.model
  application_name = var.data_integrator.app_name
  endpoints        = ["database"]
  depends_on       = [module.mongodb-k8s]
}

resource "juju_offer" "tls_provider_offer" {
  for_each = local.enable_tls && var.self_signed_certificates.model != var.mongodb-k8s.model ? { "offered" = true } : {}

  model            = var.self_signed_certificates.model
  application_name = var.self_signed_certificates.app_name
  endpoints        = ["certificates"]
  depends_on       = [juju_application.self-signed-certificates["deployed"]]
}

resource "juju_offer" "s3_integrator_offer" {
  for_each = var.s3_integrator.model != var.mongodb-k8s.model ? { "offered" = true } : {}

  model            = var.s3_integrator.model
  application_name = var.s3_integrator.app_name
  endpoints        = ["s3-credentials"]
  depends_on       = [juju_application.s3_integrator]
}

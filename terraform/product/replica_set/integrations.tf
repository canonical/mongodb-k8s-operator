# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 3. Integrations
#--------------------------------------------------------

## Same model integrations

resource "juju_integration" "mongodb_tls_same_model_integration" {
  for_each = local.enable_tls && var.self_signed_certificates.model_uuid == var.mongodb_k8s.model_uuid ? { "integrated" = true } : {}

  application {
    name     = var.mongodb_k8s.app_name
    endpoint = "certificates"
  }
  application {
    name = var.self_signed_certificates.app_name
  }
  depends_on = [
    module.mongodb_k8s,
    juju_application.self-signed-certificates["deployed"],
  ]
  model_uuid = var.mongodb_k8s.model_uuid
}

resource "juju_integration" "mongodb_s3_same_model_integration" {
  for_each = var.s3_integrator.model_uuid == var.mongodb_k8s.model_uuid ? { "integrated" = true } : {}

  application {
    name = var.mongodb_k8s.app_name
  }
  application {
    name = var.s3_integrator.app_name
  }
  depends_on = [
    module.mongodb_k8s,
    juju_application.s3_integrator,
  ]
  model_uuid = var.mongodb_k8s.model_uuid
}

resource "juju_integration" "mongodb_data_same_model_integration" {
  for_each = var.data_integrator.model_uuid == var.mongodb_k8s.model_uuid ? { "integrated" = true } : {}

  application {
    name = var.mongodb_k8s.app_name
  }
  application {
    name = var.data_integrator.app_name
  }
  depends_on = [
    module.mongodb_k8s,
    juju_application.data_integrator,
  ]
  model_uuid = var.mongodb_k8s.model_uuid
}

## Cross model integrations
resource "juju_integration" "mongodb_data_cross_model_integration" {
  for_each = var.data_integrator.model_uuid != var.mongodb_k8s.model_uuid ? { "integrated" = true } : {}

  application {
    offer_url = juju_offer.mongodb_client_offer["offered"].url
  }
  application {
    name     = var.data_integrator.app_name
    endpoint = "mongodb"
  }
  depends_on = [
    juju_application.data_integrator,
    juju_offer.mongodb_client_offer,
  ]
  model_uuid = var.data_integrator.model_uuid
}

resource "juju_integration" "mongodb_tls_cross_model_integration" {
  for_each = local.enable_tls && var.self_signed_certificates.model_uuid != var.mongodb_k8s.model_uuid ? { "integrated" = true } : {}

  application {
    offer_url = juju_offer.tls_provider_offer["offered"].url
  }
  application {
    name     = var.mongodb_k8s.app_name
    endpoint = "certificates"
  }
  depends_on = [
    module.mongodb_k8s,
    juju_offer.tls_provider_offer,
  ]
  model_uuid = var.mongodb_k8s.model_uuid
}

resource "juju_integration" "mongodb_s3_cross_model_integration" {
  for_each = var.s3_integrator.model_uuid != var.mongodb_k8s.model_uuid ? { "integrated" = true } : {}

  application {
    offer_url = juju_offer.s3_integrator_offer["offered"].url
  }
  application {
    name     = var.mongodb_k8s.app_name
    endpoint = "s3-credentials"
  }
  depends_on = [
    module.mongodb_k8s,
    juju_offer.s3_integrator_offer,
  ]
  model_uuid = var.mongodb_k8s.model_uuid
}

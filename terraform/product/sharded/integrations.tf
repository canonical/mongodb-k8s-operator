# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 3. Integrations
#--------------------------------------------------------

## Same model integrations

resource "juju_integration" "mongos_data_integrator_same_model_integration" {
  application {
    name = var.data_integrator.app_name
  }
  application {
    name = var.mongos_k8s.app_name
  }
  depends_on = [
    module.mongodb_k8s,
    juju_application.data_integrator,
  ]
  model_uuid = var.data_integrator.model_uuid
}

resource "juju_integration" "tls_peer_mongo_same_model_integration" {
  count = length(local.tls_same_model_mongo_apps)

  model_uuid = local.tls_same_model_mongo_apps[count.index].model_uuid
  application {
    name     = local.tls_same_model_mongo_apps[count.index].app_name
    endpoint = "peer-certificates"
  }
  application {
    name = var.self_signed_certificates.app_name
  }
  depends_on = [
    module.mongodb_k8s,
    juju_application.self-signed-certificates["deployed"],
  ]
}

resource "juju_integration" "tls_client_mongo_same_model_integration" {
  count = length(local.tls_same_model_mongo_apps)

  model_uuid = local.tls_same_model_mongo_apps[count.index].model_uuid
  application {
    name     = local.tls_same_model_mongo_apps[count.index].app_name
    endpoint = "client-certificates"
  }
  application {
    name = var.self_signed_certificates.app_name
  }
  depends_on = [
    module.mongodb_k8s,
    juju_application.self-signed-certificates["deployed"],
  ]
}

resource "juju_integration" "s3_config_server_same_model_integration" {
  for_each = var.s3_integrator.model_uuid == var.config_server.model_uuid ? { "integrated" = true } : {}

  application {
    name = var.config_server.app_name
  }
  application {
    name = var.s3_integrator.app_name
  }
  depends_on = [
    module.mongodb_k8s,
    juju_application.s3_integrator,
  ]
  model_uuid = var.config_server.model_uuid
}

#--------------------------------------------------------
## Cross model integrations

resource "juju_integration" "tls_peer_mongo_cross_model_integration" {
  count = length(local.tls_cross_model_mongo_apps)

  model_uuid = local.tls_cross_model_mongo_apps[count.index].model_uuid

  application {
    offer_url = juju_offer.tls_provider_offer["offered"].url
  }
  application {
    name     = local.tls_cross_model_mongo_apps[count.index].app_name
    endpoint = "peer-certificates"
  }
  depends_on = [
    module.mongodb_k8s,
    juju_offer.tls_provider_offer,
  ]
}

resource "juju_integration" "tls_client_mongo_cross_model_integration" {
  count = length(local.tls_cross_model_mongo_apps)

  model_uuid = local.tls_cross_model_mongo_apps[count.index].model_uuid

  application {
    offer_url = juju_offer.tls_provider_offer["offered"].url
  }
  application {
    name     = local.tls_cross_model_mongo_apps[count.index].app_name
    endpoint = "client-certificates"
  }
  depends_on = [
    module.mongodb_k8s,
    juju_offer.tls_provider_offer,
  ]
}

resource "juju_integration" "s3_config_server_cross_model_integration" {
  for_each = var.s3_integrator.model_uuid != var.config_server.model_uuid ? { "integrated" = true } : {}

  application {
    offer_url = juju_offer.s3_integrator_offer["offered"].url
  }
  application {
    name     = var.config_server.app_name
    endpoint = "s3-credentials"
  }
  depends_on = [
    module.mongodb_k8s,
    juju_offer.s3_integrator_offer,
  ]
  model_uuid = var.config_server.model_uuid
}

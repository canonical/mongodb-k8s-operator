# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 3. Integrations
#--------------------------------------------------------

## Same model integrations

resource "juju_integration" "mongos_data_integrator_same_model_integration" {
  model = var.data_integrator.model
  application {
    name = var.data_integrator.app_name
  }
  application {
    name = var.mongos_k8s.app_name
  }
  depends_on = [
    juju_application.mongos_k8s,
    juju_application.data_integrator,
  ]
}

resource "juju_integration" "config_server_mongos_same_model_integration" {
  model = var.mongos_k8s.model
  application {
    name = var.config_server.app_name
  }
  application {
    name = var.mongos_k8s.app_name
  }
  depends_on = [
    module.mongodb-k8s,
    juju_integration.mongos_data_integrator_same_model_integration,
  ]
}

resource "juju_integration" "tls_mongo_same_model_integration" {
  count = length(local.tls_same_model_mongo_apps)

  model = local.tls_same_model_mongo_apps[count.index].model
  application {
    name     = local.tls_same_model_mongo_apps[count.index].app_name
    endpoint = "certificates"
  }
  application {
    name = var.self_signed_certificates.app_name
  }
  depends_on = [
    module.mongodb-k8s,
    juju_application.self-signed-certificates["deployed"],
  ]
}

resource "juju_integration" "s3_config_server_same_model_integration" {
  for_each = var.s3_integrator.model == var.config_server.model ? { "integrated" = true } : {}

  model = var.config_server.model
  application {
    name = var.config_server.app_name
  }
  application {
    name = var.s3_integrator.app_name
  }
  depends_on = [
    module.mongodb-k8s,
    juju_application.s3_integrator,
  ]
}

#--------------------------------------------------------
## Cross model integrations

resource "juju_integration" "config_server_mongos_cross_model_integration" {
  for_each = var.mongos_k8s.model != var.config_server.model ? { "integrated" = true } : {}
  model    = var.mongos_k8s.model

  application {
    offer_url = juju_offer.config_server_mongos_offer["offered"].url
  }
  application {
    name     = var.mongos_k8s.app_name
    endpoint = "cluster"
  }
  depends_on = [
    juju_application.mongos_k8s,
    juju_offer.config_server_mongos_offer,
  ]
}

resource "juju_integration" "tls_mongo_cross_model_integration" {
  count = length(local.tls_cross_model_mongo_apps)

  model = local.tls_cross_model_mongo_apps[count.index].model

  application {
    offer_url = juju_offer.tls_provider_offer["offered"].url
  }
  application {
    name     = local.tls_cross_model_mongo_apps[count.index].app_name
    endpoint = "certificates"
  }
  depends_on = [
    module.mongodb-k8s,
    juju_offer.tls_provider_offer,
  ]
}

resource "juju_integration" "s3_config_server_cross_model_integration" {
  for_each = var.s3_integrator.model != var.config_server.model ? { "integrated" = true } : {}
  model    = var.config_server.model

  application {
    offer_url = juju_offer.s3_integrator_offer["offered"].url
  }
  application {
    name     = var.config_server.app_name
    endpoint = "s3-credentials"
  }
  depends_on = [
    module.mongodb-k8s,
    juju_offer.s3_integrator_offer,
  ]
}

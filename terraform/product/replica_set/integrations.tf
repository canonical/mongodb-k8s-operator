# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 3. Integrations
#--------------------------------------------------------

resource "juju_integration" "client_certificates" {
  count      = local.client_certificates_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.client_certificates_integration.kind == "endpoint" ? var.client_certificates_integration.name : null
    endpoint  = var.client_certificates_integration.kind == "endpoint" ? var.client_certificates_integration.endpoint : null
    offer_url = var.client_certificates_integration.kind == "offer" ? var.client_certificates_integration.url : null
  }

  application {
    name     = module.mongodb.requires["client_certificates"].name
    endpoint = module.mongodb.requires["client_certificates"].endpoint
  }

  depends_on = [module.mongodb]
}

resource "juju_integration" "grafana_dashboard" {
  count      = local.grafana_dashboard_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.grafana_dashboard_integration.kind == "endpoint" ? var.grafana_dashboard_integration.name : null
    endpoint  = var.grafana_dashboard_integration.kind == "endpoint" ? var.grafana_dashboard_integration.endpoint : null
    offer_url = var.grafana_dashboard_integration.kind == "offer" ? var.grafana_dashboard_integration.url : null
  }

  application {
    name     = module.mongodb.provides["grafana_dashboard"].name
    endpoint = module.mongodb.provides["grafana_dashboard"].endpoint
  }

  depends_on = [module.mongodb]
}

resource "juju_integration" "metrics_endpoint" {
  count      = local.metrics_endpoint_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.metrics_endpoint_integration.kind == "endpoint" ? var.metrics_endpoint_integration.name : null
    endpoint  = var.metrics_endpoint_integration.kind == "endpoint" ? var.metrics_endpoint_integration.endpoint : null
    offer_url = var.metrics_endpoint_integration.kind == "offer" ? var.metrics_endpoint_integration.url : null
  }

  application {
    name     = module.mongodb.provides["metrics_endpoint"].name
    endpoint = module.mongodb.provides["metrics_endpoint"].endpoint
  }

  depends_on = [module.mongodb]
}

resource "juju_integration" "logging" {
  count      = local.logging_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.logging_integration.kind == "endpoint" ? var.logging_integration.name : null
    endpoint  = var.logging_integration.kind == "endpoint" ? var.logging_integration.endpoint : null
    offer_url = var.logging_integration.kind == "offer" ? var.logging_integration.url : null
  }

  application {
    name     = module.mongodb.requires["logging"].name
    endpoint = module.mongodb.requires["logging"].endpoint
  }

  depends_on = [module.mongodb]
}

resource "juju_integration" "ldap" {
  count      = local.ldap_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.ldap_integration.kind == "endpoint" ? var.ldap_integration.name : null
    endpoint  = var.ldap_integration.kind == "endpoint" ? var.ldap_integration.endpoint : null
    offer_url = var.ldap_integration.kind == "offer" ? var.ldap_integration.url : null
  }

  application {
    name     = module.mongodb.requires["ldap"].name
    endpoint = module.mongodb.requires["ldap"].endpoint
  }

  depends_on = [module.mongodb]
}

resource "juju_integration" "ldap_certificate_transfer" {
  count      = local.ldap_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.ldap_certificate_transfer_integration.kind == "endpoint" ? var.ldap_certificate_transfer_integration.name : null
    endpoint  = var.ldap_certificate_transfer_integration.kind == "endpoint" ? var.ldap_certificate_transfer_integration.endpoint : null
    offer_url = var.ldap_certificate_transfer_integration.kind == "offer" ? var.ldap_certificate_transfer_integration.url : null
  }

  application {
    name     = module.mongodb.requires["ldap_certificate_transfer"].name
    endpoint = module.mongodb.requires["ldap_certificate_transfer"].endpoint
  }

  depends_on = [module.mongodb]
}

resource "juju_integration" "peer_certificates" {
  count      = local.peer_certificates_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.peer_certificates_integration.kind == "endpoint" ? var.peer_certificates_integration.name : null
    endpoint  = var.peer_certificates_integration.kind == "endpoint" ? var.peer_certificates_integration.endpoint : null
    offer_url = var.peer_certificates_integration.kind == "offer" ? var.peer_certificates_integration.url : null
  }

  application {
    name     = module.mongodb.requires["peer_certificates"].name
    endpoint = module.mongodb.requires["peer_certificates"].endpoint
  }

  depends_on = [module.mongodb]
}

resource "juju_integration" "vault_kv" {
  count      = local.encryption_at_rest_enabled ? 1 : 0
  model_uuid = module.mongodb.application.model_uuid

  application {
    name      = var.vault_kv_integration.kind == "endpoint" ? var.vault_kv_integration.name : null
    endpoint  = var.vault_kv_integration.kind == "endpoint" ? var.vault_kv_integration.endpoint : null
    offer_url = var.vault_kv_integration.kind == "offer" ? var.vault_kv_integration.url : null
  }

  application {
    name     = module.mongodb.requires["vault_kv"].name
    endpoint = module.mongodb.requires["vault_kv"].endpoint
  }

  depends_on = [module.mongodb]
}

# Integrator relations

resource "juju_integration" "mongodb_data" {
  model_uuid = var.data_integrator.model_uuid

  application {
    name      = var.data_integrator.model_uuid == module.mongodb.application.model_uuid ? module.mongodb.provides["database"].name : null
    endpoint  = var.data_integrator.model_uuid == module.mongodb.application.model_uuid ? module.mongodb.provides["database"].endpoint : null
    offer_url = try(juju_offer.mongodb_client["offered"].url, null)
  }
  application {
    name     = module.data_integrator.application.name
    endpoint = "mongodb"
  }
  depends_on = [
    module.mongodb,
    module.data_integrator,
  ]
}

resource "juju_integration" "mongodb_s3" {
  for_each = local.s3_integrator_enabled ? { "integrated" = true } : {}

  model_uuid = module.mongodb.application.model_uuid

  application {
    name     = module.mongodb.requires["s3_credentials"].name
    endpoint = module.mongodb.requires["s3_credentials"].endpoint
  }
  application {
    name      = var.backups_integrator.model_uuid == module.mongodb.application.model_uuid ? module.s3_integrator[0].provides.s3_credentials.name : null
    endpoint  = var.backups_integrator.model_uuid == module.mongodb.application.model_uuid ? module.s3_integrator[0].provides.s3_credentials.endpoint : null
    offer_url = var.backups_integrator.model_uuid != module.mongodb.application.model_uuid ? module.s3_integrator[0].offers.s3_credentials.url : null
  }
  depends_on = [
    module.mongodb,
    module.s3_integrator,
  ]
}

resource "juju_integration" "mongodb_gcs" {
  for_each = local.gcs_integrator_enabled ? { "integrated" = true } : {}

  model_uuid = module.mongodb.application.model_uuid

  application {
    name     = module.mongodb.requires["gcs_credentials"].name
    endpoint = module.mongodb.requires["gcs_credentials"].endpoint
  }
  application {
    name      = var.backups_integrator.model_uuid == module.mongodb.application.model_uuid ? module.gcs_integrator[0].provides.gcs_credentials.name : null
    endpoint  = var.backups_integrator.model_uuid == module.mongodb.application.model_uuid ? module.gcs_integrator[0].provides.gcs_credentials.endpoint : null
    offer_url = var.backups_integrator.model_uuid != module.mongodb.application.model_uuid ? module.gcs_integrator[0].offers.gcs_credentials.url : null
  }
  depends_on = [
    module.mongodb,
    module.gcs_integrator,
  ]
}

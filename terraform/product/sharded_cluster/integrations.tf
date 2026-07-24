# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 3. Integrations
#--------------------------------------------------------

# Shards
resource "juju_integration" "config_server_shards" {
  for_each   = tomap({ for shard_key, shard in local.shards : shard_key => shard })
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == module.config_and_routing.components["config_server"].model_uuid ? module.config_and_routing.provides["config_server"].name : null
    endpoint  = each.value.model_uuid == module.config_and_routing.components["config_server"].model_uuid ? module.config_and_routing.provides["config_server"].endpoint : null
    offer_url = each.value.model_uuid != module.config_and_routing.components["config_server"].model_uuid ? module.config_and_routing.offers["config_server"].url : null
  }

  application {
    name     = module.shards[each.key].requires["sharding"].name
    endpoint = module.shards[each.key].requires["sharding"].endpoint
  }

  depends_on = [
    module.config_and_routing,
    module.shards,
  ]
}

# Integrators
resource "juju_integration" "gcs_credentials" {
  for_each   = local.gcs_credentials_enabled ? { "integrated" = true } : {}
  model_uuid = module.config_and_routing.components["config_server"].model_uuid

  application {
    name     = module.config_and_routing.requires["config_server_gcs_credentials"].name
    endpoint = module.config_and_routing.requires["config_server_gcs_credentials"].endpoint
  }

  application {
    name      = var.backups_integrator.model_uuid == module.config_and_routing.components["config_server"].model_uuid ? module.gcs_integrator[0].provides.gcs_credentials.name : null
    endpoint  = var.backups_integrator.model_uuid == module.config_and_routing.components["config_server"].model_uuid ? module.gcs_integrator[0].provides.gcs_credentials.endpoint : null
    offer_url = var.backups_integrator.model_uuid != module.config_and_routing.components["config_server"].model_uuid ? module.gcs_integrator[0].offers.gcs_credentials.url : null
  }

  depends_on = [
    module.config_and_routing,
    module.gcs_integrator,
  ]
}

resource "juju_integration" "mongos_client" {
  model_uuid = module.config_and_routing.components["mongos"].model_uuid

  application {
    name     = module.config_and_routing.provides["mongos_proxy"].name
    endpoint = module.config_and_routing.provides["mongos_proxy"].endpoint
  }

  application {
    name     = module.data_integrator.application.name
    endpoint = "mongos"
  }

  depends_on = [
    module.config_and_routing,
    module.data_integrator,
  ]
}

resource "juju_integration" "s3_credentials" {
  for_each   = local.s3_credentials_enabled ? { "integrated" = true } : {}
  model_uuid = module.config_and_routing.components["config_server"].model_uuid

  application {
    name     = module.config_and_routing.requires["config_server_s3_credentials"].name
    endpoint = module.config_and_routing.requires["config_server_s3_credentials"].endpoint
  }

  application {
    name      = var.backups_integrator.model_uuid == module.config_and_routing.components["config_server"].model_uuid ? module.s3_integrator[0].provides.s3_credentials.name : null
    endpoint  = var.backups_integrator.model_uuid == module.config_and_routing.components["config_server"].model_uuid ? module.s3_integrator[0].provides.s3_credentials.endpoint : null
    offer_url = var.backups_integrator.model_uuid != module.config_and_routing.components["config_server"].model_uuid ? module.s3_integrator[0].offers.s3_credentials.url : null
  }

  depends_on = [
    module.config_and_routing,
    module.s3_integrator,
  ]
}

# Other apps
resource "juju_integration" "grafana_dashboard" {
  for_each   = { for app in local.grafana_dashboard_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.grafana_dashboard_integration.model_uuid ? var.grafana_dashboard_integration.name : null
    endpoint  = each.value.model_uuid == var.grafana_dashboard_integration.model_uuid ? var.grafana_dashboard_integration.endpoint : null
    offer_url = each.value.model_uuid != var.grafana_dashboard_integration.model_uuid ? var.grafana_dashboard_integration.url : null
  }

  application {
    name     = local.grafana_dashboard_provides[each.key].name
    endpoint = local.grafana_dashboard_provides[each.key].endpoint
  }

  depends_on = [module.config_and_routing, module.shards]
}

resource "juju_integration" "metrics_endpoint" {
  for_each   = { for app in local.metrics_endpoint_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.metrics_endpoint_integration.model_uuid ? var.metrics_endpoint_integration.name : null
    endpoint  = each.value.model_uuid == var.metrics_endpoint_integration.model_uuid ? var.metrics_endpoint_integration.endpoint : null
    offer_url = each.value.model_uuid != var.metrics_endpoint_integration.model_uuid ? var.metrics_endpoint_integration.url : null
  }

  application {
    name     = local.metrics_endpoint_provides[each.key].name
    endpoint = local.metrics_endpoint_provides[each.key].endpoint
  }

  depends_on = [module.config_and_routing, module.shards]
}

resource "juju_integration" "logging" {
  for_each   = { for app in local.logging_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.logging_integration.model_uuid ? var.logging_integration.name : null
    endpoint  = each.value.model_uuid == var.logging_integration.model_uuid ? var.logging_integration.endpoint : null
    offer_url = each.value.model_uuid != var.logging_integration.model_uuid ? var.logging_integration.url : null
  }

  application {
    name     = local.logging_requires[each.key].name
    endpoint = local.logging_requires[each.key].endpoint
  }

  depends_on = [module.config_and_routing, module.shards]
}

resource "juju_integration" "client_certificates" {
  for_each   = { for app in local.client_certificates_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.client_certificates_integration.model_uuid ? var.client_certificates_integration.name : null
    endpoint  = each.value.model_uuid == var.client_certificates_integration.model_uuid ? var.client_certificates_integration.endpoint : null
    offer_url = each.value.model_uuid != var.client_certificates_integration.model_uuid ? var.client_certificates_integration.url : null
  }

  application {
    name     = local.client_certificates_requires[each.value.app_name].name
    endpoint = local.client_certificates_requires[each.value.app_name].endpoint
  }

  depends_on = [module.config_and_routing]
}

resource "juju_integration" "ldap" {
  for_each   = { for app in local.ldap_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.ldap_integration.model_uuid ? var.ldap_integration.name : null
    endpoint  = each.value.model_uuid == var.ldap_integration.model_uuid ? var.ldap_integration.endpoint : null
    offer_url = each.value.model_uuid != var.ldap_integration.model_uuid ? var.ldap_integration.url : null
  }

  application {
    name     = local.ldap_requires[each.value.app_name].name
    endpoint = local.ldap_requires[each.value.app_name].endpoint
  }

  depends_on = [module.config_and_routing]
}

resource "juju_integration" "ldap_certificate_transfer" {
  for_each   = { for app in local.ldap_certificate_transfer_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.ldap_certificate_transfer_integration.model_uuid ? var.ldap_certificate_transfer_integration.name : null
    endpoint  = each.value.model_uuid == var.ldap_certificate_transfer_integration.model_uuid ? var.ldap_certificate_transfer_integration.endpoint : null
    offer_url = each.value.model_uuid != var.ldap_certificate_transfer_integration.model_uuid ? var.ldap_certificate_transfer_integration.url : null
  }

  application {
    name     = local.ldap_certificate_transfer_requires[each.value.app_name].name
    endpoint = local.ldap_certificate_transfer_requires[each.value.app_name].endpoint
  }

  depends_on = [module.config_and_routing]
}

resource "juju_integration" "peer_certificates" {
  for_each   = { for app in local.peer_certificates_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.peer_certificates_integration.model_uuid ? var.peer_certificates_integration.name : null
    endpoint  = each.value.model_uuid == var.peer_certificates_integration.model_uuid ? var.peer_certificates_integration.endpoint : null
    offer_url = each.value.model_uuid != var.peer_certificates_integration.model_uuid ? var.peer_certificates_integration.url : null
  }

  application {
    name     = local.peer_certificates_requires[each.value.app_name].name
    endpoint = local.peer_certificates_requires[each.value.app_name].endpoint
  }

  depends_on = [module.config_and_routing]
}

resource "juju_integration" "vault_kv" {
  for_each   = { for app in local.vault_kv_apps : app.app_name => app }
  model_uuid = each.value.model_uuid

  application {
    name      = each.value.model_uuid == var.vault_kv_integration.model_uuid ? var.vault_kv_integration.name : null
    endpoint  = each.value.model_uuid == var.vault_kv_integration.model_uuid ? var.vault_kv_integration.endpoint : null
    offer_url = each.value.model_uuid != var.vault_kv_integration.model_uuid ? var.vault_kv_integration.url : null
  }

  application {
    name     = local.vault_kv_requires[each.value.app_name].name
    endpoint = local.vault_kv_requires[each.value.app_name].endpoint
  }

  depends_on = [module.config_and_routing]
}

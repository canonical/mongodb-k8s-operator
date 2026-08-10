# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

output "components" {
  description = "All deployed applications."
  value = {
    config_server = module.config_server.application
    mongos        = module.mongos.application
  }
}

output "app_names" {
  description = "Names of of all deployed applications."
  value = {
    config_server = module.config_server.application.name
    mongos        = module.mongos.application.name
  }
}

output "offers" {
  description = "Map of all offer endpoints."
  value = {
    config_server = {
      kind = "offer"
      url  = juju_offer.config_server.url
    }
    config_server_cluster = {
      kind = "offer"
      url  = juju_offer.config_server_cluster.url
    }
    config_server_grafana_dashboard = module.config_server.offers["grafana_dashboard"]
    config_server_metrics_endpoint  = module.config_server.offers["metrics_endpoint"]
    mongos_proxy                    = module.mongos.offers["mongos_proxy"]
  }
}

output "provides" {
  description = "Map of all \"provides\" endpoints"
  value = {
    config_server                   = module.config_server.provides["config_server"]
    config_server_cluster           = module.config_server.provides["cluster"]
    config_server_grafana_dashboard = module.config_server.provides["grafana_dashboard"]
    config_server_metrics           = module.config_server.provides["metrics_endpoint"]
    mongos_proxy                    = module.mongos.provides["mongos_proxy"]
  }
}

output "requires" {
  description = "Map of all \"requires\" endpoints"
  value = {
    config_server_client_certificates       = module.config_server.requires["client_certificates"]
    config_server_gcs_credentials           = module.config_server.requires["gcs_credentials"]
    config_server_ldap                      = module.config_server.requires["ldap"]
    config_server_ldap_certificate_transfer = module.config_server.requires["ldap_certificate_transfer"]
    config_server_logging                   = module.config_server.requires["logging"]
    config_server_peer_certificates         = module.config_server.requires["peer_certificates"]
    config_server_s3_credentials            = module.config_server.requires["s3_credentials"]
    config_server_vault_kv                  = module.config_server.requires["vault_kv"]
    mongos_client_certificates              = module.mongos.requires["client_certificates"]
    mongos_cluster                          = module.mongos.requires["cluster"]
    mongos_ldap                             = module.mongos.requires["ldap"]
    mongos_ldap_certificate_transfer        = module.mongos.requires["ldap_certificate_transfer"]
    mongos_peer_certificates                = module.mongos.requires["peer_certificates"]
  }
}

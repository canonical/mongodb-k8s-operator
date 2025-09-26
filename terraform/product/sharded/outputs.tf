# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

output "app_names" {
  description = "Names of of all deployed applications."
  value = merge(
    module.mongodb-k8s.app_names,
    {
      "data_integrator" : juju_application.data_integrator.name
      "s3_integrator" : juju_application.s3_integrator.name
      "self_signed_certificates" : var.self_signed_certificates != null ? juju_application.self-signed-certificates["deployed"].name : null
      "mongos_k8s" : juju_application.mongos_k8s.name
    }
  )
}


# Provided integration endpoints
output "provides" {
  description = "Map of all \"provides\" endpoints"
  value = {
    database          = "database"
    config_server     = "config-server"
    cluster           = "cluster"
    grafana_dashboard = "grafana-dashboard"
    metrics_endpoint  = "metrics-endpoint"
  }
}

# Required integration endpoints
output "requires" {
  description = "Map of all \"requires\" endpoints"
  value = {
    sharding                  = "sharding"
    certificates              = "certificates"
    s3_credentials            = "s3-credentials"
    ldap                      = "ldap"
    ldap_certificate_transfer = "ldap-certificate-transfer"
    logging                   = "logging"
  }
}

# Offers
output "offers" {
  description = "List of offers URLs."
  value = merge(
    module.mongodb-k8s.offers,
    {
      "config_server_mongos" : try(juju_offer.config_server_mongos_offer["offered"].url, null),
      "tls_provider" : try(juju_offer.tls_provider_offer["offered"].url, null),
      "s3_credentials" : try(juju_offer.s3_integrator_offer["offered"].url, null)
    }
  )
}

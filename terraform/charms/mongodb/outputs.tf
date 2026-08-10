# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

output "application" {
  description = "Object representing the deployed MongoDB application."
  value       = juju_application.mongodb_k8s
}

output "offers" {
  description = "Map of all offers exposed by the single charm."
  value = {
    grafana_dashboard = {
      kind = "offer"
      url  = juju_offer.grafana_dashboard.url
    }
    metrics_endpoint = {
      kind = "offer"
      url  = juju_offer.metrics_endpoint.url
    }
  }
}

# Provided integration endpoints
output "provides" {
  description = "Map of all \"provides\" endpoints"
  value = {
    cluster = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "cluster"
    }
    config_server = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "config-server"
    }
    database = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "database"
    }
    grafana_dashboard = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "grafana-dashboard"
    }
    metrics_endpoint = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "metrics-endpoint"
    }
  }
}

# Required integration endpoints
output "requires" {
  description = "Map of all \"requires\" endpoints"
  value = {
    client_certificates = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "client-certificates"
    }
    gcs_credentials = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "gcs-credentials"
    }
    ldap = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "ldap"
    }
    ldap_certificate_transfer = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "ldap-certificate-transfer"
    }
    logging = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "logging"
    }
    peer_certificates = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "peer-certificates"
    }
    sharding = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "sharding"
    }
    s3_credentials = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "s3-credentials"
    }
    vault_kv = {
      kind     = "endpoint"
      name     = juju_application.mongodb_k8s.name
      endpoint = "vault-kv"
    }
  }
}

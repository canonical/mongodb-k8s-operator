# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

# Names of deployed applications
output "app_names" {
  description = "Names of of all deployed applications."
  value = {
    mongodb_config_server = module.mongodb_config_server.app_names["mongodb-k8s"]
    shards = [
      for shard_module in module.mongodb_shards : shard_module.app_names["mongodb-k8s"]
    ]
  }
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
  value = {
    mongodb_config_server = try(juju_offer.mongodb_config_server_offer["offered"].url, null)
  }
}

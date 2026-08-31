# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

output "components" {
  description = "All deployed applications."
  value = {
    mongodb         = module.mongodb.application
    data_integrator = module.data_integrator.application
    s3_integrator   = try(module.s3_integrator[0].application, null)
  }
}

output "models" {
  description = "Models and deployed components managed by this module."
  value = {
    for model_uuid in distinct([for component in local.model_components : component.model_uuid]) :
    model_uuid => {
      model_uuid = model_uuid
      components = merge([
        for component in local.model_components :
        { (component.key) = component.value }
        if component.model_uuid == model_uuid
      ]...)
    }
  }
}

output "offers" {
  description = "List of offers URLs."
  value = {
    mongodb_database = try({
      kind = "offer"
      name = module.mongodb.application.name
      url  = juju_offer.mongodb_client["offered"].url
    }, null)
    s3_integrator_credentials = try(merge(
      module.s3_integrator[0].offers.s3_credentials,
      {
        name = module.s3_integrator[0].application.name
      }
    ), null)
  }
}

output "metadata" {
  description = "Metadata of the product deployment."
  value = {
    deployed_at = terraform_data.deployed_at.output
    updated_at  = timestamp()
  }
}

output "provides" {
  description = "Map of all \"provides\" endpoints"
  value = {
    mongodb_database          = module.mongodb.provides["database"]
    mongodb_grafana_dashboard = module.mongodb.provides["grafana_dashboard"]
    mongodb_metrics_endpoint  = module.mongodb.provides["metrics_endpoint"]
  }
}

output "requires" {
  description = "Map of all \"requires\" endpoints"
  value = {
    mongodb_certificates       = module.mongodb.requires["certificates"]
    mongodb_ldap                      = module.mongodb.requires["ldap"]
    mongodb_ldap_certificate_transfer = module.mongodb.requires["ldap_certificate_transfer"]
    mongodb_logging                   = module.mongodb.requires["logging"]
    mongodb_s3_credentials            = module.mongodb.requires["s3_credentials"]
  }
}

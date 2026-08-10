# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

output "components" {
  description = "All deployed applications."
  value = merge(
    module.config_and_routing.components,
    {
      shards = [
        for shard_module in module.shards : shard_module.application
      ]
    },
    {
      data_integrator = module.data_integrator.application
      s3_integrator   = try(module.s3_integrator[0].application, null)
      gcs_integrator  = try(module.gcs_integrator[0].application, null)
    }
  )
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

output "app_names" {
  description = "Names of of all deployed applications."
  value = merge(
    module.config_and_routing.app_names,
    {
      shards = [
        for shard_module in module.shards : shard_module.application.name
      ]
    },
    {
      "data_integrator" : module.data_integrator.application.name
      "s3_integrator" : try(module.s3_integrator[0].application.name, null)
      "gcs_integrator" : try(module.gcs_integrator[0].application.name, null)
    }
  )
}

output "provides" {
  description = "Map of all \"provides\" endpoints"
  value = merge(
    module.config_and_routing.provides,
    length(module.shards) > 0 ? merge([
      for shard_key, shard_module in module.shards : {
        "${local.shards[tonumber(shard_key)].app_name}_grafana_dashboard" = shard_module.provides["grafana_dashboard"]
        "${local.shards[tonumber(shard_key)].app_name}_metrics_endpoint"  = shard_module.provides["metrics_endpoint"]
      }
    ]...) : {}
  )
}

output "requires" {
  description = "Map of all \"requires\" endpoints"
  value = merge(
    module.config_and_routing.requires,
    length(module.shards) > 0 ? merge([
      for shard_key, shard_module in module.shards : {
        "${local.shards[tonumber(shard_key)].app_name}_client_certificates" = shard_module.requires["client_certificates"]
        "${local.shards[tonumber(shard_key)].app_name}_logging"             = shard_module.requires["logging"]
        "${local.shards[tonumber(shard_key)].app_name}_peer_certificates"   = shard_module.requires["peer_certificates"]
        "${local.shards[tonumber(shard_key)].app_name}_sharding"            = shard_module.requires["sharding"]
        "${local.shards[tonumber(shard_key)].app_name}_vault_kv"            = shard_module.requires["vault_kv"]
      }
    ]...) : {}
  )
}

output "offers" {
  description = "Map of all offer endpoints."
  value = merge(
    module.config_and_routing.offers,
    length(module.shards) > 0 ? merge([
      for shard_key, shard_module in module.shards : {
        "${local.shards[tonumber(shard_key)].app_name}_grafana_dashboard" = shard_module.offers["grafana_dashboard"]
        "${local.shards[tonumber(shard_key)].app_name}_metrics_endpoint"  = shard_module.offers["metrics_endpoint"]
      }
    ]...) : {},
    {
      gcs_credentials = try(module.gcs_integrator[0].offers.gcs_credentials, null)
      s3_credentials  = try(module.s3_integrator[0].offers.s3_credentials, null)
    }
  )
}

output "metadata" {
  description = "Metadata of the product deployment."
  value = {
    deployed_at = terraform_data.deployed_at.output
    updated_at  = timestamp()
  }
}

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 3. INTEGRATIONS
#--------------------------------------------------------

resource "juju_integration" "mongodb_config_server_same_model_integrations" {
  for_each   = tomap({ for shard in local.shards_in_config_server_model : shard.app_name => shard })
  model_uuid = each.value.model_uuid

  application {
    name     = var.config_server.app_name
    endpoint = "config-server"
  }
  application {
    name     = each.value.app_name
    endpoint = "sharding"
  }

  depends_on = [
    module.mongodb_config_server,
    module.mongodb_shards,
  ]
}

resource "juju_integration" "mongodb_config_server_cross_model_integrations" {
  for_each   = tomap({ for shard in local.shards_not_in_config_server_model : shard.app_name => shard })
  model_uuid = each.value.model_uuid

  application {
    offer_url = juju_offer.mongodb_config_server_offer["offered"].url
  }
  application {
    name     = each.value.app_name
    endpoint = "sharding"
  }

  depends_on = [
    module.mongodb_config_server,
    module.mongodb_shards,
    juju_offer.mongodb_config_server_offer,
  ]
}

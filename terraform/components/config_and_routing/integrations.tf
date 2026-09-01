# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 2. INTEGRATIONS
#--------------------------------------------------------

resource "juju_integration" "config_server_mongos" {
  model_uuid = module.mongos.application.model_uuid

  application {
    name      = var.config_server.model_uuid == var.mongos.model_uuid ? module.config_server.provides["cluster"].name : null
    endpoint  = var.config_server.model_uuid == var.mongos.model_uuid ? module.config_server.provides["cluster"].endpoint : null
    offer_url = var.config_server.model_uuid != var.mongos.model_uuid ? juju_offer.config_server_cluster.url : null
  }

  application {
    name     = module.mongos.requires["cluster"].name
    endpoint = module.mongos.requires["cluster"].endpoint
  }

  depends_on = [
    module.config_server,
    module.mongos
  ]
}

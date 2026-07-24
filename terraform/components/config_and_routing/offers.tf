# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 2. OFFERS
#--------------------------------------------------------

resource "juju_offer" "config_server" {
  application_name = module.config_server.provides["config_server"].name
  endpoints        = [module.config_server.provides["config_server"].endpoint]
  model_uuid       = module.config_server.application.model_uuid
}

resource "juju_offer" "config_server_cluster" {
  application_name = module.config_server.provides["cluster"].name
  endpoints        = [module.config_server.provides["cluster"].endpoint]
  model_uuid       = module.config_server.application.model_uuid
}

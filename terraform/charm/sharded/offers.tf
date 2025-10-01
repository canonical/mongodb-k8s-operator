# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


#--------------------------------------------------------
# 2. OFFERS (if cross model)
#--------------------------------------------------------

resource "juju_offer" "mongodb_config_server_offer" {
  for_each = length(local.shards_not_in_config_server_model) > 1 ? { "offered" = true } : {}

  model            = var.config_server.model
  application_name = var.config_server.app_name
  endpoints        = ["config-server"]
}

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

#--------------------------------------------------------
# 2. Offers
#--------------------------------------------------------

resource "juju_offer" "mongodb_client" {
  for_each = var.data_integrator.model_uuid != module.mongodb.application.model_uuid ? { "offered" = true } : {}

  application_name = module.mongodb.provides["database"].name
  endpoints        = [module.mongodb.provides["database"].endpoint]
  depends_on       = [module.mongodb]
  model_uuid       = module.mongodb.application.model_uuid
}

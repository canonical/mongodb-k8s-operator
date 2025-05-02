locals {
  mongodb_apps = merge(
    {
      "config-server" = {
        app_name = var.config_server_app_name
        units    = var.config_server_replicas
        role     = "config-server"
      }
    },
    { for shard in var.shards : shard.name => {
      app_name = shard.name
      units    = shard.replicas
      role     = "shard"
      }
    }
  )
}

module "mongodb-k8s" {
  for_each = local.mongodb_apps
  source   = "../../"
  app_name = each.value.app_name
  model    = var.model_name
  units    = each.value.units
  channel  = "6/edge"
  config = {
    role = each.value.role
  }
}


resource "juju_integration" "data-integrator_mongos-integration" {
  model = var.model_name

  application {
    name = juju_application.data-integrator.name
  }
  application {
    name = juju_application.mongos-k8s.name
  }
  depends_on = [
    juju_application.data-integrator,
    juju_application.mongos-k8s
  ]

}

resource "juju_integration" "config-server_integrations" {
  for_each = tomap({
    for shard in var.shards : shard.name => {
      app_name = shard.name
    }
  })

  model = var.model_name

  application {
    name     = var.config_server_app_name
    endpoint = "config-server"
  }

  application {
    name     = each.value.app_name
    endpoint = "sharding"
  }

  depends_on = [
    module.mongodb-k8s,
  ]
}

resource "juju_integration" "mongodb_mongos-integration" {
  model = var.model_name

  application {
    name = juju_application.mongos-k8s.name
  }
  application {
    name = var.config_server_app_name
  }
  depends_on = [
    juju_application.mongos-k8s,
    module.mongodb-k8s,
    juju_integration.data-integrator_mongos-integration
  ]

}

resource "juju_integration" "tls-operator_mongodb-integration" {
  for_each = merge(
    local.mongodb_apps,
    {
      "mongos-k8s" = {
        app_name = "mongos-k8s"
        units    = 1
      }
    }
  )

  model = var.model_name

  application {
    name     = juju_application.self-signed-certificates.name
    endpoint = "certificates"
  }

  application {
    name     = each.value.app_name
    endpoint = "certificates"
  }

  depends_on = [
    juju_application.self-signed-certificates,
    juju_integration.mongodb_mongos-integration,
    juju_integration.config-server_integrations
  ]
}

resource "juju_integration" "s3-integrator_mongodb-integration" {
  model = var.model_name

  application {
    name = juju_application.s3-integrator.name
  }
  application {
    name = var.config_server_app_name
  }
  depends_on = [
    juju_application.s3-integrator,
    juju_integration.config-server_integrations,
  ]

}

resource "null_resource" "juju_wait_deployment" {
  provisioner "local-exec" {
    command = <<-EOT
    juju-wait -v --model ${var.model_name}
    EOT
  }

  depends_on = [juju_integration.tls-operator_mongodb-integration]
}

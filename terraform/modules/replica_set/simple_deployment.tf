module "mongodb-k8s" {
  source   = "../../"
  app_name = var.app_name
  model    = var.model_name
  units    = var.simple_mongodb_units
  channel  = "6/edge"
}

resource "juju_integration" "tls-operator_mongodb-integration" {
  model = var.model_name

  application {
    name     = juju_application.self-signed-certificates.name
    endpoint = "certificates"
  }
  application {
    name     = var.app_name
    endpoint = "certificates"
  }
  depends_on = [
    juju_application.self-signed-certificates,
    module.mongodb-k8s
  ]

}

resource "juju_integration" "data-integrator_mongodb-integration" {
  model = var.model_name

  application {
    name = juju_application.data-integrator.name
  }
  application {
    name = var.app_name
  }
  depends_on = [
    juju_application.data-integrator,
    module.mongodb-k8s
  ]

}

resource "juju_integration" "s3-integrator_mongodb-integration" {
  model = var.model_name

  application {
    name = juju_application.s3-integrator.name
  }
  application {
    name = var.app_name
  }
  depends_on = [
    juju_application.s3-integrator,
    module.mongodb-k8s
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

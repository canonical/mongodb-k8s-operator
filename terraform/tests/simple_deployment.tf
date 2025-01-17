module "mongodb-k8s" {
  source   = "../"
  app_name = var.app_name
  model    = var.model_name
  units    = var.simple_mongodb_units
  channel  = "6/edge"
}

resource "juju_integration" "simple_deployment_tls-operator_mongodb-integration" {
  model = var.model_name

  application {
    name = juju_application.self-signed-certificates.name
  }
  application {
    name = var.app_name
  }
  depends_on = [
    juju_application.self-signed-certificates,
    module.mongodb-k8s
  ]

}

resource "juju_integration" "simple_deployment_data-integrator_mongodb-integration" {
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

resource "null_resource" "simple_deployment_juju_wait_deployment" {
  provisioner "local-exec" {
    command = <<-EOT
    juju-wait -v --model ${var.model_name}
    EOT
  }

  depends_on = [juju_integration.simple_deployment_tls-operator_mongodb-integration]
}

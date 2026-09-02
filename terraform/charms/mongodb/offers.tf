# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

resource "juju_offer" "grafana_dashboard" {
  name             = "${juju_application.mongodb_k8s.name}-grafana-dashboard"
  application_name = juju_application.mongodb_k8s.name
  endpoints        = ["grafana-dashboard"]
  model_uuid       = juju_application.mongodb_k8s.model_uuid
}

resource "juju_offer" "metrics_endpoint" {
  name             = "${juju_application.mongodb_k8s.name}-metrics-endpoint"
  application_name = juju_application.mongodb_k8s.name
  endpoints        = ["metrics-endpoint"]
  model_uuid       = juju_application.mongodb_k8s.model_uuid
}

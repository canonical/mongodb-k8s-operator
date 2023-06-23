## Viewing Metrics

You can view Charmed MongoDB's metrics in two ways: by querying the metric endpoint directly or by viewing them in grafana.

### Querying the metric endpoint

Charmed MongoDB comes with [MongoDB Exporter](https://github.com/percona/mongodb_exporter) and provides replication and cluster metrics. The metrics can be queried by accessing the `http://<unit-ip>:9216/metrics` endpoint. To quickly view them you can do the following:

1. First identify the unit which you would like to query by entering `juju status`. Choose a unit to gather metrics from and copy its "Address"

2. Then `curl` the metrics endpoint for your unit of interest with `curl http://<unit-ip>:9216/metrics` this should show a long set of metrics.

### Accessing metrics with grafana

Charmed MongoDB provides integration with the [Canonical Observability Stack](https://charmhub.io/topics/canonical-observability-stack) - which allows you to view the metrics in a GUI. To view the GUI:

1. Deploy cos-lite bundle in a Kubernetes environment. This can be done by following the [deployment tutorial](https://charmhub.io/topics/canonical-observability-stack/tutorials/install-microk8s). Wait for the cos-lite bundle to become active and idle, by viewing `juju status`

2. Switch back to your model which hosts Charmed MongoDB and consume the offers from the COS bundle

```

juju switch <model with Charmed MongoDB K8s>

juju consume <k8s-controller-name>:admin/cos.prometheus-scrape

juju consume <k8s-controller-name>:admin/cos.alertmanager-karma-dashboard

juju consume <k8s-controller-name>:admin/cos.grafana-dashboards

juju consume <k8s-controller-name>:admin/cos.loki-logging

juju consume <k8s-controller-name>:admin/cos.prometheus-receive-remote-write

```

3. Now relate Charmed MongoDB to the applications within the cos bundle:

```

juju relate mongodb-k8s prometheus-scrape

juju relate mongodb-k8s loki-logging

juju relate mongodb-k8s grafana-dashboards

```

4. Watch `juju status` and wait for the model to become idle

5. To view the dashboard we need to find the URL of grafana along with the password for the dashboard. Switch to the k8s model hosting the COS-lite bundle and show all applications

```

juju switch <model with cos bundle>

juju status

```

under the report of juju status, the IP address of the grafana GUI should be listed as the "Address" for the application `grafana`. Copy this address and navigate to it in your browser. In your browser you should see a login. Use the username `admin`, to retrieve the password go back to your terminal and run the action: `juju run-action grafana/0 get-admin-password --wait` - use this password to access the grafana Dashboard.
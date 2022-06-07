SHELL    := /bin/bash
unit     := 0
app      := $(shell yq '.name' metadata.yaml)
model    := $(shell juju models --format=yaml|yq '.current-model')
workload := $(shell yq '.containers | keys' metadata.yaml -o t)
scale    := 3
res_name := $(shell yq '.containers.*.resource' metadata.yaml)
res_val  := $(shell yq ".resources.$(res_name).upstream-source" metadata.yaml)


build:
# pack your charm
	charmcraft pack

deploy-local:
# deploy locally with a given container as defined in metadata.yaml
# default to 3 units, but can be changed with `make deploy-local scale=n`
	juju deploy ./*.charm --resource $(res_name)=$(res_val) -n $(scale)

wait-remove:
# watch status until app is completely removed
	while true; do juju status|grep -q $(app) || break; sleep 0.5; done

remove:
# remove app from current model
	juju remove-application --destroy-storage $(app)

# redeployment chain
redeploy: remove build wait-remove deploy-local

watch-test-log:
# debug-log for integration test
	juju debug-log -m $$(juju models |grep test| awk '{ print $$1 }')

watch-test-status:
# juju status for integration test
	watch -n 1 -c juju status -m $$(juju models |grep test| awk '{ print $$1 }')

clean:
# mrproper stuff
	charmcraft clean
	rm -rf .tox .coverage
	find . -name __pycache__ -type d -exec rm -rf {} +;

install-deps:
# install deps in unit test venv
	tox -e unit --notest
	source .tox/unit/bin/activate && pip install black ipdb

pod-logs:
# workload container logs
	microk8s.kubectl logs pod/$(app)-$(unit) --namespace=$(model) --container $(workload) -f

debug-hook:
# debug hook for given unit
	juju debug-hook $(app)/$(unit)

ssh-workload-container:
# ssh into workload container for given unit (default=0)
	microk8s.kubectl exec -n $(model) -it $(app)-$(unit) --container $(workload)  -- /bin/bash

ssh-charm-container:
# ssh into charm container for given unit (default=0)
	microk8s.kubectl exec -n $(model) -it $(app)-$(unit) -- /bin/bash

prune-pvcs:
# remove dangling pvcs
	for i in $$(microk8s.kubectl get pvc -n $(model)|grep database| awk "{ print $$1 }"); do microk8s.kubectl delete pvc/$${i} -n $(model); done

get-credentials:
# run mysql specific action on leader 
	juju run-action $(app)/leader get-cluster-admin-credentials --wait

get-status:
# run mysql specific action on leader 
	juju run-action $(app)/leader get-cluster-status --wait

rerun-integration-tests:
# Rerun integration test with non-destructive flags
	PYTEST_SKIP_DEPLOY="TRUE" tox -e integration -- --model $(model) --no-deploy

refresh-charm: build
# refresh charm
	juju refresh $(app) --path ./*.charm

status-update-fast:
# make status updates run faster
	juju model-config update-status-hook-interval=30s

status-update-slow:
# make status updates run slower
	juju model-config update-status-hook-interval=5m

debug-log-level:
# config debug log level for model
	juju model-config logging-config="<root>=WARNING;unit=DEBUG"

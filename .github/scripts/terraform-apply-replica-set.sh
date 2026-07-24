#!/usr/bin/env bash
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

set -euo pipefail

# Pass the Juju model name as the first argument.
model_name="${1}"
model="$(juju show-model "${model_name}" | awk -F': ' '/model-uuid/ {print $2}')"

pushd ./terraform/product/replica_set/
terraform init
terraform apply \
  -var="mongodb={\"model_uuid\": \"${model}\", \"channel\": \"8/edge\"}" \
  -var="backups_integrator={\"storage_type\": \"s3\", \"model_uuid\": \"${model}\", \"config\": {\"bucket\": \"test\"}}" \
  -var="data_integrator={\"model_uuid\": \"${model}\"}" \
  -auto-approve
popd

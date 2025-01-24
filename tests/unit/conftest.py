# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest


@pytest.fixture
def mock_fs_interactions(mocker):
    mocker.patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.exec")
    mocker.patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.delete")
    mocker.patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.write")
    mocker.patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.start")
    mocker.patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.stop")
    mocker.patch(
        "single_kernel_mongo.core.k8s_workload.KubernetesWorkload.active",
        return_value=True,
    )
    mocker.patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.update_env")
    mocker.patch("single_kernel_mongo.core.k8s_workload.KubernetesWorkload.copy_to_unit")
    mocker.patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    mocker.patch("single_kernel_mongo.managers.config.BackupConfigManager.configure_and_restart")

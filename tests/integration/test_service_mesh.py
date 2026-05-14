#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for service mesh support with sharded MongoDB clusters."""

import json
import logging
import time
import urllib.request
from typing import Any, Dict, List

import pytest
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest

from .helpers import (
    DEPLOYMENT_TIMEOUT,
    METADATA,
    get_cluster_shards,
    get_direct_mongo_client,
    has_correct_shards,
    mongodb_uri,
    wait_for_mongodb_units_blocked,
)

logger = logging.getLogger(__name__)

CONFIG_SERVER_APP_NAME = "config-server"
SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
SHARD_APPS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME]
ALL_APPS = [CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME]

SHARD_REL_NAME = "sharding"
CONFIG_SERVER_REL_NAME = "config-server"
CONFIG_SERVER_NEEDS_SHARD_STATUS = "Missing relation to shard(s)."
SHARD_NEEDS_CONFIG_SERVER_STATUS = "Missing relation to config-server."

MONGOS_PORT = "27018"
MONGODB_PORT = "27017"

TIMEOUT = 30 * 60


async def service_mesh(
    enable: bool,
    ops_test: OpsTest,
    beacon_app_name: str,
    apps_to_be_related_with_beacon: List[str],
):
    """Enable or disable the service-mesh in the model.

    This puts the entire model, that the beacon app is part of, on mesh.
    This integrates the apps_to_be_related_with_beacon with the beacon app
    via the ``service-mesh`` relation.
    """
    assert ops_test.model is not None
    await ops_test.model.applications[beacon_app_name].set_config(
        {"model-on-mesh": str(enable).lower()}
    )
    await ops_test.model.wait_for_idle(status="active", timeout=1000)
    if enable:
        for app in apps_to_be_related_with_beacon:
            await ops_test.model.integrate(
                f"{beacon_app_name}:service-mesh", f"{app}:service-mesh"
            )
    else:
        for app in apps_to_be_related_with_beacon:
            await ops_test.model.applications[beacon_app_name].remove_relation(
                "service-mesh", f"{app}:service-mesh"
            )
    await ops_test.model.wait_for_idle(status="active", timeout=1000)


async def get_prometheus_targets(
    ops_test: OpsTest,
    prometheus_app: str = "prometheus",
    unit_num: int = 0,
) -> Dict[str, Any]:
    """Get Prometheus scrape targets."""
    status = await ops_test.model.get_status()
    address = status["applications"][prometheus_app]["units"][f"{prometheus_app}/{unit_num}"][
        "address"
    ]
    url = f"http://{address}:9090/api/v1/targets"
    response = urllib.request.urlopen(url, data=None, timeout=10.0)
    if response.code != 200:
        raise RuntimeError(f"Failed to get Prometheus targets: {response.code}")
    response_data = response.read().decode("utf-8")
    response_json = json.loads(response_data)
    if response_json.get("status") != "success":
        raise RuntimeError(f"Prometheus API returned error: {response_json}")
    return response_json.get("data", {})


def get_mongodb_targets_from_prometheus(
    targets_data: Dict[str, Any],
    app_name: str,
) -> List[Dict[str, Any]]:
    """Filter Prometheus targets to get only MongoDB targets for a specific app."""
    active_targets = targets_data.get("activeTargets", [])
    return [
        target
        for target in active_targets
        if target.get("labels", {}).get("juju_application") == app_name
    ]


@pytest.mark.setup
@pytest.mark.abort_on_fail
async def test_build_and_deploy_sharded_cluster(ops_test: OpsTest, charm: str):
    """Build and deploy sharded MongoDB cluster with Istio components."""
    assert ops_test.model is not None

    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}

    # Deploy config server
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=CONFIG_SERVER_APP_NAME,
        config={"role": "config-server"},
        num_units=2,
        trust=True,
    )

    # Deploy two shards
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=SHARD_ONE_APP_NAME,
        config={"role": "shard"},
        num_units=2,
        trust=True,
    )
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=SHARD_TWO_APP_NAME,
        config={"role": "shard"},
        num_units=2,
        trust=True,
    )

    # Deploy Istio service mesh components
    await ops_test.model.deploy(
        "istio-k8s",
        application_name="istio",
        channel="2/edge",
        trust=True,
    )
    await ops_test.model.deploy(
        "istio-beacon-k8s",
        application_name="istio-beacon",
        channel="2/edge",
        trust=True,
    )

    # Deploy Prometheus for metrics testing
    await ops_test.model.deploy(
        "prometheus-k8s",
        application_name="prometheus",
        channel="1/edge",
        trust=True,
    )

    await ops_test.model.wait_for_idle(
        apps=["istio", "istio-beacon", "prometheus"],
        status="active",
        timeout=DEPLOYMENT_TIMEOUT,
        idle_period=20,
    )

    # Wait for MongoDB apps - they will be blocked until sharding relations are added
    await ops_test.model.wait_for_idle(
        apps=ALL_APPS,
        timeout=DEPLOYMENT_TIMEOUT,
        idle_period=20,
        raise_on_blocked=False,
        raise_on_error=False,
    )

    # Verify that MongoDB apps are blocked and report missing relations
    await wait_for_mongodb_units_blocked(
        ops_test,
        CONFIG_SERVER_APP_NAME,
        status=CONFIG_SERVER_NEEDS_SHARD_STATUS,
        timeout=300,
    )
    await wait_for_mongodb_units_blocked(
        ops_test,
        SHARD_ONE_APP_NAME,
        status=SHARD_NEEDS_CONFIG_SERVER_STATUS,
        timeout=300,
    )
    await wait_for_mongodb_units_blocked(
        ops_test,
        SHARD_TWO_APP_NAME,
        status=SHARD_NEEDS_CONFIG_SERVER_STATUS,
        timeout=300,
    )


@pytest.mark.setup
@pytest.mark.abort_on_fail
async def test_integrate_sharded_cluster(ops_test: OpsTest):
    """Integrate sharded cluster and observability relations before enabling mesh."""
    assert ops_test.model is not None

    # Integrate shards with config server
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )
    await ops_test.model.integrate(
        f"{SHARD_TWO_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    # Integrate metrics endpoints with Prometheus
    await ops_test.model.integrate(
        f"{CONFIG_SERVER_APP_NAME}:metrics-endpoint",
        "prometheus:metrics-endpoint",
    )
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:metrics-endpoint",
        "prometheus:metrics-endpoint",
    )
    await ops_test.model.integrate(
        f"{SHARD_TWO_APP_NAME}:metrics-endpoint",
        "prometheus:metrics-endpoint",
    )

    await ops_test.model.wait_for_idle(
        apps=ALL_APPS,
        status="active",
        timeout=TIMEOUT,
        idle_period=30,
    )


@pytest.mark.setup
@pytest.mark.abort_on_fail
async def test_verify_sharded_cluster_before_mesh(ops_test: OpsTest):
    """Verify sharded cluster is functioning before enabling service mesh."""
    # Get mongos connection to config server
    mongos_client = await get_direct_mongo_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )

    # Verify sharded cluster config
    assert has_correct_shards(
        mongos_client,
        expected_shards=[SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME],
    ), "Config server did not process config properly"

    logger.info("Sharded cluster is properly configured before enabling service mesh")


@pytest.mark.setup
@pytest.mark.abort_on_fail
async def test_enable_service_mesh(ops_test: OpsTest):
    """Enable service mesh for all MongoDB apps.

    This is done after establishing relations to ensure apps can communicate first.
    """
    await service_mesh(
        enable=True,
        ops_test=ops_test,
        beacon_app_name="istio-beacon",
        apps_to_be_related_with_beacon=ALL_APPS + ["prometheus"],
    )

    logger.info("Service mesh enabled for all applications")


@pytest.mark.abort_on_fail
async def test_sharded_cluster_with_mesh(ops_test: OpsTest):
    """Verify sharded cluster still functions correctly with service mesh enabled."""
    # Get mongos connection through the mesh
    mongos_client = await get_direct_mongo_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )

    # Verify sharded cluster configuration is still correct
    assert has_correct_shards(
        mongos_client,
        expected_shards=[SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME],
    ), "Sharded cluster configuration broken after enabling service mesh"

    logger.info("Sharded cluster configuration correct with service mesh enabled")


@pytest.mark.abort_on_fail
async def test_database_operations_with_mesh(ops_test: OpsTest):
    """Test that database write and read operations work through the service mesh."""
    # Get mongos connection
    connection_string = await mongodb_uri(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, port=MONGOS_PORT
    )
    client = MongoClient(connection_string)

    # Create a test database and collection
    test_db_name = "test_mesh_db"
    test_collection_name = "test_collection"
    test_data = {"test_key": "test_value", "mesh_enabled": True, "timestamp": time.time()}

    # Write data through the mesh
    db = client[test_db_name]
    collection = db[test_collection_name]
    insert_result = collection.insert_one(test_data)
    assert insert_result.acknowledged, "Failed to write data through service mesh"

    logger.info("Successfully wrote data to MongoDB through service mesh")

    # Read data back through the mesh
    retrieved_data = collection.find_one({"_id": insert_result.inserted_id})
    assert retrieved_data is not None, "Failed to read data through service mesh"
    assert retrieved_data["test_key"] == "test_value", "Data corrupted through service mesh"
    assert retrieved_data["mesh_enabled"] is True, "Data integrity issue through service mesh"

    logger.info("Successfully read data from MongoDB through service mesh")

    # Verify data count
    count = collection.count_documents({})
    assert count >= 1, "Document count verification failed through service mesh"

    logger.info(f"Verified {count} document(s) in collection through service mesh")

    # Cleanup
    collection.drop()
    client.close()


@pytest.mark.abort_on_fail
async def test_metrics_endpoint_all_units_with_mesh(ops_test: OpsTest):
    """Check that all MongoDB units appear in Prometheus scrape targets with mesh enabled."""
    assert ops_test.model is not None

    # Wait for Prometheus to scrape the targets
    time.sleep(60)

    # Query Prometheus for targets and verify each app
    for app_name in ALL_APPS:
        expected_unit_count = len(ops_test.model.applications[app_name].units)

        targets_data = await get_prometheus_targets(ops_test, "prometheus")
        mongodb_targets = get_mongodb_targets_from_prometheus(targets_data, app_name)

        # Check that we have targets for all units
        assert len(mongodb_targets) >= expected_unit_count, (
            f"Expected at least {expected_unit_count} MongoDB targets for {app_name} "
            f"in Prometheus, got {len(mongodb_targets)}"
        )

        # Verify all targets are healthy
        unhealthy_targets = [
            target for target in mongodb_targets if target.get("health") != "up"
        ]
        assert not unhealthy_targets, (
            f"Some MongoDB targets for {app_name} are not healthy: {unhealthy_targets}"
        )

        logger.info(
            f"All {len(mongodb_targets)} MongoDB targets for {app_name} "
            f"are healthy in Prometheus"
        )


@pytest.mark.abort_on_fail
async def test_peer_communication_with_mesh(ops_test: OpsTest):
    """Verify that replica set peer communication works through the service mesh."""
    # Test each shard's replica set
    for shard_app in SHARD_APPS:
        # Get a direct connection to verify replica set status
        connection_string = await mongodb_uri(ops_test, app_name=shard_app, port=MONGODB_PORT)
        client = MongoClient(connection_string)

        # Check replica set status
        rs_status = client.admin.command("replSetGetStatus")
        assert rs_status["ok"] == 1, f"Replica set {shard_app} not healthy with mesh"

        # Verify all members are reachable
        members = rs_status["members"]
        expected_members = len(ops_test.model.applications[shard_app].units)
        assert (
            len(members) == expected_members
        ), f"Expected {expected_members} members in {shard_app}, got {len(members)}"

        # Verify all members are in healthy states (PRIMARY or SECONDARY)
        for member in members:
            assert member["state"] in [1, 2], (
                f"Member {member['name']} in {shard_app} is in unhealthy state "
                f"{member['stateStr']} with mesh enabled"
            )

        logger.info(
            f"Replica set {shard_app} has {len(members)} healthy members with service mesh"
        )

        client.close()

    logger.info("All replica sets functioning correctly with service mesh peer communication")

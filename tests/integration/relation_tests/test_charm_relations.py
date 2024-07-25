#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import time
from pathlib import Path

import pytest
import yaml
from pymongo.uri_parser import parse_uri
from pytest_operator.plugin import OpsTest
from tenacity import RetryError

from ..ha_tests.helpers import get_replica_set_primary as replica_set_primary
from ..helpers import check_or_scale_app, get_app_name, is_relation_joined, run_mongo_op
from .helpers import (
    get_application_relation_data,
    get_connection_string,
    verify_application_data,
)

logger = logging.getLogger(__name__)

MEDIAN_REELECTION_TIME = 12
APPLICATION_APP_NAME = "application"
DATABASE_METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
PORT = 27017
DATABASE_APP_NAME = "mongodb-k8s"
FIRST_DATABASE_RELATION_NAME = "first-database"
SECOND_DATABASE_RELATION_NAME = "second-database"
MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "multiple-database-clusters"
ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "aliased-multiple-database-clusters"
ANOTHER_DATABASE_APP_NAME = "another-database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME, ANOTHER_DATABASE_APP_NAME]
TEST_APP_CHARM_PATH = "./tests/integration/relation_tests/application-charm"
REQUIRED_UNITS = 2


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms (2 units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    application_charm = await ops_test.build_charm(TEST_APP_CHARM_PATH)
    database_charm = await ops_test.build_charm(".")

    app_name = await get_app_name(ops_test)
    if app_name == ANOTHER_DATABASE_APP_NAME:
        assert (
            False
        ), f"provided MongoDB application, cannot be named {ANOTHER_DATABASE_APP_NAME}, this name is reserved for this test."

    db_resources = {
        "mongodb-image": DATABASE_METADATA["resources"]["mongodb-image"]["upstream-source"]
    }

    if app_name:
        await asyncio.gather(check_or_scale_app(ops_test, app_name, REQUIRED_UNITS))
    else:
        await asyncio.gather(
            ops_test.model.deploy(
                database_charm,
                application_name=DATABASE_APP_NAME,
                resources=db_resources,
                num_units=REQUIRED_UNITS,
            )
        )

    await asyncio.gather(
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=REQUIRED_UNITS,
        ),
        ops_test.model.deploy(
            database_charm,
            application_name=ANOTHER_DATABASE_APP_NAME,
            resources=db_resources,
            num_units=REQUIRED_UNITS,
        ),
    )

    APP_NAMES.append(await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME]))
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", timeout=1000)


@pytest.mark.group(1)
async def verify_crud_operations(ops_test: OpsTest, connection_string: str):
    # insert some data
    cmd = (
        'var ubuntu = {"release_name": "Focal Fossa", "version": "20.04", "LTS": "true"}; '
        "EJSON.stringify(db.test_collection.insertOne(ubuntu), );"
    )
    result = await run_mongo_op(ops_test, cmd, f'"{connection_string}"', stringify=False)
    assert result.data["acknowledged"] is True

    # query the data
    cmd = 'db.test_collection.find({}, {"release_name": 1}).toArray()'
    result = await run_mongo_op(
        ops_test, f"EJSON.stringify({cmd})", f'"{connection_string}"', stringify=False
    )
    assert result.data[0]["release_name"] == "Focal Fossa"

    # update the data
    ubuntu_version = '{"version": "20.04"}'
    ubuntu_name_updated = '{"$set": {"release_name": "Fancy Fossa"}}'
    cmd = f"EJSON.stringify(db.test_collection.updateOne({ubuntu_version}, {ubuntu_name_updated}))"
    result = await run_mongo_op(
        ops_test, cmd, f'"{connection_string}"', stringify=False, expect_json_load=False
    )
    assert result.data["acknowledged"] is True

    # query the data
    cmd = 'db.test_collection.find({}, {"release_name": 1}).toArray()'
    result = await run_mongo_op(
        ops_test, f"EJSON.stringify({cmd})", f'"{connection_string}"', stringify=False
    )
    assert len(result.data) == 1
    assert result.data[0]["release_name"] == "Fancy Fossa"

    # delete the data
    cmd = 'EJSON.stringify(db.test_collection.deleteOne({"release_name": "Fancy Fossa"}))'
    result = await run_mongo_op(
        ops_test, cmd, f'"{connection_string}"', stringify=False, expect_json_load=False
    )
    assert result.data["acknowledged"] is True

    # query the data
    cmd = 'db.test_collection.find({}, {"release_name": 1}).toArray()'
    result = await run_mongo_op(
        ops_test, f"EJSON.stringify({cmd})", f'"{connection_string}"', stringify=False
    )
    assert len(result.data) == 0


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_database_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of database relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    db_app_name = await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME])

    await ops_test.model.integrate(
        f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", db_app_name
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    await ops_test.model.block_until(
        lambda: is_relation_joined(
            ops_test,
            f"{FIRST_DATABASE_RELATION_NAME}",
            "database",
        )
        is True,
        timeout=600,
    )

    connection_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    await verify_crud_operations(ops_test, connection_string)


@pytest.mark.group(1)
async def verify_primary(ops_test: OpsTest, application_name: str):
    # verify primary is present in hosts provided to application
    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)
    await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "endpoints"
    )

    try:
        primary = await replica_set_primary(ops_test, application_name=application_name)
    except RetryError:
        assert False, "replica set has no primary"

    assert primary is not None, "Replica set has no primary"


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_app_relation_metadata_change(ops_test: OpsTest) -> None:
    """Verifies that the app metadata changes with db relation joined and departed events."""
    # verify application metadata is correct before adding/removing units.
    db_app_name = await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME])

    try:
        await verify_application_data(
            ops_test, APPLICATION_APP_NAME, db_app_name, FIRST_DATABASE_RELATION_NAME
        )
    except RetryError:
        assert False, "Hosts are not correct in application data."

    connection_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )

    connection_data = parse_uri(connection_string)
    assert len(connection_data["nodelist"]) == 2
    assert sorted(connection_data["nodelist"])[0][0] == "mongodb-k8s-0.mongodb-k8s-endpoints"

    # verify application metadata is correct after adding units.
    await ops_test.model.applications[db_app_name].add_units(count=2)
    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        status="active",
        timeout=1000,
    )

    try:
        await verify_application_data(
            ops_test, APPLICATION_APP_NAME, db_app_name, FIRST_DATABASE_RELATION_NAME
        )
    except RetryError:
        assert False, "Hosts not updated in application data after adding units."

    await verify_primary(ops_test, db_app_name)

    scaled_up_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    scaled_up_data = parse_uri(scaled_up_string)
    assert len(scaled_up_data["nodelist"]) == 4
    scaled_up_data["nodelist"].sort()
    assert all(
        [
            a[0] == b
            for a, b in zip(
                scaled_up_data["nodelist"],
                [
                    "mongodb-k8s-0.mongodb-k8s-endpoints",
                    "mongodb-k8s-1.mongodb-k8s-endpoints",
                    "mongodb-k8s-2.mongodb-k8s-endpoints",
                ],
            )
        ]
    )

    # test crud operations
    await verify_crud_operations(ops_test, scaled_up_string)

    # verify application metadata is correct after removing the pre-existing units. This is
    # this is important since we want to test that the application related will work with
    # only the newly added units from above.
    await ops_test.model.applications[db_app_name].scale(scale_change=-1)
    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        status="active",
        timeout=1000,
    )

    await verify_primary(ops_test, db_app_name)

    scaled_down_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )

    scaled_down_data = parse_uri(scaled_down_string)
    assert len(scaled_down_data["nodelist"]) == 3
    scaled_down_data["nodelist"].sort()
    assert all(
        [
            a[0] == b
            for a, b in zip(
                scaled_down_data["nodelist"],
                ["mongodb-k8s-0.mongodb-k8s-endpoints", "mongodb-k8s-1.mongodb-k8s-endpoints"],
            )
        ]
    )
    # test crud operations
    await verify_crud_operations(ops_test, scaled_down_string)

    await ops_test.model.applications[db_app_name].scale(scale_change=-1)
    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        status="active",
        timeout=1000,
    )

    try:
        await verify_application_data(
            ops_test, APPLICATION_APP_NAME, db_app_name, FIRST_DATABASE_RELATION_NAME
        )
    except RetryError:
        assert False, "Hosts not updated in application data after removing units."

    await verify_primary(ops_test, db_app_name)

    scaled_down_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    scaled_down_data = parse_uri(scaled_down_string)
    assert len(scaled_down_data["nodelist"]) == 2
    assert sorted(scaled_down_data["nodelist"])[0][0] == "mongodb-k8s-0.mongodb-k8s-endpoints"

    # test crud operations
    await verify_crud_operations(ops_test, scaled_down_string)


@pytest.mark.group(1)
async def test_user_with_extra_roles(ops_test: OpsTest):
    """Test superuser actions (ie creating a new user and creating a new database)."""
    connection_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    database = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "database"
    )

    cmd = f'db.createUser({{user: "newTestUser", pwd: "Test123", roles: [{{role: "readWrite", db: "{database}"}}]}})'
    result = await run_mongo_op(
        ops_test,
        f'"EJSON.stringify({cmd})"',
        f'"{connection_string}"',
        stringify=False,
        expect_json_load=False,
    )
    cmd = "db.getUsers()"

    result = await run_mongo_op(
        ops_test,
        f"EJSON.stringify({cmd})",
        f'"{connection_string}"',
        stringify=False,
        expect_json_load=False,
    )
    # assert "application_first_database.newTestUser" in str(result)
    assert result.data["users"][0]["_id"] == "application_first_database.newTestUser"
    cmd = 'db = db.getSiblingDB("new_database"); EJSON.stringify(db.test_collection.insertOne({"test": "one"}));'
    result = await run_mongo_op(
        ops_test, cmd, f'"{connection_string}"', stringify=False, expect_json_load=False
    )
    assert result.data["acknowledged"] is True


@pytest.mark.group(1)
async def test_two_applications_doesnt_share_the_same_relation_data(ops_test: OpsTest):
    """Test that two different application connect to the database with different credentials."""
    # Set some variables to use in this test.
    application_charm = await ops_test.build_charm(TEST_APP_CHARM_PATH)
    another_application_app_name = "another-application"
    all_app_names = [another_application_app_name]
    all_app_names.extend(APP_NAMES)

    # Deploy another application.
    await ops_test.model.deploy(
        application_charm,
        application_name=another_application_app_name,
    )
    await ops_test.model.wait_for_idle(apps=all_app_names, status="active")

    db_app_name = await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME])

    # Relate the new application with the database
    # and wait for them exchanging some connection data.
    await ops_test.model.integrate(
        f"{another_application_app_name}:{FIRST_DATABASE_RELATION_NAME}", db_app_name
    )
    await ops_test.model.wait_for_idle(apps=all_app_names, status="active")

    # Assert the two application have different relation (connection) data.
    application_connection_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    another_application_connection_string = await get_connection_string(
        ops_test, another_application_app_name, FIRST_DATABASE_RELATION_NAME
    )
    assert application_connection_string != another_application_connection_string


@pytest.mark.group(1)
async def test_an_application_can_connect_to_multiple_database_clusters(ops_test: OpsTest):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    db_app_name = await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME])

    first_cluster_relation = await ops_test.model.integrate(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}", db_app_name
    )
    second_cluster_relation = await ops_test.model.integrate(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        ANOTHER_DATABASE_APP_NAME,
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = await get_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_id=first_cluster_relation.id,
    )

    another_application_connection_string = await get_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_id=second_cluster_relation.id,
    )

    assert application_connection_string != another_application_connection_string


@pytest.mark.group(1)
async def test_an_application_can_connect_to_multiple_aliased_database_clusters(ops_test: OpsTest):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    db_app_name = await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME])
    await asyncio.gather(
        ops_test.model.integrate(
            f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
            db_app_name,
        ),
        ops_test.model.integrate(
            f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
            ANOTHER_DATABASE_APP_NAME,
        ),
    )

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = await get_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_alias="cluster1",
    )

    another_application_connection_string = await get_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_alias="cluster2",
    )

    assert application_connection_string != another_application_connection_string


@pytest.mark.group(1)
async def test_an_application_can_request_multiple_databases(ops_test: OpsTest):
    """Test that an application can request additional databases using the same interface."""
    # Relate the charms using another relation and wait for them exchanging some connection data.
    db_app_name = await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME])

    await ops_test.model.integrate(
        f"{APPLICATION_APP_NAME}:{SECOND_DATABASE_RELATION_NAME}", db_app_name
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection strings to connect to both databases.
    first_database_connection_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    second_database_connection_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME
    )

    # Assert the two application have different relation (connection) data.
    assert first_database_connection_string != second_database_connection_string


@pytest.mark.group(1)
async def test_removed_relation_no_longer_has_access(ops_test: OpsTest):
    """Verify removed applications no longer have access to the database."""
    # before removing relation we need its authorisation via connection string
    connection_string = await get_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    db_app_name = await get_app_name(ops_test, test_deployments=[ANOTHER_DATABASE_APP_NAME])

    await ops_test.model.applications[db_app_name].remove_relation(
        f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", f"{db_app_name}:database"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    removed_access = False
    cmd = "db.runCommand({ replSetGetStatus : 1 });"
    result = await run_mongo_op(
        ops_test, cmd, f'"{connection_string}"', stringify=False, expect_json_load=False
    )

    removed_access = False
    if (
        result.failed
        and "code" in result.data
        and result.data["code"] == 1
        and "Authentication failed" in result.data["stderr"]
    ):
        removed_access = True
    elif result.failed:
        raise Exception(
            "OperationFailure: code {}; stdout {}; stderr: {}".format(
                result.data["code"], result.data["stdout"], result.data["stderr"]
            )
        )
    assert (
        removed_access
    ), "application: {APPLICATION_APP_NAME} still has access to mongodb after relation removal."

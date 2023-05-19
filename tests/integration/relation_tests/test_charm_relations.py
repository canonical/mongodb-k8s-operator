#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import time
import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pytest_operator.plugin import OpsTest
from tenacity import RetryError
from lightkube.core.client import Client
from lightkube.resources.core_v1 import Pod


from ..ha_tests.helpers import get_replica_set_primary as replica_set_primary
from .helpers import get_application_relation_data, verify_application_data, get_info_from_mongo_connection_string
from  .constants import *

@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms (2 units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    
    application_charm = await ops_test.build_charm(TEST_APP_CHARM_PATH)
    database_charm = await ops_test.build_charm(".")
    db_resources = {"mongodb-image": DATABASE_METADATA["resources"]["mongodb-image"]["upstream-source"]}
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=2,
        ),
        ops_test.model.deploy(
            database_charm,
            application_name=DATABASE_APP_NAME, 
            resources=db_resources,
            num_units=2,
        ),
        ops_test.model.deploy(
            database_charm,
            application_name=ANOTHER_DATABASE_APP_NAME,
            resources=db_resources
        ),
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, 
                                       status="active", 
                                       wait_for_units=1, 
                                       timeout=1000)

@pytest.mark.abort_on_fail
async def test_database_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of database relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    connection_string = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
    )
    database = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "database"
    )
    connection_params = get_info_from_mongo_connection_string(connection_string)
    model = ops_test.model.info
    client = Client(namespace=model.name)
    ips = []
    for host in connection_params["hosts"]:
        pod = client.get(Pod, name=host.split(".")[0])
        ips.append(pod.status.podIP)
    connection_params["hosts"] = ips
    connection_string = f"mongodb://{connection_params['username']}:{connection_params['password']}@{','.join(connection_params['hosts'])}/{connection_params['database']}?replicaSet={connection_params['replicaSet']}"  
    client = MongoClient(
        connection_string,
        directConnection=False,
        connect=False,
        serverSelectionTimeoutMS=1000,
        connectTimeoutMS=2000,
    )
    # test crud operations
    db = client[database]
    test_collection = db["test_collection"]
    ubuntu = {"release_name": "Focal Fossa", "version": 20.04, "LTS": True}
    test_collection.insert_one(ubuntu)

    query = test_collection.find({}, {"release_name": 1})
    assert query[0]["release_name"] == "Focal Fossa"

    ubuntu_version = {"version": 20.04}
    ubuntu_name_updated = {"$set": {"release_name": "Fancy Fossa"}}
    test_collection.update_one(ubuntu_version, ubuntu_name_updated)

    query = test_collection.find({}, {"release_name": 1})
    assert query[0]["release_name"] == "Fancy Fossa"

    test_collection.delete_one({"release_name": "Fancy Fossa"})
    assert test_collection.count_documents({"release_name": 1}) == 0

    client.close()


@pytest.mark.abort_on_fail
async def test_app_relation_metadata_change(ops_test: OpsTest) -> None:
    """Verifies that the app metadata changes with db relation joined and departed events."""
    # verify application metadata is correct before adding/removing units.
    try:
        await verify_application_data(
            ops_test, APPLICATION_APP_NAME, DATABASE_APP_NAME, FIRST_DATABASE_RELATION_NAME
        )
    except RetryError:
        assert False, "Hosts are not correct in application data."

    # verify application metadata is correct after adding units.
    await ops_test.model.applications[DATABASE_APP_NAME].add_units(count=2)
    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        status="active",
        timeout=1000,
    )

    try:
        await verify_application_data(
            ops_test, APPLICATION_APP_NAME, DATABASE_APP_NAME, FIRST_DATABASE_RELATION_NAME
        )
    except RetryError:
        assert False, "Hosts not updated in application data after adding units."

    # verify application metadata is correct after removing the pre-existing units. This is
    # this is important since we want to test that the application related will work with
    # only the newly added units from above.
    await ops_test.model.applications[DATABASE_APP_NAME].destroy_units(f"{DATABASE_APP_NAME}/0")
    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        status="active",
        timeout=1000,
    )

    await ops_test.model.applications[DATABASE_APP_NAME].destroy_units(f"{DATABASE_APP_NAME}/1")
    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        status="active",
        timeout=1000,
    )

    try:
        await verify_application_data(
            ops_test, APPLICATION_APP_NAME, DATABASE_APP_NAME, FIRST_DATABASE_RELATION_NAME
        )
    except RetryError:
        assert False, "Hosts not updated in application data after removing units."

    # verify primary is present in hosts provided to application
    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)
    endpoints_str = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "endpoints"
    )
    ip_addresses = endpoints_str.split(",")
    try:
        primary = await replica_set_primary(ip_addresses, ops_test, app=DATABASE_APP_NAME)
    except RetryError:
        assert False, "replica set has no primary"

    assert primary.public_address in endpoints_str.split(
        ","
    ), "Primary is not present in DB endpoints."

    # test crud operations
    connection_string = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
    )
    database = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "database"
    )
    client = MongoClient(
        connection_string,
        directConnection=False,
        connect=False,
        serverSelectionTimeoutMS=1000,
        connectTimeoutMS=2000,
    )

    db = client[database]
    # clear collection  writes from previous test
    test_collection = db["test_app_collection"]
    test_collection.drop()
    test_collection = db["test_app_collection"]
    ubuntu = {"release_name": "Focal Fossa", "version": 20.04, "LTS": True}
    test_collection.insert_one(ubuntu)

    query = test_collection.find({}, {"release_name": 1})
    assert query[0]["release_name"] == "Focal Fossa"

    ubuntu_version = {"version": 20.04}
    ubuntu_name_updated = {"$set": {"release_name": "Fancy Fossa"}}
    test_collection.update_one(ubuntu_version, ubuntu_name_updated)

    query = test_collection.find({}, {"release_name": 1})
    assert query[0]["release_name"] == "Fancy Fossa"

    test_collection.delete_one({"release_name": "Fancy Fossa"})
    assert test_collection.count_documents({"release_name": 1}) == 0

    client.close()


async def test_user_with_extra_roles(ops_test: OpsTest):
    """Test superuser actions (ie creating a new user and creating a new database)."""
    connection_string = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
    )
    database = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "database"
    )
    client = MongoClient(
        connection_string,
        directConnection=False,
        connect=False,
        serverSelectionTimeoutMS=1000,
        connectTimeoutMS=2000,
    )
    client.admin.command(
        "createUser", "newTestUser", pwd="Test123", roles=[{"role": "readWrite", "db": database}]
    )
    client["new_database"]
    client.close()


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

    # Relate the new application with the database
    # and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        f"{another_application_app_name}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=all_app_names, status="active")

    # Assert the two application have different relation (connection) data.
    application_connection_string = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
    )
    another_application_connection_string = await get_application_relation_data(
        ops_test, another_application_app_name, FIRST_DATABASE_RELATION_NAME, "uris"
    )
    assert application_connection_string != another_application_connection_string


async def test_an_application_can_connect_to_multiple_database_clusters(ops_test: OpsTest):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    first_cluster_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}", DATABASE_APP_NAME
    )
    second_cluster_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        ANOTHER_DATABASE_APP_NAME,
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = await get_application_relation_data(
        ops_test,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        "uris",
        relation_id=first_cluster_relation.id,
    )

    another_application_connection_string = await get_application_relation_data(
        ops_test,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        "uris",
        relation_id=second_cluster_relation.id,
    )

    assert application_connection_string != another_application_connection_string


async def test_an_application_can_connect_to_multiple_aliased_database_clusters(ops_test: OpsTest):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    await asyncio.gather(
        ops_test.model.add_relation(
            f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
            DATABASE_APP_NAME,
        ),
        ops_test.model.add_relation(
            f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
            ANOTHER_DATABASE_APP_NAME,
        ),
    )

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = await get_application_relation_data(
        ops_test,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        "uris",
        relation_alias="cluster1",
    )

    another_application_connection_string = await get_application_relation_data(
        ops_test,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        "uris",
        relation_alias="cluster2",
    )

    assert application_connection_string != another_application_connection_string


async def test_an_application_can_request_multiple_databases(ops_test: OpsTest):
    """Test that an application can request additional databases using the same interface."""
    # Relate the charms using another relation and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection strings to connect to both databases.
    first_database_connection_string = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
    )
    second_database_connection_string = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "uris"
    )

    # Assert the two application have different relation (connection) data.
    assert first_database_connection_string != second_database_connection_string


async def test_removed_relation_no_longer_has_access(ops_test:  OpsTest):
    """Verify removed applications no longer have access to the database."""
    # before removing relation we need its authorisation via connection string
    connection_string = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
    )

    await ops_test.model.applications[DATABASE_APP_NAME].remove_relation(
        f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", f"{DATABASE_APP_NAME}:database"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    client = MongoClient(
        connection_string,
        directConnection=False,
        connect=False,
        serverSelectionTimeoutMS=1000,
        connectTimeoutMS=2000,
    )
    removed_access = False
    try:
        client.admin.command("replSetGetStatus")
    except OperationFailure as e:
        # error code 18 for OperationFailure is an authentication error, meaning disabling of
        # authentication was successful
        if e.code == 18:
            removed_access = True
        else:
            raise

    assert (
        removed_access
    ), "application: {APPLICATION_APP_NAME} still has access to mongodb after relation removal."

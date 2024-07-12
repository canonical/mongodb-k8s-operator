#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import time

import pytest
from ops import Unit
from pytest_operator.plugin import OpsTest
import os

from ..helpers import (
    check_or_scale_app,
    get_app_name,
    get_application_relation_data,
    get_secret_content,
    get_secret_id,
)
from .helpers import (
    METADATA,
    check_tls,
    scp_file_preserve_ctime,
    time_file_created,
    time_process_started,
)

TLS_CERTIFICATES_APP_NAME = "self-signed-certificates"
DATABASE_APP_NAME = "mongodb-k8s"
TLS_RELATION_NAME = "certificates"
TLS_TEST_DATA = "tests/integration/tls_tests/data"
EXTERNAL_CERT_PATH = "/etc/mongod/external-ca.crt"
INTERNAL_CERT_PATH = "/etc/mongod/internal-ca.crt"
DB_SERVICE = "mongod.service"

logger = logging.getLogger(__name__)


@pytest.mark.group(1)
async def check_certs_correctly_distributed(
    ops_test: OpsTest, unit: Unit, app_name: str | None = None
) -> None:
    """Comparing expected vs distributed certificates.

    Verifying certificates downloaded on the charm against the ones distributed by the TLS operator
    """
    app_name = app_name or await get_app_name(ops_test)
    unit_secret_id = await get_secret_id(ops_test, unit.name)
    unit_secret_content = await get_secret_content(ops_test, unit_secret_id)

    # Get the values for certs from the relation, as provided by TLS Charm
    certificates_raw_data = await get_application_relation_data(
        ops_test, app_name, TLS_RELATION_NAME, "certificates"
    )
    certificates_data = json.loads(certificates_raw_data)

    # compare the TLS resources stored on the disk of the unit with the ones from the TLS relation
    for cert_type, cert_path in [("int", INTERNAL_CERT_PATH), ("ext", EXTERNAL_CERT_PATH)]:
        unit_csr = unit_secret_content[f"{cert_type}-csr-secret"]
        tls_item = [
            data
            for data in certificates_data
            if data["certificate_signing_request"].rstrip() == unit_csr.rstrip()
        ][0]

        # Read the content of the cert file stored in the unit
        cert_file_copy_path = await scp_file_preserve_ctime(ops_test, unit.name, cert_path)
        with open(cert_file_copy_path, mode="r") as f:
            cert_file_content = f.read()

        # cleanup the file
        os.remove(cert_file_copy_path)

        # Get the external cert value from the relation
        relation_cert = "\n".join(tls_item["chain"]).strip()

        # confirm that they match
        assert (
            relation_cert == cert_file_content
        ), f"Relation Content for {cert_type}-cert:\n{relation_cert}\nFile Content:\n{cert_file_content}\nMismatch."


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy three units of MongoDB and one unit of TLS."""
    app_name = await get_app_name(ops_test)
    if app_name:
        await check_or_scale_app(ops_test, app_name, 3)
    else:
        app_name = DATABASE_APP_NAME
        async with ops_test.fast_forward():
            my_charm = await ops_test.build_charm(".")
            resources = {
                "mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]
            }
            await ops_test.model.deploy(my_charm, num_units=3, resources=resources, series="jammy")
            await ops_test.model.wait_for_idle(apps=[app_name], status="active", timeout=2000)

    tls_app_deployed = False
    status = await ops_test.model.get_status()
    for app in ops_test.model.applications:
        if TLS_CERTIFICATES_APP_NAME in status["applications"][app]["charm"]:
            logger.debug("Found TLS app name app named '%s'", app)
            tls_app_deployed = True

    if not tls_app_deployed:
        async with ops_test.fast_forward():
            config = {"ca-common-name": "Test CA"}
            await ops_test.model.deploy(
                TLS_CERTIFICATES_APP_NAME, channel="stable", config=config, series="jammy"
            )
            await ops_test.model.wait_for_idle(
                apps=[TLS_CERTIFICATES_APP_NAME], status="active", timeout=1000
            )


@pytest.mark.group(1)
async def test_enable_tls(ops_test: OpsTest) -> None:
    """Verify each unit has TLS enabled after relating to the TLS application."""
    # Relate it to the MongoDB to enable TLS.
    app_name = await get_app_name(ops_test)
    # check if relation exists
    await ops_test.model.integrate(app_name, TLS_CERTIFICATES_APP_NAME)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(status="active", timeout=1000)

    # Wait for all units enabling TLS.
    for unit in ops_test.model.applications[app_name].units:
        assert await check_tls(ops_test, unit, enabled=True)


@pytest.mark.group(1)
async def test_rotate_tls_key(ops_test: OpsTest) -> None:
    """Verify rotating tls private keys restarts mongod with new certificates.

    This test rotates tls private keys to randomly generated keys.
    """
    # dict of values for cert file certion and mongod service start times. After resetting the
    # private keys these certificates should be updated and the mongod service should be
    # restarted
    original_tls_times = {}

    app_name = await get_app_name(ops_test)

    for unit in ops_test.model.applications[app_name].units:
        original_tls_times[unit.name] = {}
        original_tls_times[unit.name]["external_cert"] = await time_file_created(
            ops_test, unit.name, EXTERNAL_CERT_PATH
        )
        original_tls_times[unit.name]["internal_cert"] = await time_file_created(
            ops_test, unit.name, INTERNAL_CERT_PATH
        )
        original_tls_times[unit.name]["mongod_service"] = await time_process_started(
            ops_test, unit.name, DB_SERVICE
        )
        await check_certs_correctly_distributed(ops_test, unit)

    # restart times for mongod_service are in the format H:M, meaning that if we want to check that
    # the restart time is different, we have to ensure that a minute has passed.
    time.sleep(61)

    # set external and internal key using auto-generated key for each unit
    for unit in ops_test.model.applications[app_name].units:
        action = await unit.run_action(action_name="set-tls-private-key")
        action = await action.wait()
        assert action.status == "completed", "setting external and internal key failed."

    # wait for certificate to be available and processed. Larger than normal idle period so that
    # we guarantee that the charm receives + processes all events
    await ops_test.model.wait_for_idle(status="active", timeout=1000, idle_period=30)

    # After updating both the external key and the internal key a new certificate request will be
    # made; then the certificates should be available and updated.
    for unit in ops_test.model.applications[app_name].units:
        new_external_cert_time = await time_file_created(ops_test, unit.name, EXTERNAL_CERT_PATH)
        new_internal_cert_time = await time_file_created(ops_test, unit.name, INTERNAL_CERT_PATH)
        new_mongod_service_time = await time_process_started(ops_test, unit.name, DB_SERVICE)

        await check_certs_correctly_distributed(ops_test, unit, app_name=app_name)

        assert (
            new_external_cert_time > original_tls_times[unit.name]["external_cert"]
        ), f"external cert for {unit.name} was not updated."
        assert (
            new_internal_cert_time > original_tls_times[unit.name]["internal_cert"]
        ), f"internal cert for {unit.name} was not updated."

        # Once the certificate requests are processed and updated the mongod.service should be
        # restarted
        assert (
            new_mongod_service_time > original_tls_times[unit.name]["mongod_service"]
        ), f"mongod service for {unit.name} was not restarted."

    # Verify that TLS is functioning on all units.
    for unit in ops_test.model.applications[app_name].units:
        assert await check_tls(
            ops_test, unit, enabled=True
        ), f"tls is not enabled for {unit.name}."


@pytest.mark.group(1)
async def test_set_tls_key(ops_test: OpsTest) -> None:
    """Verify rotating tls private keys restarts mongod with new certificates.

    This test rotates tls private keys to user specified keys.
    """
    app_name = await get_app_name(ops_test)
    # dict of values for cert file certion and mongod service start times. After resetting the
    # private keys these certificates should be updated and the mongod service should be
    # restarted
    original_tls_times = {}
    for unit in ops_test.model.applications[app_name].units:
        original_tls_times[unit.name] = {}
        original_tls_times[unit.name]["external_cert"] = await time_file_created(
            ops_test, unit.name, EXTERNAL_CERT_PATH
        )
        original_tls_times[unit.name]["internal_cert"] = await time_file_created(
            ops_test, unit.name, INTERNAL_CERT_PATH
        )
        original_tls_times[unit.name]["mongod_service"] = await time_process_started(
            ops_test, unit.name, DB_SERVICE
        )

    # restart times for mongod_service are in the format H:M, meaning that if we want to check that
    # the restart time is different, we have to ensure that a minute has passed.
    time.sleep(61)

    with open(f"{TLS_TEST_DATA}/internal-key.pem") as f:
        internal_key_contents = f.readlines()
        internal_key_contents = "".join(internal_key_contents)

    # set external and internal key for each unit
    for unit_id in range(len(ops_test.model.applications[app_name].units)):
        unit = ops_test.model.applications[app_name].units[unit_id]

        with open(f"{TLS_TEST_DATA}/external-key-{unit_id}.pem") as f:
            external_key_contents = f.readlines()
            external_key_contents = "".join(external_key_contents)

        key_settings = {
            "internal-key": internal_key_contents,
            "external-key": external_key_contents,
        }

        action = await unit.run_action(
            action_name="set-tls-private-key",
            **key_settings,
        )
        action = await action.wait()
        assert action.status == "completed", "setting external and internal key failed."

    # wait for certificate to be available and processed. Larger than normal idle period so that
    # we guarantee that the charm receives + processes all events
    await ops_test.model.wait_for_idle(status="active", timeout=1000, idle_period=30)

    # After updating both the external key and the internal key a new certificate request will be
    # made; then the certificates should be available and updated.
    for unit in ops_test.model.applications[app_name].units:
        new_external_cert_time = await time_file_created(ops_test, unit.name, EXTERNAL_CERT_PATH)
        new_internal_cert_time = await time_file_created(ops_test, unit.name, INTERNAL_CERT_PATH)
        new_mongod_service_time = await time_process_started(ops_test, unit.name, DB_SERVICE)

        await check_certs_correctly_distributed(ops_test, unit)

        assert (
            new_external_cert_time > original_tls_times[unit.name]["external_cert"]
        ), f"external cert for {unit.name} was not updated."
        assert (
            new_internal_cert_time > original_tls_times[unit.name]["internal_cert"]
        ), f"internal cert for {unit.name} was not updated."

        # Once the certificate requests are processed and updated the mongod.service should be
        # restarted
        assert (
            new_mongod_service_time > original_tls_times[unit.name]["mongod_service"]
        ), f"mongod service for {unit.name} was not restarted."

    # Verify that TLS is functioning on all units.
    for unit in ops_test.model.applications[app_name].units:
        assert await check_tls(
            ops_test, unit, enabled=True
        ), f"tls is not enabled for {unit.name}."


@pytest.mark.group(1)
async def test_disable_tls(ops_test: OpsTest) -> None:
    """Verify each unit has TLS disabled after removing relation to the TLS application."""
    app_name = await get_app_name(ops_test)
    # Remove the relation.
    await ops_test.model.applications[app_name].remove_relation(
        f"{app_name}:certificates", f"{TLS_CERTIFICATES_APP_NAME}:certificates"
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app_name], status="active", timeout=1000)

    # Wait for all units disabling TLS.
    for unit in ops_test.model.applications[app_name].units:
        assert await check_tls(ops_test, unit, enabled=False)

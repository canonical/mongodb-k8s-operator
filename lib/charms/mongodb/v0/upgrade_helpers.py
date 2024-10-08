#!/usr/bin/env python3
"""Substrate agnostic manager for handling MongoDB in-place upgrades."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import abc
import copy
import enum
import json
import logging
import pathlib
import secrets
import string
from typing import Dict, List, Tuple

import poetry.core.constraints.version as poetry_version
from charms.mongodb.v0.mongo import MongoConfiguration
from charms.mongodb.v1.mongodb import FailedToMovePrimaryError, MongoDBConnection
from charms.mongodb.v1.mongos import MongosConnection
from ops import ActionEvent, BlockedStatus, MaintenanceStatus, StatusBase, Unit
from ops.charm import CharmBase
from ops.framework import Object
from pymongo.errors import OperationFailure, PyMongoError, ServerSelectionTimeoutError
from tenacity import RetryError, Retrying, retry, stop_after_attempt, wait_fixed

from config import Config

# The unique Charmhub library identifier, never change it
LIBID = "7f59c0d1c6f44d1993c57afa783e205c"


# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 3

logger = logging.getLogger(__name__)

SHARD_NAME_INDEX = "_id"
WRITE_KEY = "write_value"
ROLLBACK_INSTRUCTIONS = "To rollback, `juju refresh` to the previous revision"

PEER_RELATION_ENDPOINT_NAME = "upgrade-version-a"
RESUME_ACTION_NAME = "resume-refresh"
PRECHECK_ACTION_NAME = "pre-refresh-check"


# BEGIN: Helper functions
def unit_number(unit_: Unit) -> int:
    """Get unit number."""
    return int(unit_.name.split("/")[-1])


# END: Helper functions


# BEGIN: Exceptions
class StatusException(Exception):
    """Exception with ops status."""

    def __init__(self, status: StatusBase) -> None:
        super().__init__(status.message)
        self.status = status


class PrecheckFailed(StatusException):
    """App is not ready to upgrade."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(
            BlockedStatus(
                f"Rollback with `juju refresh`. Pre-refresh check failed: {self.message}"
            )
        )


class FailedToElectNewPrimaryError(Exception):
    """Raised when a new primary isn't elected after stepping down."""


class ClusterNotHealthyError(Exception):
    """Raised when the cluster is not healthy."""


class BalancerStillRunningError(Exception):
    """Raised when the balancer is still running after stopping it."""


class PeerRelationNotReady(Exception):
    """Upgrade peer relation not available (to this unit)."""


# END: Exceptions


class UnitState(str, enum.Enum):
    """Unit upgrade state."""

    HEALTHY = "healthy"
    RESTARTING = "restarting"  # Kubernetes only
    UPGRADING = "upgrading"  # Machines only
    OUTDATED = "outdated"  # Machines only


# BEGIN: Useful classes
class AbstractUpgrade(abc.ABC):
    """In-place upgrades abstract class (typing).

    Based off specification: DA058 - In-Place Upgrades - Kubernetes v2
    (https://docs.google.com/document/d/1tLjknwHudjcHs42nzPVBNkHs98XxAOT2BXGGpP7NyEU/)
    """

    def __init__(self, charm_: CharmBase) -> None:
        relations = charm_.model.relations[PEER_RELATION_ENDPOINT_NAME]
        if not relations:
            raise PeerRelationNotReady
        assert len(relations) == 1
        self._peer_relation = relations[0]
        self._charm = charm_
        self._unit: Unit = charm_.unit
        self._unit_databag = self._peer_relation.data[self._unit]
        self._app_databag = self._peer_relation.data[charm_.app]
        self._app_name = charm_.app.name
        self._current_versions = {}  # For this unit
        for version, file_name in {
            "charm": "charm_version",
            "workload": "workload_version",
        }.items():
            self._current_versions[version] = pathlib.Path(file_name).read_text().strip()

    @property
    def unit_state(self) -> UnitState | None:
        """Unit upgrade state."""
        if state := self._unit_databag.get("state"):
            return UnitState(state)

    @unit_state.setter
    def unit_state(self, value: UnitState) -> None:
        self._unit_databag["state"] = value.value

    @property
    def is_compatible(self) -> bool:
        """Whether upgrade is supported from previous versions."""
        assert self.versions_set
        try:
            previous_version_strs: Dict[str, str] = json.loads(self._app_databag["versions"])
        except KeyError as exception:
            logger.debug("`versions` missing from peer relation", exc_info=exception)
            return False
        # TODO charm versioning: remove `.split("+")` (which removes git hash before comparing)
        previous_version_strs["charm"] = previous_version_strs["charm"].split("+")[0]
        previous_versions: Dict[str, poetry_version.Version] = {
            key: poetry_version.Version.parse(value)
            for key, value in previous_version_strs.items()
        }
        current_version_strs = copy.copy(self._current_versions)
        current_version_strs["charm"] = current_version_strs["charm"].split("+")[0]
        current_versions = {
            key: poetry_version.Version.parse(value) for key, value in current_version_strs.items()
        }
        try:
            # TODO Future PR: change this > sign to support downgrades
            if (
                previous_versions["charm"] > current_versions["charm"]
                or previous_versions["charm"].major != current_versions["charm"].major
            ):
                logger.debug(
                    f'{previous_versions["charm"]=} incompatible with {current_versions["charm"]=}'
                )
                return False
            if (
                previous_versions["workload"] > current_versions["workload"]
                or previous_versions["workload"].major != current_versions["workload"].major
            ):
                logger.debug(
                    f'{previous_versions["workload"]=} incompatible with {current_versions["workload"]=}'
                )
                return False
            logger.debug(
                f"Versions before refresh compatible with versions after refresh {previous_version_strs=} {self._current_versions=}"
            )
            return True
        except KeyError as exception:
            logger.debug(f"Version missing from {previous_versions=}", exc_info=exception)
            return False

    @property
    def in_progress(self) -> bool:
        """Whether upgrade is in progress."""
        logger.debug(
            f"{self._app_workload_container_version=} {self._unit_workload_container_versions=}"
        )
        return any(
            version != self._app_workload_container_version
            for version in self._unit_workload_container_versions.values()
        )

    @property
    def _sorted_units(self) -> List[Unit]:
        """Units sorted from highest to lowest unit number."""
        return sorted((self._unit, *self._peer_relation.units), key=unit_number, reverse=True)

    @abc.abstractmethod
    def _get_unit_healthy_status(self) -> StatusBase:
        """Status shown during upgrade if unit is healthy."""
        raise NotImplementedError()

    def get_unit_juju_status(self) -> StatusBase | None:
        """Unit upgrade status."""
        if self.in_progress:
            return self._get_unit_healthy_status()

    @property
    def app_status(self) -> StatusBase | None:
        """App upgrade status."""
        if not self.in_progress:
            return
        if not self.upgrade_resumed:
            # User confirmation needed to resume upgrade (i.e. upgrade second unit)
            # Statuses over 120 characters are truncated in `juju status` as of juju 3.1.6 and
            # 2.9.45
            resume_string = ""
            if len(self._sorted_units) > 1:
                resume_string = (
                    f"Verify highest unit is healthy & run `{RESUME_ACTION_NAME}` action. "
                )
            return BlockedStatus(
                f"Refreshing. {resume_string}To rollback, `juju refresh` to last revision"
            )
        return MaintenanceStatus(
            "Refreshing. To rollback, `juju refresh` to the previous revision"
        )

    @property
    def versions_set(self) -> bool:
        """Whether versions have been saved in app databag.

        Should only be `False` during first charm install.

        If a user upgrades from a charm that does not set versions, this charm will get stuck.
        """
        return self._app_databag.get("versions") is not None

    def set_versions_in_app_databag(self) -> None:
        """Save current versions in app databag.

        Used after next upgrade to check compatibility (i.e. whether that upgrade should be
        allowed).
        """
        assert not self.in_progress
        logger.debug(f"Setting {self._current_versions=} in upgrade peer relation app databag")
        self._app_databag["versions"] = json.dumps(self._current_versions)
        logger.debug(f"Set {self._current_versions=} in upgrade peer relation app databag")

    @property
    @abc.abstractmethod
    def upgrade_resumed(self) -> bool:
        """Whether user has resumed upgrade with Juju action."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def _unit_workload_container_versions(self) -> Dict[str, str]:
        """{Unit name: unique identifier for unit's workload container version}.

        If and only if this version changes, the workload will restart (during upgrade or
        rollback).

        On Kubernetes, the workload & charm are upgraded together
        On machines, the charm is upgraded before the workload

        This identifier should be comparable to `_app_workload_container_version` to determine if
        the unit & app are the same workload container version.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def _app_workload_container_version(self) -> str:
        """Unique identifier for the app's workload container version.

        This should match the workload version in the current Juju app charm version.

        This identifier should be comparable to `_unit_workload_container_versions` to determine if
        the app & unit are the same workload container version.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def reconcile_partition(self, *, action_event: ActionEvent | None = None) -> None:
        """If ready, allow next unit to upgrade."""
        raise NotImplementedError()

    def pre_upgrade_check(self) -> None:
        """Check if this app is ready to upgrade.

        Runs before any units are upgraded

        Does *not* run during rollback

        On machines, this runs before any units are upgraded (after `juju refresh`)
        On machines & Kubernetes, this also runs during pre-upgrade-check action

        Can run on leader or non-leader unit

        Raises:
            PrecheckFailed: App is not ready to upgrade

        TODO Kubernetes: Run (some) checks after `juju refresh` (in case user forgets to run
        pre-upgrade-check action). Note: 1 unit will upgrade before we can run checks (checks may
        need to be modified).
        See https://chat.canonical.com/canonical/pl/cmf6uhm1rp8b7k8gkjkdsj4mya
        """
        logger.debug("Running pre-refresh checks")

        # TODO if shard is getting upgraded but BOTH have same revision, then fail
        try:
            self._charm.upgrade.wait_for_cluster_healthy()
        except RetryError:
            logger.error("Cluster is not healthy")
            raise PrecheckFailed("Cluster is not healthy")

        # On VM charms we can choose the order to upgrade, but not on K8s. In order to keep the
        # two charms in sync we decided to have the VM charm have the same upgrade order as the K8s
        # charm (i.e. highest to lowest.) Hence, we move the primary to the last unit to upgrade.
        # This prevents the primary from jumping around from unit to unit during the upgrade
        # procedure.
        try:
            self._charm.upgrade.move_primary_to_last_upgrade_unit()
        except FailedToMovePrimaryError:
            logger.error("Cluster failed to move primary before re-election.")
            raise PrecheckFailed("Primary switchover failed")

        if not self._charm.upgrade.is_cluster_able_to_read_write():
            logger.error("Cluster cannot read/write to replicas")
            raise PrecheckFailed("Cluster is not healthy")

        if self._charm.is_role(Config.Role.CONFIG_SERVER):
            if not self._charm.upgrade.are_pre_upgrade_operations_config_server_successful():
                raise PrecheckFailed("Pre-refresh operations on config-server failed.")


# END: Useful classes


class GenericMongoDBUpgrade(Object, abc.ABC):
    """Substrate agnostif, abstract handler for upgrade events."""

    def __init__(self, charm: CharmBase, *args, **kwargs):
        super().__init__(charm, *args, **kwargs)
        self._observe_events(charm)

    @abc.abstractmethod
    def _observe_events(self, charm: CharmBase) -> None:
        """Handler that should register all event observers."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def _upgrade(self) -> AbstractUpgrade | None:
        raise NotImplementedError()

    # BEGIN: Helpers
    def move_primary_to_last_upgrade_unit(self) -> None:
        """Moves the primary to last unit that gets upgraded (the unit with the lowest id).

        Raises FailedToMovePrimaryError
        """
        # no need to move primary in the scenario of one unit
        if len(self._upgrade._sorted_units) < 2:
            return

        with MongoDBConnection(self.charm.mongodb_config) as mongod:
            unit_with_lowest_id = self._upgrade._sorted_units[-1]
            if mongod.primary() == self.charm.unit_host(unit_with_lowest_id):
                logger.debug(
                    "Not moving Primary before refresh, primary is already on the last unit to refresh."
                )
                return

            logger.debug("Moving primary to unit: %s", unit_with_lowest_id)
            mongod.move_primary(new_primary_ip=self.charm.unit_host(unit_with_lowest_id))

    def wait_for_cluster_healthy(self) -> None:
        """Waits until the cluster is healthy after upgrading.

        After a unit restarts it can take some time for the cluster to settle.

        Raises:
            ClusterNotHealthyError.
        """
        for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(1)):
            with attempt:
                if not self.is_cluster_healthy():
                    raise ClusterNotHealthyError()

    def is_cluster_healthy(self) -> bool:
        """Returns True if all nodes in the cluster/replcia set are healthy."""
        # TODO: check mongos
        with MongoDBConnection(
            self.charm.mongodb_config, "localhost", direct=True
        ) as direct_mongo:
            if not direct_mongo.is_ready:
                logger.error("Cannot proceed with refresh. Service mongod is not running")
                return False

        # It is possible that in a previous run of post-upgrade-check, that the unit was set to
        # unhealthy. In order to check if this unit has resolved its issue, we ignore the status
        # that was set in a previous check of cluster health. Otherwise, we are stuck in an
        # infinite check of cluster health due to never being able to reset an unhealthy status.
        if not self.charm.status.is_current_unit_ready(
            ignore_unhealthy_upgrade=True
        ) or not self.charm.status.are_all_units_ready_for_upgrade(
            unit_to_ignore=self.charm.unit.name
        ):
            logger.error(
                "Cannot proceed with refresh. Status of charm units do not show active / waiting for refresh."
            )
            return False

        if self.charm.is_role(Config.Role.CONFIG_SERVER):
            if not self.charm.status.are_shards_status_ready_for_upgrade():
                logger.error(
                    "Cannot proceed with refresh. Status of shard units do not show active / waiting for refresh."
                )
                return False

        try:
            return self.are_nodes_healthy()
        except (PyMongoError, OperationFailure, ServerSelectionTimeoutError) as e:
            logger.error(
                "Cannot proceed with refresh. Failed to check cluster health, error: %s", e
            )
            return False

    def are_nodes_healthy(self) -> bool:
        """Returns true if all nodes in the MongoDB deployment are healthy."""
        if self.charm.is_role(Config.Role.REPLICATION):
            return self.are_replica_set_nodes_healthy(self.charm.mongodb_config)

        mongos_config = self.get_cluster_mongos()
        if not self.are_shards_healthy(mongos_config):
            logger.debug(
                "One or more individual shards are not healthy - do not proceed with refresh."
            )
            return False

        if not self.are_replicas_in_sharded_cluster_healthy(mongos_config):
            logger.debug("One or more nodes are not healthy - do not proceed with refresh.")
            return False

        return True

    def are_replicas_in_sharded_cluster_healthy(self, mongos_config: MongoConfiguration) -> bool:
        """Returns True if all replicas in the sharded cluster are healthy."""
        # dictionary of all replica sets in the sharded cluster
        for mongodb_config in self.get_all_replica_set_configs_in_cluster():
            if not self.are_replica_set_nodes_healthy(mongodb_config):
                logger.debug(f"Replica set: {mongodb_config.replset} contains unhealthy nodes.")
                return False

        return True

    def are_shards_healthy(self, mongos_config: MongoConfiguration) -> bool:
        """Returns True if all shards in the cluster are healthy."""
        with MongosConnection(mongos_config) as mongos:
            if mongos.is_any_draining():
                logger.debug("Cluster is draining a shard, do not proceed with refresh.")
                return False

            if not mongos.are_all_shards_aware():
                logger.debug("Not all shards are shard aware, do not proceed with refresh.")
                return False

            # Config-Server has access to all the related shard applications.
            if self.charm.is_role(Config.Role.CONFIG_SERVER):
                relation_shards = self.charm.config_server.get_shards_from_relations()
                cluster_shards = mongos.get_shard_members()
                if len(relation_shards - cluster_shards):
                    logger.debug(
                        "Not all shards have been added/drained, do not proceed with refresh."
                    )
                    return False

        return True

    def get_all_replica_set_configs_in_cluster(self) -> List[MongoConfiguration]:
        """Returns a list of all the mongodb_configurations for each application in the cluster."""
        mongos_config = self.get_cluster_mongos()
        mongodb_configurations = []
        if self.charm.is_role(Config.Role.SHARD):
            # the hosts of the integrated mongos application are also the config-server hosts
            config_server_hosts = self.charm.shard.get_mongos_hosts()
            mongodb_configurations = [
                self.charm.remote_mongodb_config(
                    config_server_hosts, replset=self.charm.shard.get_config_server_name()
                )
            ]
        elif self.charm.is_role(Config.Role.CONFIG_SERVER):
            mongodb_configurations = [self.charm.mongodb_config]

        with MongosConnection(mongos_config) as mongos:
            sc_status = mongos.client.admin.command("listShards")
            for shard in sc_status["shards"]:
                mongodb_configurations.append(self.get_mongodb_config_from_shard_entry(shard))

        return mongodb_configurations

    def are_replica_set_nodes_healthy(self, mongodb_config: MongoConfiguration) -> bool:
        """Returns true if all nodes in the MongoDB replica set are healthy."""
        with MongoDBConnection(mongodb_config) as mongod:
            rs_status = mongod.get_replset_status()
            rs_status = mongod.client.admin.command("replSetGetStatus")
            return not mongod.is_any_sync(rs_status)

    def is_cluster_able_to_read_write(self) -> bool:
        """Returns True if read and write is feasible for cluster."""
        if self.charm.is_role(Config.Role.REPLICATION):
            return self.is_replica_set_able_read_write()
        else:
            return self.is_sharded_cluster_able_to_read_write()

    def is_sharded_cluster_able_to_read_write(self) -> bool:
        """Returns True if possible to write all cluster shards and read from all replicas."""
        mongos_config = self.get_cluster_mongos()
        with MongosConnection(mongos_config) as mongos:
            sc_status = mongos.client.admin.command("listShards")
            for shard in sc_status["shards"]:
                # force a write to a specific shard to ensure the primary on that shard can
                # receive writes
                db_name, collection_name, write_value = self.get_random_write_and_collection()
                self.add_write_to_sharded_cluster(
                    mongos_config, db_name, collection_name, write_value
                )
                mongos.client.admin.command("movePrimary", db_name, to=shard[SHARD_NAME_INDEX])

                write_replicated = self.is_write_on_secondaries(
                    self.get_mongodb_config_from_shard_entry(shard),
                    collection_name,
                    write_value,
                    db_name,
                )

                self.clear_db_collection(mongos_config, db_name)
                if not write_replicated:
                    logger.debug(f"Test read/write to shard {shard['_id']} failed.")
                    return False

        return True

    def get_mongodb_config_from_shard_entry(self, shard_entry: dict) -> MongoConfiguration:
        """Returns a replica set MongoConfiguration based on a shard entry from ListShards."""
        # field hosts is of the form shard01/host1:27018,host2:27018,host3:27018
        shard_hosts = shard_entry["host"].split("/")[1]
        parsed_ips = [host.split(":")[0] for host in shard_hosts.split(",")]
        return self.charm.remote_mongodb_config(parsed_ips, replset=shard_entry[SHARD_NAME_INDEX])

    def get_cluster_mongos(self) -> MongoConfiguration:
        """Return a mongos configuration for the sharded cluster."""
        return (
            self.charm.mongos_config
            if self.charm.is_role(Config.Role.CONFIG_SERVER)
            else self.charm.remote_mongos_config(self.charm.shard.get_mongos_hosts())
        )

    def is_replica_set_able_read_write(self) -> bool:
        """Returns True if is possible to write to primary and read from replicas."""
        _, collection_name, write_value = self.get_random_write_and_collection()
        self.add_write_to_replica_set(self.charm.mongodb_config, collection_name, write_value)
        write_replicated = self.is_write_on_secondaries(
            self.charm.mongodb_config, collection_name, write_value
        )
        self.clear_tmp_collection(self.charm.mongodb_config, collection_name)
        return write_replicated

    def clear_db_collection(self, mongos_config: MongoConfiguration, db_name: str) -> None:
        """Clears the temporary collection."""
        with MongoDBConnection(mongos_config) as mongos:
            mongos.client.drop_database(db_name)

    def clear_tmp_collection(
        self, mongodb_config: MongoConfiguration, collection_name: str
    ) -> None:
        """Clears the temporary collection."""
        with MongoDBConnection(mongodb_config) as mongod:
            db = mongod.client["admin"]
            db.drop_collection(collection_name)

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_fixed(1),
        reraise=True,
    )
    def confirm_excepted_write_on_replica(
        self,
        host: str,
        db_name: str,
        collection: str,
        expected_write_value: str,
        secondary_config: MongoConfiguration,
    ) -> bool:
        """Returns True if the replica contains the expected write in the provided collection."""
        secondary_config.hosts = {host}
        with MongoDBConnection(secondary_config, direct=True) as direct_seconary:
            db = direct_seconary.client[db_name]
            test_collection = db[collection]
            query = test_collection.find({}, {WRITE_KEY: 1})
            if query[0][WRITE_KEY] != expected_write_value:
                raise ClusterNotHealthyError

    def get_random_write_and_collection(self) -> Tuple[str, str]:
        """Returns a tuple for a random collection name and a unique write to add to it."""
        choices = string.ascii_letters + string.digits
        collection_name = "collection_" + "".join([secrets.choice(choices) for _ in range(32)])
        write_value = "unique_write_" + "".join([secrets.choice(choices) for _ in range(16)])
        db_name = "db_name_" + "".join([secrets.choice(choices) for _ in range(32)])
        return (db_name, collection_name, write_value)

    def add_write_to_sharded_cluster(
        self, mongos_config: MongoConfiguration, db_name, collection_name, write_value
    ) -> None:
        """Adds a the provided write to the provided database with the provided collection."""
        with MongoDBConnection(mongos_config) as mongod:
            db = mongod.client[db_name]
            test_collection = db[collection_name]
            write = {WRITE_KEY: write_value}
            test_collection.insert_one(write)

    def add_write_to_replica_set(
        self, mongodb_config: MongoConfiguration, collection_name, write_value
    ) -> None:
        """Adds a the provided write to the admin database with the provided collection."""
        with MongoDBConnection(mongodb_config) as mongod:
            db = mongod.client["admin"]
            test_collection = db[collection_name]
            write = {WRITE_KEY: write_value}
            test_collection.insert_one(write)

    def is_write_on_secondaries(
        self,
        mongodb_config: MongoConfiguration,
        collection_name,
        expected_write_value,
        db_name: str = "admin",
    ):
        """Returns true if the expected write."""
        for replica_ip in mongodb_config.hosts:
            try:
                self.confirm_excepted_write_on_replica(
                    replica_ip, db_name, collection_name, expected_write_value, mongodb_config
                )
            except ClusterNotHealthyError:
                # do not return False immediately - as it is
                logger.debug("Secondary with IP %s, does not contain the expected write.")
                return False

        return True

    def step_down_primary_and_wait_reelection(self) -> None:
        """Steps down the current primary and waits for a new one to be elected."""
        if len(self.charm.mongodb_config.hosts) < 2:
            logger.warning(
                "No secondaries to become primary - upgrading primary without electing a new one, expect downtime."
            )
            return

        old_primary = self.charm.primary
        with MongoDBConnection(self.charm.mongodb_config) as mongod:
            mongod.step_down_primary()

        for attempt in Retrying(stop=stop_after_attempt(30), wait=wait_fixed(1), reraise=True):
            with attempt:
                new_primary = self.charm.primary
                if new_primary == old_primary:
                    raise FailedToElectNewPrimaryError()

    def are_pre_upgrade_operations_config_server_successful(self):
        """Runs pre-upgrade operations for config-server and returns True if successful."""
        if not self.charm.is_role(Config.Role.CONFIG_SERVER):
            return False

        if not self.is_feature_compatibility_version(Config.Upgrade.FEATURE_VERSION_6):
            logger.debug(
                "Not all replicas have the expected feature compatibility: %s",
                Config.Upgrade.FEATURE_VERSION_6,
            )
            return False

        self.set_mongos_feature_compatibilty_version(Config.Upgrade.FEATURE_VERSION_6)

        # pre-upgrade sequence runs twice. Once when the user runs the pre-upgrade action and
        # again automatically on refresh (just in case the user forgot to). Disabling the balancer
        # can negatively impact the cluster, so we only disable it once the upgrade sequence has
        # begun.
        if self._upgrade and self._upgrade.in_progress:
            try:
                self.turn_off_and_wait_for_balancer()
            except BalancerStillRunningError:
                logger.debug("Balancer is still running. Please try the pre-refresh check later.")
                return False

        return True

    def is_feature_compatibility_version(self, expected_feature_version) -> bool:
        """Returns True if all nodes in the sharded cluster have the expected_feature_version.

        Note it is NOT sufficient to check only mongos or the individual shards. It is necessary to
        check each node according to MongoDB upgrade docs.
        """
        for replica_set_config in self.get_all_replica_set_configs_in_cluster():
            for single_host in replica_set_config.hosts:
                single_replica_config = self.charm.remote_mongodb_config(
                    single_host, replset=replica_set_config.replset, standalone=True
                )
                with MongoDBConnection(single_replica_config) as mongod:
                    version = mongod.client.admin.command(
                        ({"getParameter": 1, "featureCompatibilityVersion": 1})
                    )
                    if (
                        version["featureCompatibilityVersion"]["version"]
                        != expected_feature_version
                    ):
                        return False

        return True

    def set_mongos_feature_compatibilty_version(self, feature_version) -> None:
        """Sets the mongos feature compatibility version."""
        with MongosConnection(self.charm.mongos_config) as mongos:
            mongos.client.admin.command("setFeatureCompatibilityVersion", feature_version)

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_fixed(1),
        reraise=True,
    )
    def turn_off_and_wait_for_balancer(self):
        """Sends the stop command to the balancer and wait for it to stop running."""
        with MongosConnection(self.charm.mongos_config) as mongos:
            mongos.client.admin.command("balancerStop")
            balancer_state = mongos.client.admin.command("balancerStatus")
            if balancer_state["mode"] != "off":
                raise BalancerStillRunningError("balancer is still Running.")

    # END: helpers

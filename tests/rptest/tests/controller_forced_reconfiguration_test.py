# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
from enum import Enum
from random import shuffle
from rptest.clients.kcl import KCL
from typing import Any, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import PartitionDetails, Replica
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.partition_movement import PartitionMovementMixin

from rptest.util import wait_until_result

from connectrpc.unary import UnaryOutput
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.admin.proto.redpanda.core.admin.v2.internal import (
    breakglass_pb2,
    breakglass_pb2_connect,
)


@dataclass
class NTP:
    namespace: str = "kafka"
    topic: str = "topic"
    partition: int = 0


@dataclass
class TimeoutConfig:
    timeout_s: int
    backoff_s: int

class Scenario(str, Enum):
    Simple = "Simple"
    Decommission = "Decommission"
    RandomMoves = "RandomMoves"


really_short_timeout = TimeoutConfig(timeout_s=5, backoff_s=1)
short_timeout = TimeoutConfig(timeout_s=30, backoff_s=2)
medium_timeout = TimeoutConfig(timeout_s=60, backoff_s=2)
long_timeout = TimeoutConfig(timeout_s=120, backoff_s=10)
really_long_timeout = TimeoutConfig(timeout_s=300, backoff_s=10)


class ControllerForceReconfigurationTestBase(RedpandaTest):
    def __init__(
        self, test_context: TestContext, cluster_size: int, *args: Any, **kwargs: Any
    ):
        super(ControllerForceReconfigurationTestBase, self).__init__(
            test_context,
            num_brokers=cluster_size,
            *args,
            **kwargs,
        )
        self.next_node_id = cluster_size + 1

    def _next_node_id(self) -> int:
        """this test kills nodes, cleans them, then reboots with a new node_id, keep track of the node id"""
        next = self.next_node_id
        self.next_node_id += 1
        return next

    def setUp(self):
        """rp will be custom started in each test"""
        pass

    def _start_redpanda(self, cluster_size: int) -> list[ClusterNode]:
        """start redpanda with a specific cluster size"""
        seed_nodes = self.redpanda.nodes[0:cluster_size]
        joiner_nodes = self.redpanda.nodes[cluster_size:]

        self.redpanda.set_seed_servers(seed_nodes)
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)
        return joiner_nodes

    def _setup_topic(self, topic_spec: TopicSpec, timeout: TimeoutConfig):
        """start a topic given the spec"""
        self.client().create_topic(topic_spec)
        # Wait for initial leader
        self.redpanda._admin.await_stable_leader(
            topic=topic_spec.name,
            replication=topic_spec.replication_factor,
            timeout_s=timeout.timeout_s,
            backoff_s=timeout.backoff_s,
        )

    def _living_nodes(self) -> list[ClusterNode]:
        return self.redpanda.started_nodes()

    def _living_hostnames(self) -> list[str]:
        hostnames: list[str] = []
        node: ClusterNode
        for node in self.redpanda.started_nodes():
            hostname = node.account.hostname
            assert hostname is not None
            hostnames.append(hostname)
        return hostnames

    def _wait_until_no_leader(self, ntp: NTP, timeout: TimeoutConfig):
        """Scrapes the debug endpoints of all replicas and checks if any of the replicas think they are the leader"""

        def no_leader():
            living_nodes = self._living_nodes()
            for living_node in living_nodes:
                state = self.redpanda._admin.get_partition_state(
                    ntp.namespace, ntp.topic, ntp.partition, node=living_node
                )
                if "replicas" not in state.keys() or len(state["replicas"]) == 0:
                    continue
                for r in state["replicas"]:
                    assert "raft_state" in r.keys()
                    if r["raft_state"]["is_leader"]:
                        return False
            return True

        wait_until(
            no_leader,
            timeout_sec=timeout.timeout_s,
            backoff_sec=timeout.backoff_s,
            err_msg="Partition has a leader",
        )

    def _split_cluster(
        self, ntp: NTP, timeout: TimeoutConfig, replication: int = 5
    ) -> tuple[list[Replica], list[Replica]]:
        """
        Splits the cluster into nodes to kill and nodes to survive
        """
        assert self.redpanda

        def _get_details() -> tuple[bool, Optional[PartitionDetails]]:
            d = self.redpanda._admin._get_stable_configuration(
                hosts=self._living_hostnames(),
                namespace=ntp.namespace,
                topic=ntp.topic,
                partition=ntp.partition,
                replication=replication,
            )
            if d is None:
                return (False, None)
            return (True, d)

        partition_details: PartitionDetails = wait_until_result(
            _get_details, timeout_sec=timeout.timeout_s, backoff_sec=timeout.backoff_s
        )

        replicas = partition_details.replicas
        shuffle(replicas)
        mid = len(replicas) // 2 + 1
        (to_kill, to_survive) = (replicas[0:mid], replicas[mid:])
        return (to_kill, to_survive)

    def _do_stop_nodes(self, ntp: NTP, to_kill: list[Replica], timeout: TimeoutConfig):
        """ingests the output of _split_cluster, actually stops those nodes"""
        for replica in to_kill:
            node = self.redpanda.get_node_by_id(replica.node_id)
            assert node
            self.logger.debug(f"Stopping node with node_id: {replica.node_id}")
            self.redpanda.stop_node(node)
        # The partition should be leaderless.
        self._wait_until_no_leader(ntp=ntp, timeout=timeout)

    def _stop_majority_nodes(
        self, ntp: NTP, timeout: TimeoutConfig, replication: int = 5
    ) -> tuple[list[Replica], list[Replica]]:
        """chains together the above two, split the cluster then kill the majority"""
        killed, alive = self._split_cluster(
            ntp=ntp, timeout=timeout, replication=replication
        )
        self._do_stop_nodes(ntp=ntp, to_kill=killed, timeout=timeout)
        return (killed, alive)

    def _toggle_recovery_mode(
        self, node: ClusterNode, timeout: TimeoutConfig, recovery_mode_enabled: bool
    ):
        """reboot a node with recovery mode set accordingly"""
        self.redpanda.nodes
        self.logger.info(f"stopping node: {node.name}")
        self.redpanda.stop_node(node, timeout=timeout.timeout_s)

        self.logger.info(f"restarting node: {node.name}")
        self.redpanda.start_node(
            node,
            timeout=timeout.timeout_s,
            auto_assign_node_id=True,
            override_cfg_params={"recovery_mode_enabled": recovery_mode_enabled},
        )

    def _bulk_toggle_recovery_mode(
        self,
        nodes: list[ClusterNode],
        timeout: TimeoutConfig,
        recovery_mode_enabled: bool,
    ):
        """bulk toggle recovery mode"""
        for node in nodes:
            self._toggle_recovery_mode(node, timeout, recovery_mode_enabled)

    def _do_request(
        self,
        client: breakglass_pb2_connect.BreakglassServiceClient,
        request: breakglass_pb2.ControllerForcedReconfigurationRequest,
    ) -> UnaryOutput[breakglass_pb2.ControllerForcedReconfigurationResponse]:
        """helper method to do a cfr request, handles the typing concerns"""
        return client.call_controller_forced_reconfiguration(request)  # type: ignore

    def _join_new_node(self, joiner_node: ClusterNode) -> int:
        """joins a given cluster node with a new node id"""
        self.redpanda.logger.debug(f"joining {joiner_node.name=}")
        self.redpanda.clean_node(
            joiner_node, preserve_logs=True, preserve_current_install=True
        )
        joiner_node_id = self._next_node_id()
        self.redpanda.logger.debug(f"assigned {joiner_node_id=} to {joiner_node.name=}")
        self.redpanda.start_node(
            joiner_node,
            auto_assign_node_id=False,
            node_id_override=joiner_node_id,
            omit_seeds_on_idx_one=True,
        )
        wait_until(
            lambda: self.redpanda.registered(joiner_node),
            timeout_sec=120,
            backoff_sec=5,
        )
        return joiner_node_id

    def _check_tp_recovered(
        self,
        node: ClusterNode,
        ntp: NTP,
        replication_factor: int,
        killed_node_ids: list[int],
    ) -> bool:
        state = self.redpanda._admin.get_partition_state(
            ntp.namespace,
            ntp.topic,
            ntp.partition,
            node=node,
        )
        self.redpanda.logger.debug(
            f"_check_tp_recovered: node: {node.name} waiting for recovery of {ntp=}, found {state=}"
        )

        leader_raft_state: Any = None
        for replica in state["replicas"]:
            raft_state = replica["raft_state"]
            if raft_state["is_leader"]:
                leader_raft_state = raft_state
                break

        if not leader_raft_state:
            self.redpanda.logger.debug(f"_check_tp_recovered: no leader yet for {ntp=}")
            return False

        nodes: list[int] = []
        nodes.append(leader_raft_state["node_id"])

        # get all followers that are NOT learners
        for follower in leader_raft_state["followers"]:
            if not follower["is_learner"]:
                nodes.append(follower["id"])

        if len(nodes) != replication_factor:
            self.redpanda.logger.debug(
                f"_check_tp_recovered: expected group of size: {replication_factor}, but found {len(nodes)}"
            )
            return False

        for killed_node_id in killed_node_ids:
            if killed_node_id in nodes:
                self.redpanda.logger.debug(
                    f"_check_tp_recovered: dead node: {killed_node_id} still in configuration"
                )
                return False

        self.redpanda.logger.debug(
            f"_check_tp_recovered: success for node: {node.name} {ntp=}"
        )
        return True

    def _check_topic_recovered(self, topic: TopicSpec, killed_node_ids: list[int]):
        """checks that a topic recovered, meaning it has the correct amount of followers which are NOT learners. Checks that no topic is hosted on a dead node"""
        for partition in range(0, topic.partition_count):
            ntp = NTP(topic=topic.name, partition=partition)
            for living_node in self._living_nodes():
                wait_until(
                    lambda: self._check_tp_recovered(
                        node=living_node,
                        ntp=ntp,
                        replication_factor=topic.replication_factor,
                        killed_node_ids=killed_node_ids,
                    ),
                    timeout_sec=120,
                    backoff_sec=1,
                )
        return True

    def _no_majority_lost_partitions(
        self, node: ClusterNode, dead_node_ids: list[int], timeout: TimeoutConfig
    ) -> bool:
        def controller_available() -> bool:
            controller = self.redpanda.controller()
            return controller is not None and self.redpanda.node_id(controller)

        try:
            wait_until(
                controller_available,
                timeout_sec=timeout.timeout_s,
                backoff_sec=timeout.backoff_s,
                err_msg="Controller not available",
            )
            lost_majority = (
                self.redpanda._admin.get_majority_lost_partitions_from_nodes(
                    dead_brokers=dead_node_ids, node=node, timeout=3
                )
            )
            self.redpanda.logger.debug(
                f"Partitions with lost majority: {lost_majority}"
            )
            return len(lost_majority) == 0
        except Exception as e:
            self.redpanda.logger.debug(e, exc_info=True)
            return False

    def _pin_ntp_brokers(self, ntp: NTP, assignments: list[int]):
        ''' use the admin api to move a partition '''
        INVALID_CORE = 12121212
        self.redpanda.logger.info(f"setting assignments for {ntp=}: {assignments=}")

        self.redpanda._admin.set_partition_replicas(
            namespace=ntp.namespace,
            topic=ntp.topic,
            partition=ntp.partition,
            replicas=[
                {
                    "node_id": a,
                    "core": INVALID_CORE,
                }
                for a in assignments
            ],
        )

    def _pin_partition_to_dying_brokers(
        self, dead_node_ids: list[int], topic: TopicSpec
    ):
        """this will pin at least one partition to be completely lost in the cluster breakdown"""
        kcl = KCL(self.redpanda)
        assert len(dead_node_ids) >= topic.replication_factor, (
            "can't fully lose a partition which has greater replication than dead brokers"
        )
        node_ids_to_pin = dead_node_ids[0 : topic.replication_factor]
        # pin partition 0
        p0_pinning: dict[int, list[int]] = {0: node_ids_to_pin}
        topic_pinning: dict[str, dict[int, list[int]]] = {topic.name: p0_pinning}
        kcl.alter_partition_reassignments(topics=topic_pinning)

    def _pin_partition_to_living_brokers(
        self,
        living_node_ids: list[int],
        dead_node_ids: list[int],
        ntp: NTP,
        replication_factor: int,
    ):
        """
        pins the partition as much as possible to living brokers
        this is primarily to prevent total replica loss on partitions such as producer_id which
        would make tests remarkably hard to execute
        """
        # might be more replication factor than available living brokers
        living_node_count = min(len(living_node_ids), replication_factor)
        nodes_to_pin = living_node_ids[0:living_node_count]
        if living_node_count < replication_factor:
            dead_node_count = replication_factor - living_node_count
            nodes_to_pin.extend(dead_node_ids[0:dead_node_count])
        assert len(nodes_to_pin) >= replication_factor, (
            f"not enough nodes found on which to pin, expected {replication_factor=}, found {nodes_to_pin=}"
        )
        self._pin_ntp_brokers(ntp, nodes_to_pin)


class ControllerForcedReconfiguration_SmokeTest(ControllerForceReconfigurationTestBase, PartitionMovementMixin):
    cluster_size: int = 3

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super(ControllerForcedReconfiguration_SmokeTest, self).__init__(
            test_context,
            cluster_size=ControllerForcedReconfiguration_SmokeTest.cluster_size,
            *args,
            **kwargs,
        )

    @cluster(num_nodes=4)
    def test_smoke_cfr(self):
        """
        1. create a cluster of size three
        2. add a topic and produce to it
        3. fail the majority of nodes in the cluster
        4. reboot into recovery mode
        5. force reconfigure the cluster to the remaining survivor
        6. add new brokers back to three
        7. reboot into normal mode
        8. produce to topic
        9. check that all partitions on topic have voter set of 3
        """
        admin = AdminV2(self.redpanda)

        # will start a cluster of 3 nodes on 1, 2, 3
        cluster_size: int = 3
        _ = self._start_redpanda(cluster_size=cluster_size)

        controller_ntp = NTP(namespace="redpanda", topic="controller", partition=0)
        short_timeout = TimeoutConfig(timeout_s=30, backoff_s=2)

        topic = TopicSpec(
            replication_factor=3,
            partition_count=1,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )

        self.client().create_topic(topic)

        KgoVerifierProducer.oneshot(  # type: ignore
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )

        killed, living = self._stop_majority_nodes(
            ntp=controller_ntp, timeout=short_timeout, replication=cluster_size
        )

        killed_node_ids = [dead_node.node_id for dead_node in killed]

        self.redpanda.logger.debug(f"killed nodes: {killed}, living nodes: {living}")

        designated_survivors = self._living_nodes()
        assert len(designated_survivors) == 1, (
            f"found too many living expected 1 found: {len(designated_survivors)}"
        )
        designated_survivor = designated_survivors[0]

        self._toggle_recovery_mode(
            node=designated_survivor,
            timeout=TimeoutConfig(timeout_s=60, backoff_s=2),
            recovery_mode_enabled=True,
        )

        self.redpanda.logger.debug("beginning CFR request")
        breakgass_client = admin.breakglass(node=designated_survivor)
        request = breakglass_pb2.ControllerForcedReconfigurationRequest(
            dead_node_ids=killed_node_ids, surviving_node_count=1
        )
        result = self._do_request(breakgass_client, request)
        self.redpanda.logger.debug("CFR request finished")

        error = result.error()
        assert error is None, f"CFR request failed with error {result.error()}"

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in killed_node_ids
            )

        self.redpanda.logger.debug("waiting for controller to recover")
        wait_until(
            lambda: controller_available(),
            timeout_sec=really_long_timeout.timeout_s,
            backoff_sec=really_long_timeout.backoff_s,
            err_msg=f"Controller never came back",
        )
        self.redpanda.logger.debug("controller recovered")

        # these nodes will rejoin with new node-ids
        for joiner_node_id in killed_node_ids:
            joiner_node = self.redpanda.get_node_by_id(joiner_node_id)
            self.redpanda.logger.debug(f"joining node {joiner_node.name}")
            _ = self._join_new_node(joiner_node)

        self._toggle_recovery_mode(
            node=designated_survivor,
            timeout=TimeoutConfig(timeout_s=60, backoff_s=2),
            recovery_mode_enabled=False,
        )
        wait_until(
            lambda: self._check_topic_recovered(
                topic=topic, killed_node_ids=killed_node_ids
            ),
            timeout_sec=really_long_timeout.timeout_s,
            backoff_sec=really_long_timeout.backoff_s,
            retry_on_exc=True,
        )

        KgoVerifierProducer.oneshot(  # type: ignore
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )


class ControllerForcedReconfiguration_DecommissionTest(
    ControllerForceReconfigurationTestBase,
    PartitionMovementMixin
):
    '''
    This is a set of tests for controller forced reconfiguration which make sense for clusters 5+
    Namely, what happens if a broker is decomissioning

    '''
    cluster_size: int = 5

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super(ControllerForcedReconfiguration_DecommissionTest, self).__init__(
            test_context,
            cluster_size=ControllerForcedReconfiguration_DecommissionTest.cluster_size,
            *args,
            **kwargs,
        )

    @cluster(num_nodes=6)
    @matrix(scenario=[Scenario.Simple, Scenario.RandomMoves, Scenario.Decommission])
    def test_cluster_recovery_with_decommed_broker(self, scenario: Scenario):
        """constants"""
        cluster_size: int = 5
        controller_ntp = NTP(namespace="redpanda", topic="controller", partition=0)
        topic = TopicSpec(
            replication_factor=3,
            partition_count=3,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )
        producer_id_ntp = NTP(
            namespace="kafka_internal", topic="id_allocator", partition=0
        )

        """ bootstrap """
        admin = AdminV2(self.redpanda)
        _ = self._start_redpanda(cluster_size=cluster_size)
        self.client().create_topic(topic)

        """ start with some data in the topic"""
        KgoVerifierProducer.oneshot(  # type: ignore
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )

        """ divide the cluster into a majority which will be destroyed, and a minority which will survive """
        to_kill, living = self._split_cluster(
            ntp=controller_ntp, timeout=short_timeout, replication=cluster_size
        )
        # derived lists for convenience
        killed_node_ids = [dead_node.node_id for dead_node in to_kill]
        living_node_ids = [living_node.node_id for living_node in living]
        killed_cluster_nodes = [
            self.redpanda.get_node_by_id(dead_node.node_id) for dead_node in to_kill
        ]
        designated_survivors = [
            self.redpanda.get_node_by_id(living_node_id)
            for living_node_id in living_node_ids
        ]

        """ pin at least one partition in the data topic to be entirely killed """
        self._pin_partition_to_dying_brokers(dead_node_ids=killed_node_ids, topic=topic)
        """ if the internal producer id partition entirely dies, its really hard to use KGO, for the purpose of testing, make sure this partiton does not have total loss """
        self._pin_partition_to_living_brokers(
            living_node_ids=living_node_ids,
            dead_node_ids=killed_node_ids,
            ntp=producer_id_ntp,
            replication_factor=3,
        )

        # jam recovery
        self.redpanda.set_cluster_config({"raft_learner_recovery_rate": 1}, timeout=medium_timeout.timeout_s)
        if scenario == Scenario.Decommission:
            decommissioned_node_id = living[0].node_id
            self.redpanda._admin.decommission_broker(decommissioned_node_id)
        if scenario == Scenario.RandomMoves:
            try: 
                _metadata = self.client().describe_topics()
                _topic, _partition = self._random_partition(_metadata)
                self.redpanda.logger.debug(f"executing random partition move on {_topic}/{_partition}")
                self._dispatch_random_partition_move(_topic, _partition)
            except Exception as e:
                self.redpanda.logger.info(f"failed to execute random partition move with exception {e}")

        """
        the meat of the test
        1. kill the majority of nodes
        2. reboot the survivors into recovery mode
        3. foreach run CFR
        4. wait for controller restoration
        5. recycle the dead nodes as newly joined nodes to restore the cluster to original size
        6. reboot ALL nodes not in recovery mode, and wait for controller restoration
        """
        self._do_stop_nodes(ntp=controller_ntp, to_kill=to_kill, timeout=short_timeout)

        self.redpanda.logger.debug(
            f"killed nodes: {killed_node_ids}, living nodes: {living_node_ids}"
        )

        assert len(designated_survivors) == len(living), (
            f"found too many living expected {len(living)} found: {len(designated_survivors)}"
        )

        self._bulk_toggle_recovery_mode(
            nodes=designated_survivors,
            timeout=medium_timeout,
            recovery_mode_enabled=True,
        )

        self.redpanda.logger.debug("beginning CFR requests")
        for survivor in designated_survivors:
            self.redpanda.logger.debug(f"cfr on node {survivor.name}")
            breakgass_client = admin.breakglass(node=survivor)
            request = breakglass_pb2.ControllerForcedReconfigurationRequest(
                dead_node_ids=killed_node_ids,
                surviving_node_count=len(designated_survivors),
            )
            result = self._do_request(breakgass_client, request)
            self.redpanda.logger.debug("CFR request finished")

            error = result.error()
            if error is not None:
                # this happens when there are multiple candidate leaders
                # and one wins the election before all cfr requests have been finished
                if "use the existing controller leader" in error.message:
                    continue
                assert False, f"CFR request failed with error {result.error()}"

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in killed_node_ids
            )

        self.redpanda.logger.debug("waiting for controller to recover")
        recovery_timeout = TimeoutConfig(timeout_s=240, backoff_s=10)
        wait_until(
            lambda: controller_available(),
            timeout_sec=recovery_timeout.timeout_s,
            backoff_sec=recovery_timeout.backoff_s,
            err_msg=f"Controller never came back",
        )
        self.redpanda.logger.debug("controller recovered")

        # if this test is set to decommission, recommission the dying broker
        if scenario == Scenario.Decommission:
            self.redpanda._admin.recommission_broker(decommissioned_node_id, self.redpanda.controller())

        # these nodes will rejoin with new node-ids
        for resurrected_node in killed_cluster_nodes:
            _ = self._join_new_node(resurrected_node)

        self._bulk_toggle_recovery_mode(
            self.redpanda.started_nodes(), medium_timeout, recovery_mode_enabled=False
        )

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in killed_node_ids
            )

        wait_until(
            controller_available, medium_timeout.timeout_s, medium_timeout.backoff_s
        )

        """ post process validation
        1. upsert a configuration (see details for why)
        2. check that the data topic has recovered
        3. check that recovery involves no dead node id
        4. check that we can successfully produce to the data topic
        """

        # this is implicitly going to wait for the successful removal of the dead nodes too
        # because until they can be successfully decomissioned and removed, cluster status
        # will continue to show the wrong version of the configuration
        self.redpanda.set_cluster_config(
            {"raft_learner_recovery_rate": 1 << 30},
            timeout=really_long_timeout.timeout_s,
        )

        wait_until(
            lambda: self._check_topic_recovered(
                topic=topic, killed_node_ids=killed_node_ids
            ),
            timeout_sec=really_long_timeout.timeout_s,
            backoff_sec=really_long_timeout.backoff_s,
            retry_on_exc=True,
        )

        wait_until(
            lambda: self._no_majority_lost_partitions(
                designated_survivors[0], killed_node_ids, really_short_timeout
            ),
            timeout_sec=long_timeout.timeout_s,
            backoff_sec=long_timeout.backoff_s,
        )

        producer = KgoVerifierProducer(  # type: ignore
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=3000,
        )
        producer.start(clean=True)
        producer.wait(timeout_sec=medium_timeout.timeout_s)
        status = producer.produce_status
        assert status.sent == 3000

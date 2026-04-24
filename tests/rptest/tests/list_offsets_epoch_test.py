# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

import kafka.protocol.types as types
from kafka import errors as kerr
from kafka.admin import KafkaAdminClient
from kafka.protocol.api import Request, Response

from ducktape.mark.resource import cluster as ducktape_cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import V_3_0_0

from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kafka import KafkaServiceAdapter
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


# --- ListOffsets v4 (API key 2) ---
# v4 is the first version with leader_epoch in the response.


class ListOffsetsResponse_v4(Response):
    API_KEY = 2
    API_VERSION = 4
    SCHEMA = types.Schema(
        ("throttle_time_ms", types.Int32),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("error_code", types.Int16),
                        ("timestamp", types.Int64),
                        ("offset", types.Int64),
                        ("leader_epoch", types.Int32),
                    ),
                ),
            ),
        ),
    )


class ListOffsetsRequest_v4(Request):
    API_KEY = 2
    API_VERSION = 4
    RESPONSE_TYPE = ListOffsetsResponse_v4
    SCHEMA = types.Schema(
        ("replica_id", types.Int32),
        ("isolation_level", types.Int8),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("current_leader_epoch", types.Int32),
                        ("timestamp", types.Int64),
                    ),
                ),
            ),
        ),
    )


TOPIC_NAME = "epoch-test"


class BaseListOffsetsLeaderEpochTest:
    """
    Shared logic for verifying that ListOffsets returns the correct
    leader epoch per return path.  Subclasses supply the cluster (Redpanda
    or Kafka) and a cluster-specific `_advance_leader_epoch` that bumps
    the partition's leader epoch without modifying the log.

    CORE-12505: Redpanda incorrectly returns the current leader epoch
    instead of the historical epoch for earliest, timequery, and empty
    partition paths.
    """

    logger: Any

    def client(self) -> DefaultClient:
        raise NotImplementedError

    def _restart_leader(self, cluster, topic, partition):
        """Restart the current leader for the given partition.

        Subclasses resolve the leader node (cluster-specific) and invoke
        the cluster's restart primitive.  `_advance_leader_epoch` wraps
        this with pre/post epoch checks.
        """
        raise NotImplementedError

    def _wait_for_stable_partition(
        self, rpk: RpkTool, topic: str, partition: int, timeout_sec: int = 30
    ) -> Any:
        """Return the row for `topic`/`partition`, waiting until the
        partition has a non-negative leader and a known epoch.

        `describe_topic` without `tolerant=True` silently omits rows that
        are missing metadata during leadership transitions, which would
        otherwise surface as `IndexError` when indexing by position.
        """

        def ready() -> Any:
            rows = list(rpk.describe_topic(topic, tolerant=True))
            if len(rows) <= partition:
                return None
            row = rows[partition]
            if row.leader is None or row.leader < 0 or row.leader_epoch is None:
                return None
            return row

        return wait_until_result(
            ready,
            timeout_sec=timeout_sec,
            backoff_sec=0.5,
            err_msg=f"No stable leader for {topic}/{partition}",
        )

    def _advance_leader_epoch(self, cluster, topic, partition):
        """Bump the leader epoch by restarting the current leader.

        Under our config (Kafka: auto.leader.rebalance.enable=false,
        default controlled shutdown; Redpanda: enable_leader_balancer
        disabled), the controller elects a different in-sync replica
        during the restart and does not hand leadership back when the
        restarted node rejoins.  The log is unchanged, so a gap opens
        between the record epoch and the current leader epoch.
        """
        rpk = RpkTool(cluster)
        prior_epoch = self._wait_for_stable_partition(
            rpk, topic, partition
        ).leader_epoch

        self._restart_leader(cluster, topic, partition)

        def epoch_advanced():
            rows = list(rpk.describe_topic(topic, tolerant=True))
            if len(rows) <= partition:
                return False
            epoch = rows[partition].leader_epoch
            return epoch is not None and epoch > prior_epoch

        wait_until(
            epoch_advanced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=(
                f"Leader epoch for {topic}/{partition} did not advance "
                f"past {prior_epoch}"
            ),
        )

    def _setup_topic_with_epoch_gap(self, cluster, num_records: int = 12):
        """Produce `num_records` records at the initial epoch, then advance
        the leader epoch 3 times.

        After this setup all records (if any) are from the initial epoch
        and the current leader epoch is >= 3, creating a gap between the
        record epoch and the current epoch.  Pass `num_records=0` to
        exercise the empty-partition path.

        Returns (initial_epoch, current_epoch).
        """
        rpk = RpkTool(cluster)

        topic = TopicSpec(name=TOPIC_NAME, partition_count=1, replication_factor=3)
        self.client().create_topic(topic)

        # Produce records — all will be in the initial epoch
        for i in range(num_records):
            rpk.produce(TOPIC_NAME, f"key-{i}", f"val-{i}")

        partitions = list(rpk.describe_topic(TOPIC_NAME))
        initial_epoch = partitions[0].leader_epoch
        self.logger.info(
            f"Initial state: HWM={partitions[0].high_watermark}, "
            f"epoch={initial_epoch}, records={num_records}"
        )

        # Advance the leader epoch 3 times
        for i in range(3):
            self._advance_leader_epoch(cluster, TOPIC_NAME, 0)
            partitions = list(rpk.describe_topic(TOPIC_NAME))
            self.logger.info(
                f"Epoch advance {i + 1}/3: epoch={partitions[0].leader_epoch}"
            )

        current_epoch = partitions[0].leader_epoch
        self.logger.info(
            f"Setup complete: HWM={partitions[0].high_watermark}, "
            f"epoch={current_epoch} (records from epoch {initial_epoch})"
        )

        return initial_epoch, current_epoch

    def _list_offsets(self, cluster, topic, partition, timestamp):
        """Call ListOffsets API v4 and return (offset, leader_epoch).

        Args:
            cluster: Cluster to query (Redpanda or Kafka).
            topic: Topic name.
            partition: Partition index.
            timestamp: -2 for earliest, -1 for latest, or a Unix
                       timestamp in milliseconds for timequery.
        """
        rpk = RpkTool(cluster)
        leader_id = self._wait_for_stable_partition(rpk, topic, partition).leader

        client = KafkaAdminClient(bootstrap_servers=cluster.brokers())
        try:
            # Ensure the client has metadata for this topic
            f = client._client.add_topic(topic)
            client._wait_for_futures([f])

            request = ListOffsetsRequest_v4(
                replica_id=-1,
                isolation_level=0,  # read_uncommitted
                topics=[
                    (
                        topic,
                        [(partition, -1, timestamp)],  # -1 = no epoch fencing
                    )
                ],
            )
            future = client._send_request_to_node(leader_id, request)
            client._wait_for_futures([future])
            response = future.value
        finally:
            client.close()

        for _resp_topic, resp_partitions in response.topics:
            for (
                part_id,
                error_code,
                _resp_ts,
                resp_offset,
                leader_epoch,
            ) in resp_partitions:
                if part_id == partition:
                    error = kerr.for_code(error_code)
                    if error is not kerr.NoError:
                        raise error(
                            f"ListOffsets error for {topic}/{partition}: {error_code}"
                        )
                    return (resp_offset, leader_epoch)

        raise RuntimeError(f"Partition {partition} not found in ListOffsets response")

    def _test_list_offsets_epoch(self, cluster, expect_incorrect_behavior):
        """Verify ListOffsets returns the correct leader epoch for each
        timestamp query type.

        All records are produced before leadership is transferred 3
        times.  The earliest and timequery paths should return the
        initial epoch (the record epoch), while the latest path should
        return the current leader epoch (correct per Kafka).
        """
        initial_epoch, current_epoch = self._setup_topic_with_epoch_gap(cluster)

        # --- Earliest (timestamp = -2) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-2)
        self.logger.info(
            f"Earliest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        if expect_incorrect_behavior:
            assert epoch == current_epoch, (
                f"Bug expected: earliest epoch should be current "
                f"({current_epoch}), got {epoch}"
            )
        else:
            assert epoch == initial_epoch, (
                f"Earliest epoch should be {initial_epoch} (record epoch), got {epoch}"
            )

        # --- Latest (timestamp = -1) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-1)
        self.logger.info(
            f"Latest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        # Both Redpanda and Kafka return the current leader epoch for timestamp=-1.
        assert epoch == current_epoch, (
            f"Latest epoch should be current leader epoch "
            f"({current_epoch}), got {epoch}"
        )

        # --- Timequery (timestamp = 0) ---
        # timestamp=0 is earlier than any wall-clock record timestamp,
        # so the query returns the start of the log.
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=0)
        self.logger.info(
            f"Timequery: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        if expect_incorrect_behavior:
            assert epoch == current_epoch, (
                f"Bug expected: timequery epoch should be current "
                f"({current_epoch}), got {epoch}"
            )
        else:
            assert epoch == initial_epoch, (
                f"Timequery epoch should be {initial_epoch} (record epoch), got {epoch}"
            )

    def _test_empty_partition_list_offsets_epoch(
        self, cluster, expect_incorrect_behavior
    ):
        """Verify ListOffsets on an empty partition whose leader epoch has
        advanced.

        No records are produced, so earliest/latest both point at
        offset 0 and there is no record epoch to return.  Kafka returns
        the current leader epoch for those paths; a timequery has no
        matching record and returns offset=-1, leader_epoch=-1.
        """
        _initial_epoch, current_epoch = self._setup_topic_with_epoch_gap(
            cluster, num_records=0
        )

        # --- Earliest (timestamp = -2) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-2)
        self.logger.info(
            f"Earliest (empty): offset={offset}, epoch={epoch}, "
            f"current_epoch={current_epoch}"
        )
        assert offset == 0, (
            f"Earliest offset should be 0 on empty partition, got {offset}"
        )
        # Both Redpanda and Kafka should return the current epoch for earliest on an empty partition.
        assert epoch == current_epoch, (
            f"Earliest epoch on empty partition should be current "
            f"({current_epoch}), got {epoch}"
        )

        # --- Latest (timestamp = -1) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-1)
        self.logger.info(
            f"Latest (empty): offset={offset}, epoch={epoch}, "
            f"current_epoch={current_epoch}"
        )
        assert offset == 0, (
            f"Latest offset should be 0 on empty partition, got {offset}"
        )
        # Both Redpanda and Kafka should return the current epoch for latest on an empty partition.
        assert epoch == current_epoch, (
            f"Latest epoch should be current leader epoch "
            f"({current_epoch}), got {epoch}"
        )

        # --- Timequery (timestamp = 0) ---
        # No record matches any timestamp on an empty partition, so the
        # server returns offset=-1 with leader_epoch=-1.
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=0)
        self.logger.info(
            f"Timequery (empty): offset={offset}, epoch={epoch}, "
            f"current_epoch={current_epoch}"
        )
        assert offset == -1, (
            f"Timequery on empty partition should return -1, got {offset}"
        )
        if expect_incorrect_behavior:
            assert epoch == current_epoch, (
                f"Bug expected: timequery epoch on empty partition should be "
                f"current ({current_epoch}), got {epoch}"
            )
        else:
            assert epoch == -1, (
                f"Timequery epoch on empty partition should be -1, got {epoch}"
            )


class ListOffsetsLeaderEpochRedpandaTest(RedpandaTest, BaseListOffsetsLeaderEpochTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            *args,
            num_brokers=3,
            extra_rp_conf={"enable_leader_balancer": False},
            **kwargs,
        )

    def _restart_leader(self, cluster, topic, partition):
        rpk = RpkTool(cluster)
        leader_id = list(rpk.describe_topic(topic))[partition].leader
        leader_node = self.redpanda.get_node_by_id(leader_id)
        assert leader_node is not None, (
            f"Could not resolve leader node for id {leader_id}"
        )
        self.redpanda.restart_nodes(leader_node)

    @cluster(num_nodes=3)
    def test_list_offsets_epoch(self):
        self._test_list_offsets_epoch(self.redpanda, expect_incorrect_behavior=True)

    @cluster(num_nodes=3)
    def test_list_offsets_epoch_empty_partition(self):
        self._test_empty_partition_list_offsets_epoch(
            self.redpanda, expect_incorrect_behavior=True
        )


class ListOffsetsLeaderEpochKafkaTest(Test, BaseListOffsetsLeaderEpochTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=V_3_0_0)

        # Disable auto preferred-leader rebalance so leadership does not
        # bounce back on its own after we restart the leader.
        server_prop_overrides = [
            ["auto.leader.rebalance.enable", "false"],
        ]

        self.kafka = KafkaServiceAdapter(
            self.test_context,
            KafkaService(
                self.test_context,
                num_nodes=3,
                zk=self.zk,
                server_prop_overrides=server_prop_overrides,
                version=V_3_0_0,
            ),
        )

        self._client = DefaultClient(self.kafka)

    def client(self):
        return self._client

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        self.logger.info("Stopping Kafka...")
        self.kafka.stop()

        self.logger.info("Stopping Zookeeper...")
        self.zk.stop()

    def _restart_leader(self, cluster, topic, partition):
        leader_node = self.kafka.leader(topic, partition)
        self.kafka.restart_node(leader_node, clean_shutdown=True)

    @ducktape_cluster(num_nodes=4)
    def test_list_offsets_epoch(self):
        # Kafka defines the correct behavior we compare against.
        self._test_list_offsets_epoch(self.kafka, expect_incorrect_behavior=False)

    @ducktape_cluster(num_nodes=4)
    def test_list_offsets_epoch_empty_partition(self):
        # Kafka defines the correct behavior we compare against.
        self._test_empty_partition_list_offsets_epoch(
            self.kafka, expect_incorrect_behavior=False
        )

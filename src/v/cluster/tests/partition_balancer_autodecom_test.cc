// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_types.h"
#include "cluster/partition_balancer_planner.h"

#include <gtest/gtest.h>

#include <ranges>

namespace cluster {

// testing a private static function in partition_balancer_planner, we're using
// a friend class accessor to reach in
class partition_balancer_planner_accessor {
    using params = cluster::partition_balancer_planner::
      do_get_auto_decommission_actions_params;

    // convenience representation of node state
    // up_down: represents whether a has been down past the auto decom timeout
    // node_state: relevant special node statuses that should prevent a decom
    // node_and_status: packaged node state so a test scenario can be a list of
    //                  node_id and node state
    enum class up_down : uint8_t { up, down };
    enum class node_state : uint8_t { normal, maintenance, decommissioning };
    struct node_and_status {
        up_down up_or_down;
        node_state node_state;
        model::node_id node_id;
        config_version config_version;
    };

    // package a set of a params for determinining auto decommission along side
    // a holder which will keep referenced memory alive
    struct params_and_holders {
        params params;
        std::vector<std::unique_ptr<std::optional<auto_decommission_status>>>
          report_memory_holder;
    };

    // translate the convenience representation to a parameters pack, and a list
    // of memory holders to keep the packaged references alive
    static params_and_holders
    make_params(const std::vector<node_and_status>& nodes) {
        absl::flat_hash_set<model::node_id> all_nodes{};
        absl::flat_hash_set<model::node_id> downed_nodes{};
        absl::flat_hash_set<model::node_id> maintenance_nodes{};
        absl::flat_hash_set<model::node_id> decommissioning_nodes{};

        for (const auto& node_status : nodes) {
            if (node_status.up_or_down == up_down::down) {
                downed_nodes.insert(node_status.node_id);
            }
            all_nodes.insert(node_status.node_id);

            if (node_status.node_state == node_state::maintenance) {
                maintenance_nodes.insert(node_status.node_id);
            }
            if (node_status.node_state == node_state::decommissioning) {
                decommissioning_nodes.insert(node_status.node_id);
            }
            // nothing for normal
        }

        // keeps the memory of a given auto decom report alive
        std::vector<std::unique_ptr<std::optional<auto_decommission_status>>>
          memory_holder{};

        // index map upon the living reports
        partition_balancer_planner::auto_decom_ref_map decom_report_map{};

        for (const auto& node_status : nodes) {
            if (downed_nodes.contains(node_status.node_id)) {
                // skip downed nodes for report generation
                continue;
            }

            // for living nodes, gather the bearing memory and reference list on
            // their auto decom reports
            auto report_holder
              = std::make_unique<std::optional<auto_decommission_status>>(
                std::nullopt);
            // generate an actual decom status if there are dead nodes
            if (!downed_nodes.empty()) {
                auto_decommission_status_data adsd{
                  .configuration_version = node_status.config_version,
                  .nodes_past_auto_decom_timeout = std::vector<model::node_id>(
                    downed_nodes.begin(), downed_nodes.end())};
                report_holder
                  = std::make_unique<std::optional<auto_decommission_status>>(
                    std::optional<auto_decommission_status>{std::move(adsd)});
            }
            decom_report_map.emplace(
              node_status.node_id,
              std::reference_wrapper(std::as_const(*report_holder)));
            memory_holder.emplace_back(std::move(report_holder));
        }

        // clang keeps putting the entire list of initializers on one line
        // clang-format off
        return params_and_holders{
          .params
          = params{
            .node_to_decom = std::move(decom_report_map),
            .cluster_members = std::move(all_nodes),
            .decommissioning_nodes = std::move(decommissioning_nodes),
            .maintenance_mode_nodes = std::move(maintenance_nodes),
            .current_config = config_version{}},
          .report_memory_holder = std::move(memory_holder)};
        // clang-format on
    }

public:
    static void smoke_test() {
        params smoke_params{
          .node_to_decom = {},
          .cluster_members
          = {model::node_id{0}, model::node_id{2}, model::node_id{1}},
          .decommissioning_nodes = {},
          .maintenance_mode_nodes = {},
          .current_config = config_version{0}};

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            smoke_params);
        ASSERT_TRUE(result.empty());
    }

    static void test_normal_removal() {
        // check that we can decommission a node when a majority votes that its
        // past timeout
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .config_version = config_version{0}},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .config_version = config_version{0}},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .config_version = config_version{0}},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::normal,
             .node_id = model::node_id{2},
             .config_version = config_version{0}}});
        params.current_config = config_version{0};

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 1);

        ASSERT_EQ(result.begin().operator*(), model::node_id{2});
    }

    static void test_votes_no_quorum() {
        // check that we don't decommission if we dont have a quorum of votes
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .config_version = config_version{0}},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .config_version = config_version{0}},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .config_version = config_version{0}},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::normal,
             .node_id = model::node_id{2},
             .config_version = config_version{0}}});
        params.current_config = config_version{0};

        // pluck node 3's report from the reports map
        params.node_to_decom.erase(model::node_id{3});
        // now there are two reports that 2 is dead, from 0 and 1

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_ignore_incorrect_config() {
        // check that we ignore votes from old configurations
        config_version current_config{1};
        config_version old_config{0};
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .config_version = current_config},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .config_version = current_config},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .config_version = old_config},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::normal,
             .node_id = model::node_id{2},
             .config_version = current_config}});

        params.current_config = current_config;

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_dont_decom_twice() {
        // make sure a decommissioning node doesn't get another decommissioning
        // command
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .config_version = config_version{0}},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .config_version = config_version{0}},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .config_version = config_version{0}},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::decommissioning,
             .node_id = model::node_id{2},
             .config_version = config_version{0}}});
        params.current_config = config_version{0};

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_dont_decom_maintenance() {
        // if a node is in maintenance mode, dont decommission it
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .config_version = config_version{0}},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .config_version = config_version{0}},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .config_version = config_version{0}},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::maintenance,
             .node_id = model::node_id{2},
             .config_version = config_version{0}}});
        params.current_config = config_version{0};

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_ignore_non_members() {
        // make sure that we ignore reports from nodes that are no longer
        // cluster members
        config_version current_config{1};
        config_version old_config{0};
        auto [params, holder] = make_params({
          // node 0 up and normal
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{0},
            .config_version = current_config},

          // node 1 up and normal
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{1},
            .config_version = current_config},

          // node 3 up and normal
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{3},
            .config_version = old_config},

          // node 2 down
          node_and_status{
            .up_or_down = up_down::down,
            .node_state = node_state::maintenance,
            .node_id = model::node_id{2},
            .config_version = current_config},

          // node will be a non-member
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{4},
            .config_version = current_config},

          // node 5 will be a non-member
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{5},
            .config_version = current_config},

        });
        params.current_config = current_config;

        // drop 4 and 5, this should leave not enough votes to remove 2
        params.cluster_members.erase(model::node_id{4});
        params.cluster_members.erase(model::node_id{5});

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_multi_decom() {
        // checks that given a large enough cluster, we can decom multiple nodes
        // at the same time
        config_version current_config{0};
        std::vector<node_and_status> nodes_and_statuses{};

        for (auto node_number : std::ranges::iota_view(0, 5)) {
            auto node_id = model::node_id{node_number};
            nodes_and_statuses.emplace_back(
              node_and_status{
                .up_or_down = up_down::up,
                .node_state = node_state::normal,
                .node_id = node_id,
                .config_version = current_config});
        }
        nodes_and_statuses[0].up_or_down = up_down::down;
        nodes_and_statuses[1].up_or_down = up_down::down;
        auto [params, holder] = make_params(nodes_and_statuses);
        params.current_config = current_config;

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 2);
        ASSERT_TRUE(result.contains(model::node_id{0}));
        ASSERT_TRUE(result.contains(model::node_id{1}));
    }

    static void test_quorum_limits() {
        // we should only decom a node if a quorum agrees that the node is past
        // timeout
        // this test checks the limits for even & odd cluster numbers
        config_version current_config{0};
        std::vector<node_and_status> nodes_and_statuses{};
        int dead_node_number{0};
        model::node_id dead_node{dead_node_number};

        {
            // case: 5
            for (auto node_number : std::ranges::iota_view(0, 5)) {
                auto node_id = model::node_id{node_number};
                nodes_and_statuses.emplace_back(
                  node_and_status{
                    .up_or_down = up_down::up,
                    .node_state = node_state::normal,
                    .node_id = node_id,
                    .config_version = current_config});
            }
            nodes_and_statuses[dead_node_number].up_or_down = up_down::down;
            auto [params, holder] = make_params(nodes_and_statuses);
            params.current_config = current_config;
            { // all agree
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 3 agree 1 does not
                params.node_to_decom.erase(model::node_id{1});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 2 agree 2 do not
                params.node_to_decom.erase(model::node_id{2});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 0);
            }
        }
        {
            // case: 6
            for (auto node_number : std::ranges::iota_view(0, 6)) {
                auto node_id = model::node_id{node_number};
                nodes_and_statuses.emplace_back(
                  node_and_status{
                    .up_or_down = up_down::up,
                    .node_state = node_state::normal,
                    .node_id = node_id,
                    .config_version = current_config});
            }
            nodes_and_statuses[dead_node_number].up_or_down = up_down::down;
            auto [params, holder] = make_params(nodes_and_statuses);
            params.current_config = current_config;
            { // all agree
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 4 agree 1 does not
                params.node_to_decom.erase(model::node_id{1});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 3 agree 2 do not
                params.node_to_decom.erase(model::node_id{2});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 0);
            }
        }
    }
};

TEST(AutoDecomTestSuite, Smoke) {
    partition_balancer_planner_accessor::smoke_test();
};
TEST(AutoDecomTestSuite, NormalRemove) {
    partition_balancer_planner_accessor::test_normal_removal();
};
TEST(AutoDecomTestSuite, NoQuorum) {
    partition_balancer_planner_accessor::test_votes_no_quorum();
}
TEST(AutoDecomTestSuite, OldConfig) {
    partition_balancer_planner_accessor::test_ignore_incorrect_config();
}
TEST(AutoDecomTestSuite, DontDecomTwice) {
    partition_balancer_planner_accessor::test_dont_decom_twice();
}
TEST(AutoDecomTestSuite, DontDecomMaintenance) {
    partition_balancer_planner_accessor::test_dont_decom_maintenance();
}
TEST(AutoDecomTestSuite, IgnoreNonMembers) {
    partition_balancer_planner_accessor::test_ignore_non_members();
}
TEST(AutoDecomTestSuite, DecomMultipleNodes) {
    partition_balancer_planner_accessor::test_multi_decom();
}
TEST(AutoDecomTestSuite, QuorumLimits) {
    partition_balancer_planner_accessor::test_quorum_limits();
}
} // namespace cluster

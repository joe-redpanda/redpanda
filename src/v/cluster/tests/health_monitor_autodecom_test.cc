// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_types.h"
#include "model/fundamental.h"
#include "rpc/types.h"

#include <gtest/gtest.h>

#include <ranges>
#include <unordered_map>

namespace cluster {

struct health_report_accessor {
    using params
      = health_monitor_backend::do_collect_auto_decommission_status_params;

    static constexpr auto now = rpc::clock_type::time_point{72h};
    static constexpr auto auto_decom_timeout = 24h;
    static constexpr auto timed_out_timepoint = now - auto_decom_timeout - 1h;
    static constexpr auto not_timed_out_timepoint = now - 12h;
    static constexpr auto boot_time_zero = rpc::clock_type::time_point{0h};
    static constexpr auto boot_time_recent = rpc::clock_type::time_point{
      not_timed_out_timepoint - 2h};

    static params get_default_params() {
        return {
          .timeout_duration = auto_decom_timeout,
          .now = now,
          .default_last_seen = boot_time_zero,
          .nodes = {},
          .node_status_getter =
            [](model::node_id node_id) {
                return node_status{.node_id = node_id, .last_seen = now};
            },
          .current_config_version = config_version{0}

        };
    }

    static health_monitor_backend::get_node_status_t getter_from_map(
      std::unordered_map<model::node_id, rpc::clock_type::time_point>
        last_seen_map) {
        auto node_status_map
          = std::move(last_seen_map)
            | std::ranges::views::transform([](auto node_and_last_seen) {
                  return std::pair{
                    node_and_last_seen.first,
                    node_status{
                      .node_id = node_and_last_seen.first,
                      .last_seen = node_and_last_seen.second}};
              })
            | std::ranges::to<
              std::unordered_map<model::node_id, node_status>>();

        return [node_status_map = std::move(node_status_map)](
                 model::node_id node_id) -> std::optional<node_status> {
            auto it = node_status_map.find(node_id);
            if (it == node_status_map.end()) {
                return std::nullopt;
            }
            return it->second;
        };
    }

    static std::vector<model::node_id> members_from_map(
      std::unordered_map<model::node_id, rpc::clock_type::time_point>
        last_seen_map) {
        auto members = std::move(last_seen_map)
                       | std::ranges::views::transform(
                         [](auto node_and_last_seen) {
                             return node_and_last_seen.first;
                         })
                       | std::ranges::to<std::vector<model::node_id>>();
        return members;
    }

    static std::pair<
      std::vector<model::node_id>,
      health_monitor_backend::get_node_status_t>
    members_and_getter_from_map(
      std::unordered_map<model::node_id, rpc::clock_type::time_point>
        last_seen_map) {
        return {
          members_from_map(last_seen_map),
          getter_from_map(std::move(last_seen_map))};
    }

    // just checks that a healthy cluster spits out no decom reports
    static void smoke_test() {
        auto [members, getter] = members_and_getter_from_map(
          {{model::node_id{0}, now},
           {model::node_id{1}, now},
           {model::node_id{2}, now}});
        auto params = get_default_params();
        params.nodes = std::move(members);
        params.node_status_getter = std::move(getter);

        auto maybe_decom_status
          = health_monitor_backend::do_collect_auto_decommission_status(params);
        ASSERT_FALSE(maybe_decom_status.has_value());
    }

    // check that we get a decom report for an expired node
    static void test_timed_out() {
        auto [members, getter] = members_and_getter_from_map(
          {{model::node_id{0}, now},
           {model::node_id{1}, now},
           {model::node_id{2}, timed_out_timepoint}});
        auto params = get_default_params();
        params.nodes = std::move(members);
        params.node_status_getter = std::move(getter);

        auto maybe_decom_status
          = health_monitor_backend::do_collect_auto_decommission_status(params);
        ASSERT_TRUE(maybe_decom_status.has_value());
        ASSERT_EQ(maybe_decom_status->nodes_past_auto_decom_timeout.size(), 1);
        ASSERT_TRUE(
          maybe_decom_status->nodes_past_auto_decom_timeout[0]
          == model::node_id{2});
    }

    // check that we refuse to decom a node if never seen when the default is
    // above timeout timepoint
    static void test_missing_status_no_timeout() {
        std::unordered_map<model::node_id, rpc::clock_type::time_point>
          node_to_last_seen = {
            {model::node_id{0}, now},
            {model::node_id{1}, now},
            {model::node_id{2}, now}};

        // members will have all the nodes
        auto members = members_from_map(node_to_last_seen);

        // we'll be missing node status from 2
        node_to_last_seen.erase(model::node_id{2});
        auto getter = getter_from_map(node_to_last_seen);

        // construct params with a default time that is not timed out
        auto params = get_default_params();
        params.nodes = std::move(members);
        params.node_status_getter = std::move(getter);
        params.default_last_seen = not_timed_out_timepoint;

        auto maybe_decom_status
          = health_monitor_backend::do_collect_auto_decommission_status(params);
        ASSERT_FALSE(maybe_decom_status.has_value());
    }

    // check that we do decom a missing node if the default time provided is far
    // enough back to invoke timeout
    static void test_missing_status_timeout() {
        std::unordered_map<model::node_id, rpc::clock_type::time_point>
          node_to_last_seen = {
            {model::node_id{0}, now},
            {model::node_id{1}, now},
            {model::node_id{2}, now}};

        // members will have all the nodes
        auto members = members_from_map(node_to_last_seen);

        // we'll be missing node status from 2
        node_to_last_seen.erase(model::node_id{2});
        auto getter = getter_from_map(node_to_last_seen);

        // construct params with a default time that is not timed out
        auto params = get_default_params();
        params.nodes = std::move(members);
        params.node_status_getter = std::move(getter);
        params.default_last_seen = boot_time_zero;

        auto maybe_decom_status
          = health_monitor_backend::do_collect_auto_decommission_status(params);
        ASSERT_TRUE(maybe_decom_status.has_value());
        ASSERT_EQ(maybe_decom_status->nodes_past_auto_decom_timeout.size(), 1);
        ASSERT_TRUE(
          maybe_decom_status->nodes_past_auto_decom_timeout[0]
          == model::node_id{2});
    }

    // check that we're pushing the right config through
    static void test_config() {
        auto [members, getter] = members_and_getter_from_map(
          {{model::node_id{0}, now},
           {model::node_id{1}, now},
           {model::node_id{2}, timed_out_timepoint}});
        auto params = get_default_params();
        params.nodes = std::move(members);
        params.node_status_getter = std::move(getter);
        params.current_config_version = config_version{0};
        {
            auto maybe_decom_status
              = health_monitor_backend::do_collect_auto_decommission_status(
                params);
            ASSERT_TRUE(maybe_decom_status.has_value());
            ASSERT_EQ(
              maybe_decom_status->configuration_version, config_version{0});
        }

        {
            params.current_config_version = config_version{100};
            auto maybe_decom_status
              = health_monitor_backend::do_collect_auto_decommission_status(
                params);
            ASSERT_TRUE(maybe_decom_status.has_value());
            ASSERT_EQ(
              maybe_decom_status->configuration_version, config_version{100});
        }
    }

    // don't generate reports on non-cluster members
    static void test_ignore_nonmembers() {
        std::unordered_map<model::node_id, rpc::clock_type::time_point>
          node_to_last_seen = {
            {model::node_id{0}, now},
            {model::node_id{1}, now},
            {model::node_id{2}, timed_out_timepoint}};

        // getter will have all
        auto getter = getter_from_map(node_to_last_seen);
        // 2 will not be a member
        node_to_last_seen.erase(model::node_id{2});
        auto members = members_from_map(node_to_last_seen);

        // construct params with a default time that is not timed out
        auto params = get_default_params();
        params.nodes = std::move(members);
        params.node_status_getter = std::move(getter);
        params.default_last_seen = boot_time_zero;

        auto maybe_decom_status
          = health_monitor_backend::do_collect_auto_decommission_status(params);
        ASSERT_FALSE(maybe_decom_status.has_value());
    }

    // time out multiple nodes
    static void test_multi_time_out() {
        auto [members, getter] = members_and_getter_from_map(
          {{model::node_id{0}, timed_out_timepoint},
           {model::node_id{1}, timed_out_timepoint},
           {model::node_id{2}, timed_out_timepoint}});
        auto params = get_default_params();
        params.nodes = members;
        params.node_status_getter = std::move(getter);

        auto maybe_decom_status
          = health_monitor_backend::do_collect_auto_decommission_status(params);
        ASSERT_TRUE(maybe_decom_status.has_value());
        ASSERT_EQ(maybe_decom_status->nodes_past_auto_decom_timeout.size(), 3);
        ASSERT_EQ(maybe_decom_status->nodes_past_auto_decom_timeout, members);
    }
};

TEST(AutoDecomTestSuite, Smoke) { health_report_accessor::smoke_test(); };
TEST(AutoDecomTestSuite, OneTimedOut) {
    health_report_accessor::test_timed_out();
};
TEST(AutoDecomTestSuite, MissingStatusNoTimeout) {
    health_report_accessor::test_missing_status_no_timeout();
};
TEST(AutoDecomTestSuite, MissingStatusTimeout) {
    health_report_accessor::test_missing_status_timeout();
};
TEST(AutoDecomTestSuite, Config) { health_report_accessor::test_config(); };
TEST(AutoDecomTestSuite, NoNonMembers) {
    health_report_accessor::test_ignore_nonmembers();
};
TEST(AutoDecomTestSuite, MultiTimeout) {
    health_report_accessor::test_multi_time_out();
};
} // namespace cluster

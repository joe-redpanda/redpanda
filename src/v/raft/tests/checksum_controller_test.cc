// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "raft/checksum_controller.h"
#include "raft/stm_checksum_component.h"
#include "raft/types.h"
#include "serde/rw/map.h"
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <cstdlib>

using namespace std::chrono_literals; // NOLINT

struct __checksums_at_offset
  : serde::envelope<
      __checksums_at_offset,
      serde::version<0>,
      serde::compat_version<0>> {
    model::offset prepared_at_offset;
    iobuf prepared_checksums_blob;

    auto serde_fields() {
        return std::tie(prepared_at_offset, prepared_checksums_blob);
    }
};

SEASTAR_THREAD_TEST_CASE(t_checksum_controller) {
    // std::exit(EXIT_FAILURE);
    /*raft::checksum_controller controller{};

    BOOST_CHECK(!controller.should_checksum());

    for (int i{0}; i < raft::checksum_controller::checksum_batch_cadence * 10;
         ++i) {
        controller.on_batch();
    }

    BOOST_CHECK(controller.should_checksum());

    controller.on_checksum();

    // we can get two checksums from a burst, but not more
    BOOST_CHECK(controller.should_checksum());

    controller.on_checksum();

    BOOST_CHECK(!controller.should_checksum());*/
    storage::record_batch_builder builder{
      model::record_batch_type::stm_manager, model::offset(0)};

    __checksums_at_offset checksums{};

    raft::state_machine_checksums inner_map{};

    inner_map.emplace(
      137,
      raft::stm_state_checksum{
        .checksum = 201, .last_applied_offset = model::offset{17}});

    auto inner_blob = serde::to_iobuf(std::move(inner_map));

    checksums.prepared_at_offset = model::offset{0};
    checksums.prepared_checksums_blob = std::move(inner_blob);

    builder.add_raw_kv(
      serde::to_iobuf(raft::stm_checksum_component::record_type::execute),
      serde::to_iobuf(std::move(checksums)));

    auto record = std::move(builder).build();

    record.for_each_record([](model::record record) {
        auto type
          = serde::from_iobuf<raft::stm_checksum_component::record_type>(
            record.release_key());
        BOOST_CHECK(type == raft::stm_checksum_component::record_type::execute);
        auto checksums = serde::from_iobuf<__checksums_at_offset>(
          record.release_value());
        BOOST_CHECK(checksums.prepared_at_offset == model::offset{0});
    });

    BOOST_CHECK(true);
}

SEASTAR_THREAD_TEST_CASE(checksum_serialization_test) {
    // std::exit(EXIT_FAILURE);
    /*raft::checksum_controller controller{};

    BOOST_CHECK(!controller.should_checksum());

    for (int i{0}; i < raft::checksum_controller::checksum_batch_cadence * 10;
         ++i) {
        controller.on_batch();
    }

    BOOST_CHECK(controller.should_checksum());

    controller.on_checksum();

    // we can get two checksums from a burst, but not more
    BOOST_CHECK(controller.should_checksum());

    controller.on_checksum();

    BOOST_CHECK(!controller.should_checksum());*/
    storage::record_batch_builder builder{
      model::record_batch_type::stm_manager, model::offset(0)};

    raft::checksums_at_offset checksums;

    // raft::state_machine_checksums inner_map{};

    checksums.prepared_checksums.emplace(
      137,
      raft::stm_state_checksum{
        .checksum = 201, .last_applied_offset = model::offset{17}});

    // auto inner_blob = serde::to_iobuf(std::move(inner_map));

    checksums.prepared_at_offset = model::offset{0};
    // checksums.prepared_checksums_blob = std::move(inner_blob);

    builder.add_raw_kv(
      serde::to_iobuf(raft::stm_checksum_component::record_type::execute),
      serde::to_iobuf(std::move(checksums)));

    auto record = std::move(builder).build();

    record.for_each_record([](model::record record) {
        auto type
          = serde::from_iobuf<raft::stm_checksum_component::record_type>(
            record.release_key());
        BOOST_CHECK(type == raft::stm_checksum_component::record_type::execute);
        auto checksums = serde::from_iobuf<raft::checksums_at_offset>(
          record.release_value());
        BOOST_CHECK(checksums.prepared_at_offset == model::offset{0});
    });

    BOOST_CHECK(true);
}

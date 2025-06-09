/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "raft/checksum_controller.h"
#include "raft/logger.h"
#include "raft/types.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <optional>

namespace raft {

class state_machine_manager;

/**for tommorrow, create a command counter that gets handed to stms, have it
 * count the number of control batches sent out to determine 'should checksum'*/

// encapsulate logic for 'should checksum' or not
struct stm_checksum_component {
private: // types
    static constexpr std::chrono::milliseconds checksum_replication_timeout{
      100};

    struct checksum_validation_error {
        ss::sstring name;
        model::offset last_applied_offset;
    };

public:
    stm_checksum_component(
      ctx_log& log, state_machine_manager* parent, raft::consensus* raft);

    stm_checksum_component(const stm_checksum_component&) = default;
    stm_checksum_component& operator=(const stm_checksum_component&) = default;
    stm_checksum_component(stm_checksum_component&&) = default;
    stm_checksum_component& operator=(stm_checksum_component&&) = default;
    ~stm_checksum_component() = default;

    // main entry point
    ss::future<> apply(const model::record_batch& record_batch);

    void on_move(state_machine_manager* parent);

    enum class record_type : uint8_t { prepare, execute };

private:
    friend std::ostream& operator<<(std::ostream& o, record_type t);

    // application of a relevant record
    ss::future<> do_apply(const model::record_batch& record_batch);

    // application of an unpacked prepare record
    // snapshots stm checksums at the current offset and caches them for
    // future comparison when the checksums arrive in the log
    ss::future<> do_prepare(model::offset prepare_offset);

    // application of an unpacked execute checksum record
    // compares the most recent prepared checksums against the checksums
    // found in the applied execute checksum record
    void do_execute(checksums_at_offset remote_checksums_from_offset);

    // packs a prepare checksum record for replication
    model::record_batch make_prepare_checksum_batch();

    // packs an execute checksum record for replciation
    model::record_batch make_execute_checksum_batch(
      model::offset prepare_offset, state_machine_checksums checksums);

    // replicates a prepare record
    ss::future<> replicate_prepare();

    // replicates an execute checksum record
    ss::future<> replicate_checksum(model::offset prepare_offset);

    std::optional<checksum_validation_error>
    validate_checksums(const state_machine_checksums& remote);

    ctx_log* _log;
    state_machine_manager* _parent;
    consensus* _raft;
    std::optional<checksums_at_offset> maybe_prepared_checksums{std::nullopt};
    checksum_controller _checksum_controller;
};

} // namespace raft

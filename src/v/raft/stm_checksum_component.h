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

public: // types
    enum class record_type : uint8_t { prepare, execute };

    struct apply_token {
        model::offset record_offset;
        std::optional<state_machine_checksums> maybe_checksums_at_offset{
          std::nullopt};
        std::optional<record_type> maybe_record_type{std::nullopt};

    public:
        inline bool should_provide_checksums() {
            return maybe_record_type.has_value()
                   && maybe_record_type.value() == record_type::prepare;
        }

        inline void provide_checksums(state_machine_checksums&& checksums) {
            maybe_checksums_at_offset = std::move(checksums);
        }
    };

public:
    stm_checksum_component(ctx_log& log, raft::consensus* raft);

    stm_checksum_component(const stm_checksum_component&) = default;
    stm_checksum_component& operator=(const stm_checksum_component&) = default;
    stm_checksum_component(stm_checksum_component&&) = default;
    stm_checksum_component& operator=(stm_checksum_component&&) = default;
    ~stm_checksum_component() = default;

    // begins the apply operation, passes control back to caller to request
    // checksums if needed
    apply_token start_apply(const model::record_batch& record_batch);

    // completes the apply operation, requires requested checksums to be
    // fulfilled
    ss::future<> finish_apply(apply_token&& token);

    void on_move(state_machine_manager* parent);

private:
    friend std::ostream& operator<<(std::ostream& o, record_type t);

    // application of an unpacked prepare record
    // snapshots stm checksums at the current offset and caches them for
    // future comparison when the checksums arrive in the log
    ss::future<>
    do_prepare(model::offset prepare_offset, state_machine_checksums checksums);

    // application of an unpacked execute checksum record
    // compares the most recent prepared checksums against the checksums
    // found in the applied execute checksum record
    void do_execute(checksums_at_offset remote_checksums_from_offset);

    // packs a prepare checksum record for replication
    model::record_batch make_prepare_checksum_batch();

    // packs an execute checksum record for replciation
    model::record_batch
    make_execute_checksum_batch(checksums_at_offset checksums);

    // replicates a prepare record
    ss::future<> replicate_prepare();

    // replicates an execute checksum record
    ss::future<> replicate_checksum(checksums_at_offset checksums);

    std::optional<checksum_validation_error>
    validate_checksums(const state_machine_checksums& remote);

    ctx_log* _log;
    consensus* _raft;
    std::optional<checksums_at_offset> maybe_prepared_checksums{std::nullopt};
    checksum_controller _checksum_controller;
};

} // namespace raft

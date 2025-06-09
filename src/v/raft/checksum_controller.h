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

#include "model/record.h"
#include "raft/logger.h"
#include "raft/probe.h"

#include <seastar/core/lowres_clock.hh>

#include <cstdint>

namespace raft {

class consensus;

struct offset_delta_fetcher {
    explicit offset_delta_fetcher(consensus* raft, ctx_log* log);

    offset_delta_fetcher(const offset_delta_fetcher&) = default;
    offset_delta_fetcher& operator=(const offset_delta_fetcher&) = default;
    offset_delta_fetcher(offset_delta_fetcher&&) = default;
    offset_delta_fetcher& operator=(offset_delta_fetcher&&) = default;
    ~offset_delta_fetcher() = default;

    uint16_t fetch_counter_update();

    void reset();

    consensus* _raft;
    probe* _probe;
    uint64_t _prior_with_flush;
    uint64_t _prior_without_flush;
    ctx_log* _log;
};

struct checksum_cadence_stm {
    checksum_cadence_stm(
      uint16_t checksum_offset_cadence,
      seastar::lowres_clock::duration checksum_time_cadence);

    checksum_cadence_stm() = default;

    checksum_cadence_stm(const checksum_cadence_stm&) = default;
    checksum_cadence_stm& operator=(const checksum_cadence_stm&) = default;
    checksum_cadence_stm(checksum_cadence_stm&&) = default;
    checksum_cadence_stm& operator=(checksum_cadence_stm&&) = default;
    ~checksum_cadence_stm() = default;

    void update_counter(uint16_t counter_update);

    void decrement();

    void reset();

    bool check();

private:
    uint16_t _checksum_offset_cadence;
    seastar::lowres_clock::duration _checksum_time_cadence;
    uint32_t _batch_counter;
    seastar::lowres_clock::time_point _last_checksummed;
};

struct checksum_controller {
    // todo constructor
    checksum_controller(const checksum_controller&) = default;
    checksum_controller& operator=(const checksum_controller&) = default;
    checksum_controller(checksum_controller&&) = default;
    checksum_controller& operator=(checksum_controller&&) = default;
    ~checksum_controller() = default;

    enum class state : uint8_t {
        // leader or follower
        off, // do nothing until started

        // leader states
        lagging_leader,
        not_ready,
        should_prepare,
        should_checksum,
        wait_for_execute,

        // follower states
        follower
    };

    explicit checksum_controller(consensus* raft, ctx_log* log);

    bool should_replicate_prepare() const;

    bool should_replicate_checksum() const;

    void on_batch(const model::record_batch& batch);

    void on_replicate_prepare();

    void on_replicate_checksum();

    void on_execute_checksum();

    void reset();

    friend std::ostream& operator<<(std::ostream& o, state s);

private:
    state _state;
    consensus* _raft;
    checksum_cadence_stm _checksum_cadence_stm;
    offset_delta_fetcher _offset_delta_fetcher;
    ctx_log* _log;
};

} // namespace raft

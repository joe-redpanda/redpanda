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

#include "raft/checksum_controller.h"

#include "raft/consensus.h"
#include "raft/logger.h"

namespace raft {

offset_delta_fetcher::offset_delta_fetcher(consensus* raft, ctx_log* log)
  : _raft{raft}
  , _probe{&(raft->get_probe())}
  , _prior_with_flush(_probe->get_replicate_requests_ack_all_with_flush())
  , _prior_without_flush(_probe->get_replicate_requests_ack_all_without_flush())
  , _log{log} {}

uint16_t offset_delta_fetcher::fetch_counter_update() {
    auto new_with_flush = _probe->get_replicate_requests_ack_all_with_flush();

    auto new_without_flush
      = _probe->get_replicate_requests_ack_all_without_flush();

    auto delta_with_flush = new_with_flush - _prior_with_flush;
    auto delta_without_flush = new_without_flush - _prior_without_flush;

    // if the probe was reset, return no new deltas and reset counters
    if (new_with_flush < _prior_with_flush) {
        delta_with_flush = 0;
    }
    if (new_without_flush < _prior_without_flush) {
        delta_without_flush = 0;
    }

    // snap prior to current probe values
    _prior_with_flush = _probe->get_replicate_requests_ack_all_with_flush();
    _prior_without_flush
      = _probe->get_replicate_requests_ack_all_without_flush();

    // if deltas are large, obviously checkum
    if (delta_with_flush > std::numeric_limits<uint32_t>::max()) {
        return std::numeric_limits<uint16_t>::max();
    }
    if (delta_without_flush > std::numeric_limits<uint32_t>::max()) {
        return std::numeric_limits<uint16_t>::max();
    }

    auto delta = delta_with_flush + delta_without_flush;

    if (delta > std::numeric_limits<uint16_t>::max()) {
        return std::numeric_limits<uint16_t>::max();
    }

    return static_cast<uint16_t>(delta);
}

void offset_delta_fetcher::reset() {
    _prior_with_flush = _probe->get_replicate_requests_ack_all_with_flush();
    _prior_without_flush
      = _probe->get_replicate_requests_ack_all_without_flush();
}

checksum_cadence_stm::checksum_cadence_stm(
  uint16_t checksum_offset_cadence,
  seastar::lowres_clock::duration checksum_time_cadence)
  : _checksum_offset_cadence(checksum_offset_cadence)
  , _checksum_time_cadence(checksum_time_cadence)
  , _batch_counter(0)
  , _last_checksummed(seastar::lowres_clock::now()) {}

void checksum_cadence_stm::update_counter(uint16_t counter_update) {
    _batch_counter += counter_update;
    // let the counter enqueue two checksum indications but not more
    if (_batch_counter > 2 * _checksum_offset_cadence) {
        _batch_counter = 2 * _checksum_offset_cadence;
    }
}

void checksum_cadence_stm::decrement() {
    vassert(_batch_counter >= _checksum_offset_cadence, "underflow");
    _batch_counter -= _checksum_offset_cadence;
}

void checksum_cadence_stm::reset() {
    _batch_counter = 0;
    _last_checksummed = seastar::lowres_clock::now();
}

bool checksum_cadence_stm::check() {
    auto check_result = seastar::lowres_clock::now() - _checksum_time_cadence
                          > _last_checksummed
                        || _batch_counter >= _checksum_offset_cadence;
    return check_result;
}

checksum_controller::checksum_controller(consensus* raft, ctx_log* log)
  : _state(state::off)
  , _raft(raft)
  , _checksum_cadence_stm{1, std::chrono::minutes(15)}
  , _offset_delta_fetcher(raft, log)
  , _log(log) {}

bool checksum_controller::should_replicate_prepare() const {
    return _state == state::should_prepare;
}

bool checksum_controller::should_replicate_checksum() const {
    return _state == state::should_checksum;
}

void checksum_controller::on_batch(const model::record_batch& batch) {
    if (_raft->is_leader()) {
        // is leader but is caught up?
        if (_state == state::off || _state == state::follower) {
            bool is_caught_up = _raft->confirmed_term() == batch.term();

            // if leader and not caught up override to lagging leader
            if (!is_caught_up) {
                _state = state::lagging_leader;
                return;
            }

            // is caught up
            switch (_state) {
                // off and lagging leader -> not_ready
            case state::follower:
                reset(); // if promoted give the new leader a fresh state
                [[fallthrough]];
            case state::off:
                [[fallthrough]];
            case state::lagging_leader:
                _state = state::not_ready;
                [[fallthrough]];
            case state::not_ready: {
                // is leader and is applying current term
                auto counter_update
                  = _offset_delta_fetcher.fetch_counter_update();
                _checksum_cadence_stm.update_counter(counter_update);
                if (_checksum_cadence_stm.check()) {
                    _state = state::should_prepare;
                }
            }
                [[fallthrough]];
            case state::should_prepare:
                [[fallthrough]];
            case state::should_checksum:
                [[fallthrough]];
            case state::wait_for_execute:
                return;
            default:
                vassert(false, "illegal state {}", _state);
                return;
            }
        }
    } else {
        _state = state::follower;
    }
}

void checksum_controller::on_replicate_prepare() {
    vassert(
      _state == state::should_prepare, "illegal state transition {}", _state);
    _state = state::should_checksum;
}

void checksum_controller::on_replicate_checksum() {
    vassert(
      _state == state::should_checksum, "illegal state transition {}", _state);
    _state = state::wait_for_execute;
}

void checksum_controller::on_execute_checksum() {
    // caught up leader can now stop waiting for checksum and issue new
    // prepare
    if (_state == state::wait_for_execute) {
        _state = state::not_ready;
    }
}

void checksum_controller::reset() {
    _checksum_cadence_stm.reset();
    _offset_delta_fetcher.reset();
    _state = state::off;
}

std::ostream& operator<<(std::ostream& o, checksum_controller::state s) {
    switch (s) {
    case checksum_controller::state::off:
        return o << "off";
    case checksum_controller::state::lagging_leader:
        return o << "lagging_leader";
    case checksum_controller::state::not_ready:
        return o << "not_ready";
    case checksum_controller::state::should_prepare:
        return o << "should_prepare";
    case checksum_controller::state::should_checksum:
        return o << "should_checksum";
    case checksum_controller::state::wait_for_execute:
        return o << "wait_for_execute";
    case checksum_controller::state::follower:
        return o << "follower";
        __builtin_unreachable();
    }
}

} // namespace raft

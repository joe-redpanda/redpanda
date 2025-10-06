/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "metrics/metrics.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics_types.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <cstdint>

namespace cloud_topics::l0 {

/// \brief Pipeline probe
///
/// \details This class is used to collect metrics for the level_zero pipeline.
///          It should work for both read and write pipeline. It tracks basic
///          metrics like number of requests, number of errors and processing
///          time. Also, it tracks total memory usage of the pipeline and
///          memory pressure events.
class pipeline_probe {
public:
    /// \brief Probe c-tor
    ///
    /// \param disable is used to switch the internal monitoring off
    /// \param public_disable is used to switch the public monitoring off
    pipeline_probe(std::string_view name, bool disable, bool public_disable);

    void register_request() { ++_requests_in; }
    void register_request_completed() { ++_requests_completed; }
    void register_request_error() { ++_requests_error; }
    void register_request_timeout() { ++_requests_timeout; }
    auto register_request_processing_time() {
        return _request_processing_time.auto_measure();
    }
    void set_memory_usage_gauge(uint64_t mem) { _current_memory_usage = mem; }
    auto register_memory_pressure_blocked(uint64_t mem) {
        _memory_pressure_waits += 1;
        _memory_pressure_blocked += mem;
        return ss::defer([this, mem] { _memory_pressure_blocked -= mem; });
    }
    void register_bytes_in(uint64_t bytes) {
        _total_bytes_in += bytes;
        _request_memory_histogram.record(bytes);
    }
    void register_bytes_out(uint64_t bytes) { _total_bytes_out += bytes; }

private:
    void setup_internal_metrics(bool disable, ss::sstring name);
    void setup_public_metrics(bool disable, ss::sstring name);

    // requests in
    uint64_t _requests_in{0};
    // requests completed successfully
    uint64_t _requests_completed{0};
    // requests completed with error
    uint64_t _requests_error{0};
    // requests timeout errors (requests that spent too long in the pipeline)
    uint64_t _requests_timeout{0};
    // request processing time histogram
    log_hist_client_quota _request_processing_time;
    // current memory usage (gauge)
    uint64_t _current_memory_usage{0};
    // memory pressure (req. waits for semaphore)
    uint64_t _memory_pressure_waits{0};
    // memory pressure (memory blocked by waiting for semaphore)
    uint64_t _memory_pressure_blocked{0};
    // total bytes in (for write pipeline)
    uint64_t _total_bytes_in{0};
    // total bytes out (for read pipeline)
    uint64_t _total_bytes_out{0};
    // request memory histogram (distribution of memory sizes used by requests)
    batch_size_hist _request_memory_histogram;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

/// \brief Throttler probe
/// \details This class is used to collect metrics for the level_zero throttler
///          (src/v/cloud_topics/level_zero/throttler/throttler.{h,cc}).
class throttler_probe {
public:
    explicit throttler_probe(bool disable);
    void register_throttle_event() { ++_throttle_events_count; }
    auto track_throttled_bytes(uint64_t bytes) {
        _requests_throttled_gauge += 1;
        _bytes_throttled_gauge += bytes;
        return ss::defer([this, bytes] {
            _requests_throttled_gauge -= 1;
            _bytes_throttled_gauge -= bytes;
        });
    }

private:
    void setup_internal_metrics(bool disable);
    uint64_t _throttle_events_count{0};
    uint64_t _bytes_throttled_gauge{0};
    uint64_t _requests_throttled_gauge{0};
    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l0

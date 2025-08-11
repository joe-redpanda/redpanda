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
#include "kafka/client/direct_consumer/api_types.h"

#include <seastar/core/condition-variable.hh>

namespace kafka::client {
/**
 * Very simple blocking queue that is used in
 * the direct consumer to store data fetched from the broker. The queue is
 * strictly bounded by the number of elements it may contain and loosely bounded
 * on the maximum data size. At the point of being filled, the queue will force
 * additional pushes to wait.
 */
class data_queue {
public:
    data_queue(size_t max_bytes = 10_MiB, size_t max_count = 10);

    /** true if the queue can accept another entry without waiting */
    bool can_insert_immediately() const;

    /**
     * @brief pushes fetched data onto the queue, blocks if the queue is full
     * @param data the data to be emplaced
     * @param total_bytes the total size of the data to be emplaced, used to
     * limit maximum queue size
     * @throws ss::broken_condition_variable if the queue closes before
     * completion
     */
    ss::future<>
    push(chunked_vector<fetched_topic_data> data, size_t total_bytes);

    /** push operation for the queue, error case, blocks if the queue is full.
     * Throws ss::broken_condition */
    ss::future<> push_error(kafka::error_code ec);

    /**
     * @brief pushes fetched data onto the queue, blocks if there is no data to
     * pop
     * @param timeout how long to wait before throwing a ss::timed_out_error
     * @throws ss::timed_out_error thrown if the timeout is met before data is
     * found
     * @throws ss::broken_condition_variable if the queue closes before
     * completion
     */
    ss::future<fetches> pop(std::chrono::milliseconds timeout);

    /**
     * Stops the queue. All methods will return immediately after this
     * method is called. If the methods is waiting it will throw an
     * ss::broken_condition_variable exception
     */
    void stop();

    /**
     * Configuration setters for the queue.
     */
    void set_max_bytes(size_t bytes);
    void set_max_count(size_t count);

    size_t size() const { return _queue.size(); }

    size_t current_bytes() const { return _current_bytes; }

private:
    struct entry {
        kafka::error_code error_code{kafka::error_code::none};
        chunked_vector<fetched_topic_data> topics;
        size_t total_bytes{0};
    };

    ss::future<> push(entry entry);

    // specifically, does the queue have capacity for one more fetch
    bool has_capacity() const;

    // wake next if appropriate
    void update_next_push();
    void update_next_pop();

    size_t _max_count{10};
    size_t _max_bytes{10_MiB};

    size_t _current_bytes{0};
    ss::chunked_fifo<entry> _queue;

    ss::condition_variable _waiting_to_push;
    ss::condition_variable _waiting_to_pop;
};
} // namespace kafka::client

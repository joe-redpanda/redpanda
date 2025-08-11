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
#include "kafka/client/direct_consumer/data_queue.h"

#include "base/vassert.h"
#include "kafka/protocol/errors.h"

namespace kafka::client {

data_queue::data_queue(size_t max_bytes, size_t max_count)
  : _max_count(max_count)
  , _max_bytes(max_bytes) {}

bool data_queue::has_capacity() const {
    return _queue.empty()
           || (_current_bytes <= _max_bytes && _queue.size() < _max_count);
}

bool data_queue::can_insert_immediately() const {
    return has_capacity() && !_waiting_to_push.has_waiters();
}

ss::future<>
data_queue::push(chunked_vector<fetched_topic_data> data, size_t total_bytes) {
    return push(entry{
      .error_code = kafka::error_code::none,
      .topics = std::move(data),
      .total_bytes = total_bytes});
}

ss::future<> data_queue::push(entry e) {
    if (can_insert_immediately()) {
        _current_bytes += e.total_bytes;
        _queue.push_back(std::move(e));

        // we could elide the checks and call signal directly
        update_next_pop();
        // update_next_push(); no need there are no waiters
        return ss::now();
    }
    return _waiting_to_push.wait().then([this, e = std::move(e)]() mutable {
        vassert(
          has_capacity(),
          "precondition: the queue must NOT be full at the point at which a "
          "push is woken");
        _current_bytes += e.total_bytes;
        _queue.push_back(std::move(e));

        // update waiters
        update_next_pop();
        update_next_push();
    });
}

ss::future<> data_queue::push_error(kafka::error_code ec) {
    return push(entry{.error_code = ec, .topics = {}, .total_bytes = 0u});
}

ss::future<fetches> data_queue::pop(std::chrono::milliseconds timeout) {
    if (_queue.size() == 0 || _waiting_to_pop.has_waiters()) {
        co_await _waiting_to_pop.wait(timeout);
    }
    vassert(
      _queue.size() > 0,
      "precondition: the queue must have an entry on non-error completion of a "
      "wait");

    auto entry = std::move(_queue.front());
    _current_bytes -= entry.total_bytes;
    _queue.pop_front();

    update_next_push();
    update_next_pop();

    if (entry.error_code != kafka::error_code::none) {
        // If there is an error, fill result with error
        co_return entry.error_code;
    }
    auto result = std::move(entry.topics);
    co_return result;
}

void data_queue::stop() {
    _waiting_to_pop.broken();
    _waiting_to_push.broken();
}

void data_queue::set_max_bytes(size_t bytes) {
    _max_bytes = bytes;

    // reasonably the answer to 'can i insert now' may have changed
    update_next_push();
}

void data_queue::set_max_count(size_t count) {
    _max_count = count;

    // reasonably the answer to 'can i insert now' may have changed
    update_next_push();
}

void data_queue::update_next_push() {
    if (has_capacity() && _waiting_to_push.has_waiters()) {
        _waiting_to_push.signal();
    }
}

// wakes the next pop if appropriate
void data_queue::update_next_pop() {
    if (!_queue.empty() && _waiting_to_pop.has_waiters()) {
        _waiting_to_pop.signal();
    }
}

} // namespace kafka::client

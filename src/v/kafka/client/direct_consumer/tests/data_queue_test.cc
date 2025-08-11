// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/direct_consumer/data_queue.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace std::literals::chrono_literals;

using namespace kafka::client;

namespace {

static constexpr size_t max_bytes = 1024;
static constexpr size_t max_count = 10;
static constexpr auto default_timeout = 100ms;

static constexpr size_t default_payload_size = 512;

chunked_vector<fetched_topic_data>
make_data(size_t topic_count, size_t bytes_per_topic) {
    chunked_vector<fetched_topic_data> data;
    data.reserve(topic_count);
    for (auto d = 0u; d < topic_count; ++d) {
        fetched_topic_data topic_data;
        topic_data.topic = model::topic(fmt::format("topic-{}", d));
        topic_data.total_bytes = bytes_per_topic;
        data.push_back(std::move(topic_data));
    }
    return data;
}

auto insert_single_element(data_queue& queue, size_t element_size) {
    return queue.push(make_data(1, element_size), element_size);
}
} // namespace

// check initial state on construction
TEST(DataQueueTest, InitialState) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);
}

// check that push succeeds with size expectations
TEST(DataQueueTest, TestIfCanInsertWorks) {
    data_queue queue(max_bytes, max_count);

    auto f = queue.push(
      make_data(1, default_payload_size), default_payload_size);

    // check on readiness optimation, not the biggest fan
    ASSERT_TRUE(f.available());
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), default_payload_size);
}

// push, check queue, push, check queue, pop, check fifo, pop, check fifo
TEST_CORO(DataQueueTest, TestPushPop) {
    static constexpr size_t fragment_size = 64;
    static constexpr size_t num_fragments = 4;
    static constexpr size_t fragmented_payload_size = num_fragments
                                                      * fragment_size;

    data_queue queue(max_bytes, max_count);

    co_await insert_single_element(queue, default_payload_size);
    ASSERT_EQ_CORO(queue.size(), 1);
    ASSERT_EQ_CORO(queue.current_bytes(), default_payload_size);

    co_await queue.push(
      make_data(num_fragments, fragment_size), fragmented_payload_size);
    ASSERT_EQ_CORO(queue.size(), 2);
    ASSERT_EQ_CORO(
      queue.current_bytes(), fragmented_payload_size + default_payload_size);

    { // check first
        auto fetched = co_await queue.pop(default_timeout);
        ASSERT_EQ_CORO(queue.size(), 1);
        ASSERT_EQ_CORO(queue.current_bytes(), fragmented_payload_size);

        ASSERT_TRUE_CORO(fetched.has_value());
        auto fetched_value = std::move(fetched).value();

        // this was the number of topics emplaced
        ASSERT_EQ_CORO(fetched_value.size(), 1);
        ASSERT_EQ_CORO(fetched_value[0].total_bytes, default_payload_size);
    }
    { // check second
        auto fetched = co_await queue.pop(default_timeout);
        ASSERT_EQ_CORO(queue.size(), 0);
        ASSERT_EQ_CORO(queue.current_bytes(), 0);

        ASSERT_TRUE_CORO(fetched.has_value());
        auto fetched_value = std::move(fetched).value();

        ASSERT_EQ_CORO(fetched_value.size(), num_fragments);
        ASSERT_EQ_CORO(fetched_value[0].total_bytes, fragment_size);
    }
}

// check that errors come through the queue
TEST_CORO(DataQueueTest, TestPushError) {
    data_queue queue(max_bytes, max_count);

    co_await queue.push_error(kafka::error_code::broker_not_available);

    auto fetches = queue.pop(default_timeout).get();

    ASSERT_TRUE_CORO(fetches.has_error());
    ASSERT_EQ_CORO(fetches.error(), kafka::error_code::broker_not_available);
}

TEST_CORO(DataQueueTest, TestPopTimeout) {
    data_queue queue(max_bytes, max_count);

    ASSERT_THROW_CORO(
      std::ignore = co_await queue.pop(default_timeout),
      ss::condition_variable_timed_out);
}

TEST_CORO(DataQueueTest, TestMaxCountLimit) {
    data_queue queue(max_bytes, max_count);

    std::vector<ss::future<>> pushes{};
    for (size_t i = 0; i < max_count + 1; ++i) {
        pushes.emplace_back(queue.push(make_data(1, 10), 10));
    }

    // force the above to either complete or suspend
    co_await tests::drain_task_queue();

    // so we can assert that the queue respects the 'max_count' constraint
    ASSERT_EQ_CORO(queue.size(), max_count);
    ASSERT_EQ_CORO(queue.current_bytes(), max_count * 10);

    auto fetches = queue.pop(default_timeout).get();

    ASSERT_EQ_CORO(fetches.value().size(), 1);

    // yield, allowing the last fiber to resume
    co_await tests::drain_task_queue();

    // which should return us to max_count
    ASSERT_EQ_CORO(queue.size(), max_count);
    ASSERT_EQ_CORO(queue.current_bytes(), max_count * 10);

    // esure the completion of all pushes
    for (size_t i{0}; i < pushes.size(); ++i) {
        co_await std::move(pushes[i]);
    }
}

TEST_CORO(DataQueueTest, TimeoutCVConsumption) {
    data_queue queue(max_bytes, max_count);

    static constexpr auto small_timeout = 100ms;
    static constexpr auto big_timeout = 1s;

    auto should_succeed_1 = queue.pop(big_timeout);
    auto should_fail = queue.pop(small_timeout);
    auto should_succeed_2 = queue.pop(big_timeout);

    co_await ss::sleep(small_timeout);
    // at this point should_fail should already have timed out

    co_await insert_single_element(queue, default_payload_size);
    co_await insert_single_element(queue, default_payload_size);

    std::ignore = co_await std::move(should_succeed_1);

    try {
        std::ignore = co_await std::move(should_fail);
        vassert(false, "this operation should have failed on timeout");
    } catch (const ss::condition_variable_timed_out& e) {
        // timeout is expected
        std::ignore = e;
    }

    std::ignore = co_await std::move(should_succeed_2);
}

TEST_CORO(DataQueueTest, TestConfigurationSetters) {
    static constexpr size_t big_push_size{500};
    static constexpr size_t little_push_size{10};
    data_queue queue(max_bytes, max_count);

    auto big_push = queue.push(make_data(1, big_push_size), big_push_size);

    // make sure the above actually completes
    co_await tests::drain_task_queue();

    queue.set_max_bytes(2048);
    queue.set_max_count(5);

    // start 5 little pushes to put the queue over count capacity;
    std::vector<ss::future<>> little_pushes{};
    for (size_t i{0}; i < 5; ++i) {
        little_pushes.emplace_back(
          queue.push(make_data(1, little_push_size), little_push_size));
    }

    // ensure that the others run through to either completion or suspension
    co_await tests::drain_task_queue();

    // assert that we have 5 fetches, one big and four little
    // one little push should be waiting on more space
    ASSERT_EQ_CORO(queue.size(), 5);
    ASSERT_EQ_CORO(queue.current_bytes(), big_push_size + 4 * little_push_size);

    // we were previously blocked on not enough count, check that adding more
    // count wakes next
    queue.set_max_count(20);

    // yield the shard s.t. the waiting push can wake and execute
    co_await tests::drain_task_queue();

    ASSERT_EQ_CORO(queue.current_bytes(), 500 + 5 * 10);

    // await and complete all pushes
    co_await std::move(big_push);
    for (auto& little_push : little_pushes) {
        co_await std::move(little_push);
    }
}

TEST_CORO(DataQueueTest, TestBlockingPushWhenFull) {
    static constexpr size_t small_payload_size = 50;
    data_queue queue(max_bytes, max_count);

    std::vector<ss::future<>> pushes{};
    for (size_t i = 0; i < max_count; ++i) {
        pushes.emplace_back(insert_single_element(queue, small_payload_size));
    }

    co_await tests::drain_task_queue();

    ASSERT_EQ_CORO(queue.size(), max_count);
    ASSERT_EQ_CORO(queue.current_bytes(), max_count * 50);

    auto push_future = queue.push(make_data(1, 50), 50);
    ASSERT_FALSE_CORO(push_future.available());
    ASSERT_EQ_CORO(queue.size(), max_count);
    ASSERT_EQ_CORO(queue.current_bytes(), max_count * 50);

    std::ignore = queue.pop(default_timeout);
    ASSERT_EQ_CORO(queue.size(), max_count - 1);
    ASSERT_EQ_CORO(queue.current_bytes(), (max_count - 1) * 50);

    co_await std::move(push_future);
    ASSERT_EQ_CORO(queue.size(), max_count);
    ASSERT_EQ_CORO(queue.current_bytes(), max_count * 50);

    for (auto& future : pushes) {
        co_await std::move(future);
    }
}

TEST_CORO(DataQueueTest, TestEdgeCases) {
    static constexpr size_t oversized_size{2048};
    static constexpr size_t small_size{10};

    data_queue queue(max_bytes, max_count);

    { // check that we can put in an element that violates the size constraint

        co_await insert_single_element(queue, oversized_size);
        ASSERT_EQ_CORO(queue.size(), 1);
        ASSERT_EQ_CORO(queue.current_bytes(), oversized_size);

        auto fetches = co_await queue.pop(default_timeout);
        ASSERT_EQ_CORO(fetches.value().size(), 1);
        ASSERT_EQ_CORO(fetches.value()[0].total_bytes, oversized_size);
    }

    { // check that we can put in an element that violates the size constraint
      // even when there is an existing element
        co_await insert_single_element(queue, small_size);
        ASSERT_EQ_CORO(queue.size(), 1);
        ASSERT_EQ_CORO(queue.current_bytes(), small_size);

        co_await insert_single_element(queue, oversized_size);

        ASSERT_EQ_CORO(queue.size(), 2);
        ASSERT_EQ_CORO(queue.current_bytes(), oversized_size + small_size);

        auto will_not_complete = insert_single_element(queue, small_size);

        // show that the above is blocked
        co_await tests::drain_task_queue();
        ASSERT_EQ_CORO(queue.size(), 2);
        ASSERT_EQ_CORO(queue.current_bytes(), oversized_size + small_size);

        // remove enough that the push can complete for cleanup
        std::ignore = co_await queue.pop(default_timeout);
        std::ignore = co_await queue.pop(default_timeout);
        co_await std::move(will_not_complete);
    }
}

TEST(DataQueueTest, TestSizeAndCurrentBytesTracking) {
    data_queue queue(max_bytes, max_count);

    queue.push(make_data(2, 100), 200).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 200);

    queue.push(make_data(1, 300), 300).get();
    ASSERT_EQ(queue.size(), 2);
    ASSERT_EQ(queue.current_bytes(), 500);

    queue.push_error(kafka::error_code::request_timed_out).get();
    ASSERT_EQ(queue.size(), 3);
    ASSERT_EQ(queue.current_bytes(), 500);

    queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 2);
    ASSERT_EQ(queue.current_bytes(), 300);

    queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 0);

    queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);
}

namespace {

// shove a writer index & counter into a size
size_t package(uint32_t writer_index, uint32_t counter) {
    static_assert(sizeof(size_t) == 2 * sizeof(uint32_t));
    return (static_cast<size_t>(writer_index) << 32u) + counter;
}

// opposite of package
auto unpackage(size_t snuck_payload) {
    static_assert(sizeof(size_t) == 2 * sizeof(uint32_t));
    uint32_t counter = static_cast<uint32_t>(snuck_payload);
    uint32_t writer_index = static_cast<uint32_t>(snuck_payload >> 32u);
    return std::tuple<uint32_t, uint32_t>{writer_index, counter};
}

} // namespace

// set up multiple writers and single reader on a single queue, check
// monotonicity of writes
TEST_CORO(DataQueueTest, TestChaos) {
    // test constants
    static constexpr size_t payload_size{128};
    static constexpr size_t max_bytes = 1028 * 4; // intentional shadow
    static constexpr std::chrono::duration phase_runtime = 5s;
    static constexpr size_t number_of_writers{3};

    // the writer information gets snuck into the size of the first partition,
    // make helpers to package and unpackage
    static constexpr auto evil_make_data =
      [](uint32_t writer_index, uint32_t counter) {
          auto data = make_data(1, random_generators::get_int(max_bytes));
          data[0].total_bytes = package(writer_index, counter);
          return data;
      };
    static constexpr auto evil_unpack_writer_info =
      [](const chunked_vector<fetched_topic_data>& data) {
          return unpackage(data[0].total_bytes);
      };

    data_queue queue(max_bytes, max_count);

    // event variables for stopping bg fibers
    bool should_push{true};
    bool should_pop{true};

    // keep track of the offset of the writers, one for writing the other for
    // checking
    std::vector<uint32_t> push_counters(number_of_writers, 0);
    std::vector<uint32_t> pop_counters(number_of_writers, 0);

    // tweak the delays so the proportion of pops to pushes can be tweaked
    uint push_delay_multiplier{1};
    uint pop_delay_multiplier{1};

    // this is fine so long as the lambda itself lives longer than any spawned
    // execution
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
    auto push_fiber = [&](size_t writer_index) -> ss::future<> {
        while (should_push) {
            auto data = evil_make_data(
              writer_index, push_counters[writer_index]++);
            co_await queue.push(std::move(data), payload_size);
            co_await ss::sleep(std::chrono::milliseconds(
              random_generators::get_int(100) * push_delay_multiplier));
        }
        co_return;
    };

    // this is fine so long as the lambda itself lives longer than any spawned
    // execution
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
    auto pop_fiber = [&] -> ss::future<> {
        while (should_pop) {
            std::optional<fetches> maybe_maybe_data{std::nullopt};
            // the pop may legitimately be starved, longer timeout needed
            try {
                maybe_maybe_data = co_await queue.pop(
                  std::chrono::milliseconds(1000));
            } catch (ss::timed_out_error) {
                // time out is fine and expected
                continue;
            }

            // optional<result> fiddling
            vassert(
              maybe_maybe_data.has_value(),
              "at this point we should have a return value");
            auto maybe_data = std::move(maybe_maybe_data).value();
            vassert(maybe_data.has_value(), "no errors should be written");
            auto data = std::move(maybe_data).value();

            // unpack and check
            auto [writer_index, found_counter] = evil_unpack_writer_info(data);
            vassert(
              found_counter == pop_counters[writer_index]++,
              "data from queue out of order");

            // adjust sleep by the number of writers to keep the proportion even
            // by default
            co_await ss::sleep(std::chrono::milliseconds(
              random_generators::get_int(100) * pop_delay_multiplier
              / number_of_writers));
        }
        co_return;
    };

    // first phase, even push and pop
    std::vector<ss::future<>> push_fibers{};
    for (uint i{0}; i < number_of_writers; ++i) {
        push_fibers.emplace_back(push_fiber(i));
    }

    auto pop_fiber_task = pop_fiber();

    co_await ss::sleep(phase_runtime);

    // second phase, pop preferenced
    push_delay_multiplier = 2;
    pop_delay_multiplier = 1;
    co_await ss::sleep(phase_runtime);

    // third phase, push preferenced
    push_delay_multiplier = 1;
    pop_delay_multiplier = 2;
    co_await ss::sleep(phase_runtime);

    // shut down
    should_push = false;
    for (auto& push_fiber_task : push_fibers) {
        co_await std::move(push_fiber_task);
    }

    should_pop = false;
    co_await std::move(pop_fiber_task);
}

// check that the queue is fair in that no writer gets starved
TEST_CORO(DataQueueTest, QueueUnfairness) {
    static constexpr size_t small_size = 128u;
    static constexpr size_t big_size = 2048;

    data_queue queue(max_bytes, max_count);

    // drop a small record in the queue
    co_await insert_single_element(queue, small_size);
    co_await insert_single_element(queue, big_size);

    // attempt to push an oversized record, and keep tabs on its successful push
    auto big_push = insert_single_element(queue, big_size);

    ASSERT_FALSE_CORO(big_push.available());

    // start a loop that will continuously put small data on and then take small
    // data off. If the queue were fair, the small data pushes should be blocked
    // until the large data gets a chance to enter the queue
    bool should_keep_contending{true};
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
    auto contention_lambda = [&should_keep_contending, &queue] -> ss::future<> {
        while (should_keep_contending) {
            co_await ss::yield();
            std::ignore = queue.pop(default_timeout)
                            .discard_result()
                            .handle_exception_type(
                              [](ss::timed_out_error) { return ss::now(); })
                            .handle_exception_type(
                              [](ss::broken_condition_variable) {
                                  return ss::now();
                              });
            std::ignore = insert_single_element(queue, small_size)
                            .discard_result()
                            .handle_exception_type(
                              [](ss::timed_out_error) { return ss::now(); })
                            .handle_exception_type(
                              [](ss::broken_condition_variable) {
                                  return ss::now();
                              });
        }
        co_return;
    };
    auto contention_fiber = contention_lambda();

    // wait to see if big_push completes within a reasonable amount of time
    try {
        co_await ss::with_timeout(
          seastar::lowres_clock::now() + 10s, std::move(big_push));
    } catch (const ss::timed_out_error& /*ignored*/) {
        ASSERT_TRUE_CORO(false);
    }

    // stop the contention fiber, and wait for its termination
    should_keep_contending = false;
    co_await std::move(contention_fiber);

    co_await tests::drain_task_queue();
}

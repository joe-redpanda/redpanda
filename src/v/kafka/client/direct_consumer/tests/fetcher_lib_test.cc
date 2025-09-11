

#include "kafka/client/direct_consumer/api_types.h"
#include "kafka/client/direct_consumer/fetcher.h"
#include "model/fundamental.h"
#include "utils/prefix_logger.h"

#include <gtest/gtest.h>

using namespace kafka::client;

namespace {
ss::logger _logger{"fetcher_lib_test"};
prefix_logger _prefix_logger(_logger, "");

static const model::topic test_topic = model::topic("test_topic");
static constexpr model::partition_id test_partition = model::partition_id{0};
static constexpr model::node_id broker_id{0};

fetcher::partition_fetch_state& assign_ntp(
  topic_partition_map<fetcher::partition_fetch_state>& to_assign,
  const model::topic& topic,
  model::partition_id partition_id) {
    to_assign[topic][partition_id] = fetcher::partition_fetch_state{};
    return to_assign[topic][partition_id];
}

template<typename collection_t>
auto find_element(
  const collection_t& collection,
  const model::topic& topic,
  model::partition_id partition_id) {
    using t_it_t = decltype(std::declval<collection_t>().find(model::topic{}));
    using p_map_t = decltype(std::declval<t_it_t>()->second);
    using p_it_t = decltype(std::declval<p_map_t>().find(
      model::partition_id()));
    using return_t = decltype(std::declval<p_it_t>()->second);

    auto t_it = collection.find(topic);
    if (t_it == collection.end()) {
        return std::optional<return_t>{std::nullopt};
    }

    auto p_it = t_it->second.find(partition_id);
    if (p_it == t_it->second.end()) {
        return std::optional<return_t>{std::nullopt};
    }

    return std::optional<return_t>{p_it->second};
}

} // namespace

TEST(DataQueueTest, BothTrue) {
    topic_partition_map<fetcher::partition_fetch_state> fetcher_state{};
    topic_partition_map<model::partition_id> partitions_to_forget{};

    auto& fetch_state = assign_ntp(fetcher_state, test_topic, test_partition);
    fetch_state.assignment_epoch = fetcher::assignment_epoch{1};
    fetch_state.fetch_offset = kafka::offset{0};
    fetch_state.high_watermark = kafka::offset{10};
    fetch_state.incremental_include = true;
    fetch_state.partition_id = test_partition;

    auto result
      = fetcher::collect_partitons_inner(
          fetcher_state, partitions_to_forget, true, _prefix_logger, broker_id)
          .get();

    auto maybe_epoch = find_element(
      result.assignment_epochs, test_topic, test_partition);
    ASSERT_TRUE(maybe_epoch.has_value());
    ASSERT_EQ(*maybe_epoch, fetch_state.assignment_epoch);

    auto found_fetch_state = *(
      result.partitions.begin()->to_include_in_fetch.begin());

    ASSERT_EQ(found_fetch_state.fetch_offset, fetch_state.fetch_offset);
    ASSERT_EQ(found_fetch_state.partition_id, test_partition);
}

TEST(DataQueueTest, HWM) {
    topic_partition_map<fetcher::partition_fetch_state> fetcher_state{};
    topic_partition_map<model::partition_id> partitions_to_forget{};

    auto& fetch_state = assign_ntp(fetcher_state, test_topic, test_partition);
    fetch_state.assignment_epoch = fetcher::assignment_epoch{1};
    fetch_state.fetch_offset = kafka::offset{0};
    fetch_state.high_watermark = kafka::offset{10};
    fetch_state.incremental_include = false;
    fetch_state.partition_id = test_partition;

    auto result
      = fetcher::collect_partitons_inner(
          fetcher_state, partitions_to_forget, true, _prefix_logger, broker_id)
          .get();

    auto maybe_epoch = find_element(
      result.assignment_epochs, test_topic, test_partition);
    ASSERT_TRUE(maybe_epoch.has_value());
    ASSERT_EQ(*maybe_epoch, fetch_state.assignment_epoch);

    auto found_fetch_state = *(
      result.partitions.begin()->to_include_in_fetch.begin());

    ASSERT_EQ(found_fetch_state.fetch_offset, fetch_state.fetch_offset);
    ASSERT_EQ(found_fetch_state.partition_id, test_partition);
}

TEST(DataQueueTest, Include) {
    topic_partition_map<fetcher::partition_fetch_state> fetcher_state{};
    topic_partition_map<model::partition_id> partitions_to_forget{};

    auto& fetch_state = assign_ntp(fetcher_state, test_topic, test_partition);
    fetch_state.assignment_epoch = fetcher::assignment_epoch{1};
    fetch_state.fetch_offset = kafka::offset{10};
    fetch_state.high_watermark = kafka::offset{10};
    fetch_state.incremental_include = true;
    fetch_state.partition_id = test_partition;

    auto result
      = fetcher::collect_partitons_inner(
          fetcher_state, partitions_to_forget, true, _prefix_logger, broker_id)
          .get();

    auto maybe_epoch = find_element(
      result.assignment_epochs, test_topic, test_partition);
    ASSERT_TRUE(maybe_epoch.has_value());
    ASSERT_EQ(*maybe_epoch, fetch_state.assignment_epoch);

    auto found_fetch_state = *(
      result.partitions.begin()->to_include_in_fetch.begin());

    ASSERT_EQ(found_fetch_state.fetch_offset, fetch_state.fetch_offset);
    ASSERT_EQ(found_fetch_state.partition_id, test_partition);
}

TEST(DataQueueTest, Disclude) {
    topic_partition_map<fetcher::partition_fetch_state> fetcher_state{};
    topic_partition_map<model::partition_id> partitions_to_forget{};

    auto& fetch_state = assign_ntp(fetcher_state, test_topic, test_partition);
    fetch_state.assignment_epoch = fetcher::assignment_epoch{1};
    fetch_state.fetch_offset = kafka::offset{10};
    fetch_state.high_watermark = kafka::offset{10};
    fetch_state.incremental_include = false;
    fetch_state.partition_id = test_partition;

    auto result
      = fetcher::collect_partitons_inner(
          fetcher_state, partitions_to_forget, true, _prefix_logger, broker_id)
          .get();

    auto maybe_epoch = find_element(
      result.assignment_epochs, test_topic, test_partition);
    ASSERT_TRUE(maybe_epoch.has_value());
    ASSERT_EQ(*maybe_epoch, fetch_state.assignment_epoch);

    ASSERT_EQ(result.partitions.size(), 0);
}

TEST(DataQueueTest, Corruption) {
    topic_partition_map<fetcher::partition_fetch_state> fetcher_state{};
    topic_partition_map<model::partition_id> partitions_to_forget{};

    int iterations = 100;
    for (int i{0}; i < iterations; ++i) {
        auto pid = model::partition_id{i};
        auto& fetch_state = assign_ntp(fetcher_state, test_topic, pid);
        fetch_state.assignment_epoch = fetcher::assignment_epoch{1};
        fetch_state.fetch_offset = kafka::offset{0};
        fetch_state.high_watermark = kafka::offset{10};
        fetch_state.incremental_include = true;
        fetch_state.partition_id = pid;
    }

    auto result
      = fetcher::collect_partitons_inner(
          fetcher_state, partitions_to_forget, true, _prefix_logger, broker_id)
          .get();

    auto& for_topic = result.partitions[0];

    int counter{0};
    auto iterator = for_topic.to_include_in_fetch.begin();
    for (; iterator != for_topic.to_include_in_fetch.end(); ++iterator) {
        auto& found_state = *iterator;
        _logger.info("found pid: {}", found_state.partition_id);
        if (found_state.partition_id == model::partition_id{49}) {
            fetcher_state[test_topic].erase(model::partition_id{25});
            fetcher_state[test_topic].erase(model::partition_id{10});
            fetcher_state[test_topic].erase(model::partition_id{62});
            fetcher_state[test_topic].erase(model::partition_id{50});
        }
        /*ASSERT_EQ(
          partition_to_include.partition_id, model::partition_id{counter});*/
        ++counter;
    }
    ASSERT_EQ(counter, iterations - 1);
}

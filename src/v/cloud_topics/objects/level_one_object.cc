/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/objects/level_one_object.h"

#include "base/vassert.h"
#include "bytes/iostream.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "serde/rw/rw.h"
#include "serde/rw/vector.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include <fmt/format.h>

#include <algorithm>
#include <bit>
#include <cstring>

namespace experimental::cloud_topics::l1 {

namespace {
constexpr static std::string_view partition_marker_key = "pm";
constexpr static std::string_view footer_key = "f";
} // namespace

object_index::partition object_index::partition::copy() const {
    return {
      .ntp = ntp,
      .file_position = file_position,
      .indexes = indexes.copy(),
      .first_offset = first_offset,
      .last_offset = last_offset,
      .max_timestamp = max_timestamp,
    };
}

size_t object_index::file_position_before_kafka_offset(
  const model::ntp& ntp, kafka::offset target) {
    for (auto& partition : partitions) {
        if (partition.ntp != ntp) {
            continue;
        }
        if (target < partition.first_offset || target > partition.last_offset) {
            continue;
        }
        auto rev = std::views::reverse(partition.indexes);
        auto index_it = std::ranges::lower_bound(
          rev, target, std::greater<>{}, [](const auto& entry) {
              return entry.kafka_offset;
          });
        if (index_it == rev.end()) {
            return partition.file_position;
        }
        return index_it->file_position;
    }
    // If there is a partition that starts before
    return npos;
}

size_t object_index::file_position_before_max_timestamp(
  const model::ntp& ntp, model::timestamp target) {
    auto filtered = std::views::filter(
      partitions, [&ntp, &target](const auto& p) {
          return p.ntp == ntp && target <= p.max_timestamp;
      });
    auto min_it = std::ranges::min_element(
      filtered, std::less<>{}, [](const auto& p) { return p.first_offset; });
    if (min_it == filtered.end()) {
        return npos;
    }
    const auto& index = min_it->indexes;
    auto it = std::ranges::lower_bound(
      index, target, std::less<>{}, [](const auto& entry) {
          return entry.max_timestamp;
      });
    // If we're past all index entries, but still within the recorded file
    // bounds, the best we can do is start at the last well known offset (the
    // last index entry).
    if (it == index.end()) {
        return min_it->indexes.back().file_position;
    }
    // If at the first entry, we must start at the file beginning, because the
    // the max is inclusive of those entries.
    if (it == index.begin()) {
        return min_it->file_position;
    }
    return it->file_position;
}

object_index object_index::copy() const {
    object_index copy;
    copy.partitions.reserve(partitions.size());
    for (const auto& p : partitions) {
        copy.partitions.push_back(p.copy());
    }
    return copy;
}

std::variant<object_index, size_t> object_index::read_footer(iobuf buf) {
    if (buf.size_bytes() < sizeof(uint32_t)) {
        throw std::runtime_error(
          fmt::format(
            "expected at least {} bytes in footer, got: {}",
            sizeof(uint32_t),
            buf.size_bytes()));
    }
    iobuf_const_parser parser(buf);
    auto footer_size
      = iobuf_parser(buf.tail(sizeof(uint32_t))).consume_type<uint32_t>();
    if (buf.size_bytes() >= (footer_size + sizeof(uint32_t))) {
        iobuf_parser p(buf.share(
          buf.size_bytes() - sizeof(uint32_t) - footer_size, footer_size));
        auto footer_batch = serde::read<model::record_batch>(p);
        if (
          footer_batch.header().type
          != model::record_batch_type::cloud_topics_l1_metadata) {
            throw std::runtime_error(
              fmt::format(
                "expected cloud_topics_l1_metadata batch, got: {}",
                footer_batch.header().type));
        }
        if (footer_batch.record_count() != 1) {
            throw std::runtime_error(
              fmt::format(
                "expected 1 record in footer, got: {}",
                footer_batch.record_count()));
        }
        auto footer_record = std::move(footer_batch.copy_records().front());
        if (footer_record.key() != footer_key) {
            throw std::runtime_error(
              fmt::format(
                "expected footer key to be '{}', got: {}",
                footer_key,
                footer_record.key().hexdump(32)));
        }
        return serde::from_iobuf<object_index>(footer_record.release_value());
    }
    return (footer_size + sizeof(uint32_t)) - buf.size_bytes();
}

namespace {

class object_builder_impl final : public object_builder {
public:
    explicit object_builder_impl(ss::output_stream<char> output, options opts)
      : _output(std::move(output))
      , _opts(opts) {}

    ss::future<> start_partition(model::ntp ntp) final {
        end_partition();
        storage::record_batch_builder builder(
          model::record_batch_type::cloud_topics_l1_metadata, model::offset(0));
        builder.add_raw_kv(
          iobuf::from(partition_marker_key), serde::to_iobuf(ntp));
        co_await serde_write_to_stream(std::move(builder).build());
        _current_partition = {
          .ntp = std::move(ntp),
          .file_position = _offset,
          .indexes = {},
          .first_offset = {},
          .last_offset = {},
          .max_timestamp = model::timestamp::missing(),
        };
    }

    ss::future<> add_batch(model::record_batch batch) final {
        dassert(
          batch.header().type == model::record_batch_type::raft_data,
          "expected raft_data batches, got: {}",
          batch.header().type);
        dassert(
          _current_partition.ntp != model::ntp{},
          "wrote a data batch without starting a partition");
        dassert(
          _current_partition.last_offset
            < model::offset_cast(batch.base_offset()),
          "wrote an offset out of order within a batch: {} < {}",
          _current_partition.last_offset,
          model::offset_cast(batch.base_offset()));
        if (_current_partition.first_offset == kafka::offset{}) {
            _current_partition.first_offset = model::offset_cast(
              batch.header().base_offset);
        }
        _current_partition.last_offset = model::offset_cast(
          batch.header().base_offset
          + model::offset_delta(batch.header().last_offset_delta));
        _current_partition.max_timestamp = std::max(
          _current_partition.max_timestamp, batch.header().max_timestamp);
        auto last_index_write_position
          = _current_partition.indexes.empty()
              ? _current_partition.file_position
              : _current_partition.indexes.back().file_position;
        if ((_offset - last_index_write_position) >= _opts.indexing_frequency) {
            _current_partition.indexes.push_back(
              object_index::partition::index_entry{
                .file_position = _offset,
                .kafka_offset = model::offset_cast(batch.header().base_offset),
                .max_timestamp = _current_partition.max_timestamp});
        }
        co_await serde_write_to_stream(std::move(batch));
    }

    ss::future<object_info> finish() final {
        end_partition();
        storage::record_batch_builder builder(
          model::record_batch_type::cloud_topics_l1_metadata, model::offset(0));
        builder.add_raw_kv(
          iobuf::from(footer_key),
          serde::to_iobuf<object_index>(_index.copy()));
        auto footer_start = _offset + sizeof(uint32_t);
        co_await serde_write_to_stream(std::move(builder).build());
        object_info info{
          .index = std::move(_index),
          .footer_offset = footer_start,
          .size_bytes = _offset + sizeof(uint32_t),
        };
        auto footer_size
          = std::bit_cast<std::array<char, sizeof(uint32_t)>, uint32_t>(
            _offset - footer_start);
        co_await _output.write(footer_size.data(), footer_size.size());
        co_return info;
    }

    ss::future<> close() final { return _output.close(); }

private:
    void end_partition() {
        if (_current_partition.ntp == model::ntp{}) {
            return;
        }
        _index.partitions.push_back(std::exchange(_current_partition, {}));
    }

    template<typename T>
    ss::future<> serde_write_to_stream(T&& t) {
        iobuf b;
        auto placeholder = b.reserve(sizeof(uint32_t));
        serde::write(b, std::forward<T>(t));
        std::ranges::copy(
          std::bit_cast<std::array<char, sizeof(uint32_t)>, uint32_t>(
            b.size_bytes() - sizeof(uint32_t)),
          placeholder.mutable_index());
        _offset += b.size_bytes();
        return write_iobuf_to_output_stream(std::move(b), _output);
    }

    size_t _offset = 0;
    ss::output_stream<char> _output;
    object_index _index;
    object_index::partition _current_partition;
    options _opts;
};

} // namespace

std::unique_ptr<object_builder>
object_builder::create(ss::output_stream<char> output, options opts) {
    return std::make_unique<object_builder_impl>(std::move(output), opts);
}

namespace {

class object_reader_impl : public object_reader {
public:
    explicit object_reader_impl(ss::input_stream<char> input)
      : _input(std::move(input)) {}

    ss::future<result> read_next() final {
        auto batch = co_await read_next_batch();
        if (
          batch.header().type
          != model::record_batch_type::cloud_topics_l1_metadata) {
            co_return batch;
        }
        vassert(
          batch.record_count() == 1,
          "expected a single record in l1 metadata batch, got: {}",
          batch.record_count());
        model::record r = std::move(batch.copy_records().front());
        auto parser = iobuf_parser(r.release_value());
        if (r.key() == partition_marker_key) {
            co_return serde::read<model::ntp>(parser);
        } else if (r.key() == footer_key) {
            co_return serde::read<object_index>(parser);
        } else {
            constexpr static size_t max_key_size = 32;
            throw std::runtime_error(
              fmt::format(
                "unexpected key in l1 metadata batch: {}",
                r.key().hexdump(max_key_size)));
        }
    }

    ss::future<> close() final { return _input.close(); }

private:
    ss::future<model::record_batch> read_next_batch() {
        ss::temporary_buffer<char> size_prefix_buf
          = co_await _input.read_exactly(sizeof(uint32_t));
        if (size_prefix_buf.size() != sizeof(uint32_t)) {
            throw std::runtime_error(
              fmt::format(
                "expected {} bytes, got {}",
                sizeof(uint32_t),
                size_prefix_buf.size()));
        }
        uint32_t size = 0;
        std::memcpy(&size, size_prefix_buf.get(), size_prefix_buf.size());
        auto parser = iobuf_parser{co_await read_iobuf_exactly(_input, size)};
        co_return serde::read<model::record_batch>(parser);
    }

    ss::input_stream<char> _input;
};

} // namespace

std::unique_ptr<object_reader>
object_reader::create(ss::input_stream<char> input) {
    return std::make_unique<object_reader_impl>(std::move(input));
}

ss::future<std::unique_ptr<object_reader>>
object_reader::create(std::filesystem::path p, size_t offset) {
    auto f = co_await ss::open_file_dma(p.native(), ss::open_flags::ro);
    co_return create(ss::make_file_input_stream(std::move(f), offset));
}

std::unique_ptr<object_reader>
object_reader::create(ss::file f, size_t offset) {
    return create(ss::make_file_input_stream(std::move(f), offset));
}

} // namespace experimental::cloud_topics::l1

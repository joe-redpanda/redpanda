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

#include "lsm/core/internal/files.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/io/persistence.h"
#include "utils/uuid.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using namespace lsm::io;
using lsm::internal::file_id;
using lsm::internal::operator""_file_id;

using data_persistence_factory
  = std::function<ss::future<std::unique_ptr<data_persistence>>()>;

class PersistenceTest
  : public ::testing::TestWithParam<data_persistence_factory> {
protected:
    void SetUp() override { persistence = GetParam()().get(); }

    void TearDown() override {
        if (persistence) {
            persistence->close().get();
        }
    }

    ss::future<std::vector<file_id>> list_files() {
        auto gen = persistence->list_files();
        std::vector<file_id> files;
        while (auto file = co_await gen()) {
            files.push_back(*file);
        }
        co_return files;
    }

    std::unique_ptr<data_persistence> persistence;
};

TEST_P(PersistenceTest, CanWriteAndReadAFile) {
    {
        auto w = persistence->open_sequential_writer(0_file_id).get();
        auto _ = ss::defer([&w] { w->close().get(); });
        w->append(iobuf::from("hello")).get();
        w->append(iobuf::from("world")).get();
    }
    {
        auto maybe_r = persistence->open_random_access_reader(0_file_id).get();
        ASSERT_TRUE(bool(maybe_r));
        auto r = std::move(*maybe_r);
        auto _ = ss::defer([&r] { r->close().get(); });
        auto buf = r->read(1, 4).get().as_iobuf();
        EXPECT_EQ(buf, iobuf::from("ello")) << buf.hexdump(32);
        buf = r->read(5, 5).get().as_iobuf();
        EXPECT_EQ(buf, iobuf::from("world")) << buf.hexdump(32);
        EXPECT_ANY_THROW(r->read(8, 4).get());
    }
}

TEST_P(PersistenceTest, ListFiles) {
    std::vector<file_id> files;
    {
        for (auto i = 0_file_id; i < 25_file_id; ++i) {
            files.emplace_back(i);
            auto w = persistence->open_sequential_writer(i).get();
            auto _ = ss::defer([&w] { w->close().get(); });
            w->append(iobuf::from(fmt::format("hello, world: {}", i))).get();
        }
    }
    EXPECT_THAT(list_files().get(), testing::UnorderedElementsAreArray(files));
    persistence->remove_file(10_file_id).get();
    files.erase(files.begin() + 10);
    EXPECT_THAT(list_files().get(), testing::UnorderedElementsAreArray(files));
}

TEST_P(PersistenceTest, OverwriteFile) {
    for (int i = 0; i < 3; ++i) {
        auto w = persistence->open_sequential_writer(0_file_id).get();
        auto _ = ss::defer([&w] { w->close().get(); });
        w->append(iobuf::from(fmt::format("hello, world: {}", i))).get();
    }
    auto maybe_r = persistence->open_random_access_reader(0_file_id).get();
    ASSERT_TRUE(bool(maybe_r));
    auto r = std::move(*maybe_r);
    auto _ = ss::defer([&r] { r->close().get(); });
    auto buf = r->read(0, 15).get();
    EXPECT_EQ(buf.as_iobuf(), iobuf::from("hello, world: 2"))
      << buf.as_iobuf().hexdump(32);
}

TEST_P(PersistenceTest, ReadNonExisting) {
    auto maybe_r = persistence->open_random_access_reader(0_file_id).get();
    EXPECT_FALSE(bool(maybe_r));
}

TEST_P(PersistenceTest, RandomAccessReaderComprehensive) {
    // Create a single 8MiB file with predictable content
    constexpr size_t file_size = 8_MiB;

    // Create file with a pattern that's easy to verify
    {
        auto w = persistence->open_sequential_writer(0_file_id).get();
        auto _ = ss::defer([&w] { w->close().get(); });
        iobuf content;
        // Build content in chunks for efficiency
        constexpr size_t chunk_size = 4096;
        std::string chunk;
        chunk.reserve(chunk_size);
        for (size_t i = 0; i < file_size; ++i) {
            // Create a repeating pattern: a-z repeated
            chunk.push_back('a' + (i % 26));
            if (chunk.size() == chunk_size) {
                content.append(chunk.data(), chunk.size());
                chunk.clear();
            }
        }
        if (!chunk.empty()) {
            content.append(chunk.data(), chunk.size());
        }
        w->append(std::move(content)).get();
    }

    // Open reader for all tests
    auto maybe_r = persistence->open_random_access_reader(0_file_id).get();
    ASSERT_TRUE(bool(maybe_r));
    auto r = std::move(*maybe_r);

    // Test many different offset/length combinations
    std::vector<std::pair<size_t, size_t>> test_cases;

    // Small reads at various alignments
    for (auto offset : {0, 1, 2, 3, 4, 5, 7, 15, 31, 63, 127, 255, 511}) {
        for (auto length : {1, 2, 4, 8, 16, 32, 64, 128, 256}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads at DMA alignment boundaries (512 bytes)
    for (auto offset :
         {510, 511, 512, 513, 514, 1022, 1023, 1024, 1025, 1026}) {
        for (auto length :
             {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads at page alignment boundaries (4096 bytes)
    for (auto offset : {4094, 4095, 4096, 4097, 4098, 8190, 8191, 8192, 8193}) {
        for (auto length : {1, 2, 4, 8, 512, 1024, 2048, 4096, 8192}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads around ioarray chunk boundaries (128 KiB)
    for (auto offset :
         {128_KiB - 10,
          128_KiB - 1,
          128_KiB,
          128_KiB + 1,
          128_KiB + 10,
          256_KiB - 10,
          256_KiB - 1,
          256_KiB,
          256_KiB + 1,
          256_KiB + 10}) {
        for (auto length : std::to_array<size_t>(
               {1, 10, 100, 1024, 4096, 8192, 64_KiB, 128_KiB, 256_KiB})) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Large reads at various offsets
    for (auto offset :
         std::to_array<size_t>({0, 1, 511, 512, 4095, 4096, 128_KiB, 1_MiB})) {
        for (auto length : {128_KiB, 256_KiB, 512_KiB, 1_MiB, 2_MiB, 4_MiB}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads near end of file
    for (auto offset :
         {file_size - 1_MiB,
          file_size - 128_KiB,
          file_size - 4096,
          file_size - 512,
          file_size - 100,
          file_size - 10,
          file_size - 1}) {
        for (auto length :
             std::to_array<size_t>({1, 10, 100, 512, 4096, 128_KiB})) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Test all valid cases
    for (const auto& [offset, length] : test_cases) {
        if (offset + length > file_size) {
            continue;
        }

        // Read and verify the data
        auto array = r->read(offset, length).get();
        ASSERT_EQ(array.size(), length)
          << "offset=" << offset << " length=" << length;

        // Verify the content matches the expected pattern
        size_t i = 0;
        for (char c : array.as_range()) {
            char expected = 'a' + ((offset + i) % 26);
            ASSERT_EQ(c, expected) << "offset=" << offset
                                   << " length=" << length << " position=" << i;
            ++i;
        }

        // Sanity check: we read exactly length bytes
        ASSERT_EQ(i, length) << "offset=" << offset << " length=" << length;
    }

    // Test reading past end of file - should throw
    EXPECT_ANY_THROW(r->read(file_size, 1).get());
    EXPECT_ANY_THROW(r->read(file_size - 100, 200).get());
    EXPECT_ANY_THROW(r->read(0, file_size + 1).get());

    // Close reader before removing file
    r->close().get();

    // Clean up
    persistence->remove_file(0_file_id).get();
}

INSTANTIATE_TEST_SUITE_P(
  PersistenceSuite,
  PersistenceTest,
  testing::Values(
    [] { return ss::as_ready_future(make_memory_data_persistence()); },
    [] {
        std::filesystem::path tmpdir = std::getenv("TEST_TMPDIR");
        // Ensure each testcase has it's own directory.
        auto subdir = ss::sstring(uuid_t::create());
        return open_disk_data_persistence(tmpdir / std::string_view(subdir));
    }),
  [](const testing::TestParamInfo<data_persistence_factory>& info) {
      switch (info.index) {
      case 0:
          return "memory";
      case 1:
          return "disk";
      default:
          return "unknown";
      }
  });

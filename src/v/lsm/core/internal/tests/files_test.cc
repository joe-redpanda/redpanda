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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

namespace {
using namespace lsm::internal;
using ::testing::AllOf;
using ::testing::Eq;
using ::testing::Field;
using ::testing::Optional;
} // namespace

TEST(Files, SstFileName) {
    EXPECT_EQ("00000000000000000001.sst", sst_file_name(1_file_id));
    EXPECT_EQ("00000000000000000123.sst", sst_file_name(123_file_id));
    EXPECT_EQ("00000000000000000000.sst", sst_file_name(0_file_id));
    EXPECT_EQ(
      "18446744073709551615.sst",
      sst_file_name(file_id{std::numeric_limits<uint64_t>::max()}));
}

TEST(Files, ParseFilename_SstFile) {
    EXPECT_THAT(
      parse_sst_file_name(sst_file_name(1_file_id)), Optional(1_file_id));

    EXPECT_THAT(
      parse_sst_file_name(sst_file_name(123_file_id)), Optional(123_file_id));

    EXPECT_THAT(
      parse_sst_file_name(
        sst_file_name(file_id{std::numeric_limits<uint64_t>::max()})),
      Optional(file_id{std::numeric_limits<uint64_t>::max()}));
}

TEST(Files, ParseFilename_Invalid) {
    // No extension
    EXPECT_EQ(std::nullopt, parse_sst_file_name("noextension"));

    // Unknown extension
    EXPECT_EQ(std::nullopt, parse_sst_file_name("file.txt"));
    EXPECT_EQ(std::nullopt, parse_sst_file_name("00000000000000000001.log"));

    // Invalid numeric ID for sst
    EXPECT_EQ(std::nullopt, parse_sst_file_name("notanumber.sst"));
    EXPECT_EQ(std::nullopt, parse_sst_file_name("abc123.sst"));

    // Empty string
    EXPECT_EQ(std::nullopt, parse_sst_file_name(""));

    // Just extension
    EXPECT_EQ(std::nullopt, parse_sst_file_name(".sst"));
}

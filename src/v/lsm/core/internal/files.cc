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

#include "absl/strings/numbers.h"

#include <seastar/core/format.hh>

namespace lsm::internal {

ss::sstring sst_file_name(file_id id) noexcept {
    return ss::format("{:020}.sst", id());
}

std::optional<file_id> parse_sst_file_name(std::string_view filename) noexcept {
    size_t pos = filename.find_last_of('.');
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    std::string_view name = filename.substr(0, pos);
    std::string_view ext = filename.substr(pos);
    if (ext != ".sst") {
        return std::nullopt;
    }
    uint64_t raw_id = 0;
    if (!absl::SimpleAtoi(name, &raw_id)) {
        return std::nullopt;
    }
    return file_id{raw_id};
}

} // namespace lsm::internal

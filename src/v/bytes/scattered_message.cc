/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/scattered_message.h"

#include <algorithm>

iobuf buffer_vector_to_iobuf(scattered_buffer bufs) {
    iobuf result;
    for (auto& b : bufs) {
        result.append(std::move(b));
    }
    return result;
}

size_t scattered_size(const scattered_buffer& bufs) {
    return std::ranges::fold_left(
      bufs, size_t{0}, [](size_t acc, const auto& buf) {
          return acc + buf.size();
      });
}

scattered_buffer iobuf_to_buffer_vector(iobuf b) {
    scattered_buffer bufs;
    bufs.reserve(std::distance(b.begin(), b.end()));
    for (auto& frag : b) {
        bufs.push_back(frag.share());
    }
    return bufs;
}

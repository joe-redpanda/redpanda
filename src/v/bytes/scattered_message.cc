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

#include "seastar/util/log.hh"

namespace {
ss::logger _logger{"scattered_message"};
}

ss::scattered_message<char> iobuf_as_scattered(iobuf b) {
    ss::scattered_message<char> msg;
    auto in = iobuf::iterator_consumer(b.cbegin(), b.cend());
    int32_t chunk_no = 0;
    in.consume(
      b.size_bytes(), [&msg, &chunk_no, &b](const char* src, size_t sz) {
          ++chunk_no;
          vassert(
            chunk_no <= std::numeric_limits<int16_t>::max(),
            "Invalid construction of scattered_message. fragment coutn exceeds "
            "max count:{}. Usually a bug with small append() to iobuf. {}",
            chunk_no,
            b);
          msg.append_static(src, sz);
          return ss::stop_iteration::no;
      });
    msg.on_delete([b = std::move(b)] {});
    return msg;
}

ss::scattered_message<char>
print_scattered(const char* prefix, ss::scattered_message<char> msg) {
    static constexpr size_t page_size = 4096U;
    // small angle approximation
    if (msg.size() < page_size) {
        ss::net::packet packet = std::move(msg).release();
        auto fragment_vector = packet.fragments();

        ss::scattered_message<char> replacement{};

        for (auto& fragment_ref : fragment_vector) {
            const auto s = std::string_view{
              fragment_ref.base, fragment_ref.size};
            _logger.info("{} scattered message was: {}", prefix, s);

            _logger.info("{} stats on message, size: {}", prefix, packet.len());

            replacement.append_static(fragment_ref.base, fragment_ref.size);
        }

        replacement.on_delete([packet = std::move(packet)]() {});
        return replacement;
    }
    return msg;
}

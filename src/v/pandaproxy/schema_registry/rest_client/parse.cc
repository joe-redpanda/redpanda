/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "pandaproxy/schema_registry/rest_client/parse.h"

#include "serde/json/parser.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>

#include <cstdint>
#include <limits>

namespace pandaproxy::schema_registry::rest_client {

ss::future<std::expected<chunked_vector<context_subject>, parse_error>>
parse_subjects(iobuf body, qualified_subjects_enabled qualified) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_array) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON array of subjects"});
        }

        chunked_vector<context_subject> subjects;
        while (co_await p.next()) {
            switch (p.token()) {
            case token::end_array:
                // The body is exactly a JSON array of strings: reject any
                // trailing content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after subjects array"});
                }
                co_return std::move(subjects);
            case token::value_string:
                subjects.push_back(
                  context_subject::from_string(
                    p.value_string().linearize_to_string(), qualified));
                break;
            default:
                co_return std::unexpected(
                  parse_error{
                    .reason = "expected a string element in subjects array"});
            }
        }

        // next() returned false before the closing ']' was seen.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse subjects: {}", e.what())});
    }
}

ss::future<std::expected<chunked_vector<schema_version>, parse_error>>
parse_subject_versions(iobuf body) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_array) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON array of versions"});
        }

        chunked_vector<schema_version> versions;
        while (co_await p.next()) {
            switch (p.token()) {
            case token::end_array:
                // The body is exactly a JSON array of integers: reject any
                // trailing content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after versions array"});
                }
                co_return std::move(versions);
            case token::value_int: {
                auto v = p.value_int();
                if (v < 0) {
                    // Only the deletedAsNegative mode produces negatives, which
                    // this client does not request; modeling soft-deleted
                    // versions is future work.
                    co_return std::unexpected(
                      parse_error{
                        .reason = "negative version number; deletedAsNegative "
                                  "mode is not supported"});
                }
                if (v < 1 || v > std::numeric_limits<int32_t>::max()) {
                    co_return std::unexpected(
                      parse_error{.reason = "version number out of range"});
                }
                versions.push_back(schema_version{static_cast<int32_t>(v)});
                break;
            }
            default:
                co_return std::unexpected(
                  parse_error{
                    .reason = "expected an integer element in versions array"});
            }
        }

        // next() returned false before the closing ']' was seen.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse versions: {}", e.what())});
    }
}

} // namespace pandaproxy::schema_registry::rest_client

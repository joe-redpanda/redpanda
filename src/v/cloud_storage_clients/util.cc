/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/util.h"

#include "base/vlog.h"
#include "bytes/streambuf.h"
#include "container/chunked_vector.h"
#include "http/utils.h"
#include "net/connection.h"
#include "strings/string_switch.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>

#include <boost/property_tree/xml_parser.hpp>

#include <exception>
#include <system_error>

namespace {

bool is_abort_or_gate_close_exception(const std::exception_ptr& ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const ss::abort_requested_exception&) {
        return true;
    } catch (const ss::gate_closed_exception&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

namespace cloud_storage_clients::util {

bool has_abort_or_gate_close_exception(const ss::nested_exception& ex) {
    return is_abort_or_gate_close_exception(ex.inner)
           || is_abort_or_gate_close_exception(ex.outer);
}

bool is_nested_reconnect_error(const ss::nested_exception& ex) {
    try {
        std::rethrow_exception(ex.inner);
    } catch (const std::system_error& e) {
        if (!net::is_reconnect_error(e)) {
            return false;
        }
    } catch (...) {
        return false;
    }
    try {
        std::rethrow_exception(ex.outer);
    } catch (const std::system_error& e) {
        if (!net::is_reconnect_error(e)) {
            return false;
        }
    } catch (...) {
        return false;
    }
    return true;
}

template<typename Logger>
error_outcome handle_client_transport_error(
  std::exception_ptr current_exception, Logger& logger) {
    auto outcome = error_outcome::retry;

    try {
        std::rethrow_exception(current_exception);
    } catch (const std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            vlog(logger.warn, "File removed during download: ", e.path1());
            outcome = error_outcome::retry;
        } else {
            vlog(logger.error, "Filesystem error {}", e);
            outcome = error_outcome::fail;
        }
    } catch (const std::system_error& cerr) {
        // The system_error is type erased and not convenient for selective
        // handling. The following errors should be retried:
        // - connection refused, timed out or reset by peer
        // - network temporary unavailable
        // Shouldn't be retried
        // - any filesystem error
        // - broken-pipe
        // - any other network error (no memory, bad socket, etc)
        if (net::is_reconnect_error(cerr)) {
            vlog(
              logger.warn,
              "System error susceptible for retry {}",
              cerr.what());

        } else {
            vlog(logger.error, "System error {}", cerr);
            outcome = error_outcome::fail;
        }
    } catch (const ss::timed_out_error& terr) {
        // This should happen when the connection pool was disconnected
        // from the S3 endpoint and subsequent connection attmpts failed.
        vlog(logger.warn, "Connection timeout {}", terr.what());
    } catch (const boost::system::system_error& err) {
        if (
          err.code() != boost::beast::http::error::end_of_stream
          && err.code() != boost::beast::http::error::partial_message) {
            vlog(logger.warn, "Connection failed {}", err.what());
            outcome = error_outcome::fail;
        } else {
            // This is a short read error that can be caused by the abrupt TLS
            // shutdown. The content of the received buffer is discarded in this
            // case and http client receives an empty buffer.
            vlog(
              logger.info,
              "Server disconnected: '{}', retrying HTTP request",
              err.what());
        }
    } catch (const ss::gate_closed_exception&) {
        vlog(logger.debug, "Gate closed");
        throw;
    } catch (const ss::abort_requested_exception&) {
        vlog(logger.debug, "Abort requested");
        throw;
    } catch (const ss::nested_exception& ex) {
        if (has_abort_or_gate_close_exception(ex)) {
            vlog(logger.debug, "Nested abort or gate closed: {}", ex);
            throw;
        } else if (is_nested_reconnect_error(ex)) {
            vlog(logger.warn, "Connection error {}", std::current_exception());
        } else {
            vlog(logger.error, "Unexpected error {}", std::current_exception());
            outcome = error_outcome::fail;
        }
    } catch (...) {
        vlog(logger.error, "Unexpected error {}", std::current_exception());
        outcome = error_outcome::fail;
    }

    return outcome;
}

template error_outcome
handle_client_transport_error<ss::logger>(std::exception_ptr, ss::logger&);
template error_outcome handle_client_transport_error<retry_chain_logger>(
  std::exception_ptr, retry_chain_logger&);

boost::property_tree::ptree iobuf_to_ptree(iobuf&& buf, ss::logger& logger) {
    namespace pt = boost::property_tree;
    try {
        iobuf_istreambuf strbuf(buf);
        std::istream stream(&strbuf);
        pt::ptree res;
        pt::read_xml(stream, res);
        return res;
    } catch (...) {
        log_buffer_with_rate_limiting("unexpected reply", buf, logger);
        vlog(logger.error, "!!parsing error {}", std::current_exception());
        throw;
    }
}

std::chrono::system_clock::time_point parse_timestamp(std::string_view sv) {
    std::tm tm = {};
    std::stringstream ss({sv.data(), sv.size()});
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.Z%Z");
    return std::chrono::system_clock::from_time_t(timegm(&tm));
}

void log_buffer_with_rate_limiting(
  const char* msg, iobuf& buf, ss::logger& logger) {
    static constexpr int buffer_size = 0x100;
    static constexpr auto rate_limit = std::chrono::seconds(1);
    thread_local static ss::logger::rate_limit rate(rate_limit);
    auto log_with_rate_limit = [&logger](
                                 ss::logger::format_info fmt, auto... args) {
        logger.log(ss::log_level::warn, rate, fmt, args...);
    };
    iobuf_istreambuf strbuf(buf);
    std::istream stream(&strbuf);
    std::array<char, buffer_size> str{};
    auto sz = stream.readsome(str.data(), buffer_size);
    auto sview = std::string_view(str.data(), sz);
    vlog(log_with_rate_limit, "{}: {}", msg, sview);
}

std::vector<object_key> all_paths_to_file(const object_key& path) {
    if (!path().has_filename()) {
        return {};
    }

    std::vector<object_key> paths;
    std::filesystem::path current_path;
    for (auto path_iter = path().begin(); path_iter != path().end();
         ++path_iter) {
        if (current_path == "") {
            current_path += *path_iter;
        } else {
            current_path /= *path_iter;
        }

        paths.emplace_back(current_path);
    }

    return paths;
}

void url_encode_target(http::client::request_header& header) {
    auto query_pos = header.target().find_first_of("?");
    // encode full target as there are no query parameters
    if (query_pos == std::string::npos) {
        header.target(
          std::string(
            http::uri_encode(header.target(), http::uri_encode_slash::no)));
    } else {
        // encode only the path part of the target
        // TODO: add individual query parameters encoding here as well.
        header.target(
          fmt::format(
            "{}{}",
            http::uri_encode(
              std::string_view(header.target().begin(), query_pos),
              http::uri_encode_slash::no),
            header.target().substr(query_pos)));
    }
}

response_content_type
get_response_content_type(const http::client::response_header& headers) {
    static constexpr boost::beast::string_view content_type_name
      = "Content-Type";
    if (auto iter = headers.find(content_type_name); iter != headers.end()) {
        if (iter->value().find("json") != std::string_view::npos) {
            return response_content_type::json;
        }

        if (iter->value().find("xml") != std::string_view::npos) {
            return response_content_type::xml;
        }
    }

    return response_content_type::unknown;
}

namespace {

/// \brief Helper struct for tracking CRLF sequences in raw HTTP response data.
struct crlf_stack {
    crlf_stack() { stk.reserve(4); }
    /// \brief Try to push a character onto the stack
    ///
    /// \return Whether the character is a carriage return or line feed, AND
    ///         whether the character might be part of a CRLF sequence
    std::pair<bool, bool> try_put(char c) {
        if (
          (c == '\r' && stk.size() % 2 == 0)
          || (c == '\n' && stk.size() % 2 == 1)) {
            stk.push_back(c);
        } else {
            stk.clear();
        }
        return std::make_pair(c == '\r' || c == '\n', stk.size() > 0);
    }
    /// \brief Return the number of complete CRLF sequences on the stack
    ///
    /// If the top of the stack is a carriage return, return 0. In general, only
    /// complete CRLFs have any meaning in parsing logic.
    size_t count() const {
        if (stk.size() % 2) {
            return 0;
        }
        return stk.size() / 2;
    }

private:
    chunked_vector<char> stk;
};
} // namespace

mime_header mime_header::from(iobuf_parser& in) {
    mime_header result;
    crlf_stack line_feed;
    chunked_vector<std::pair<std::string, ssize_t>> raw_headers;
    static constexpr size_t max_buf = 1024;
    std::string buf;
    // track whether we reached the name/value separator (':')
    std::optional<ssize_t> sep_pos{};
    // read the buffer, char by char, splitting lines on CRLF and break when we
    // reach CRLF CRLF or run out of bytes
    while (in.bytes_left()) {
        auto c = in.consume_type<char>();
        auto [is_nl, is_crlf_part] = line_feed.try_put(c);
        if (is_nl && !is_crlf_part) {
            // For our limited use case, we expect to see newline chars ONLY as
            // part of a CRLF sequence
            throw std::runtime_error(
              "Failed to parse MIME header: Misplaced newline");
        } else if (buf.size() >= max_buf) {
            throw std::runtime_error(
              fmt::format(
                "Failed to parse MIME header: Exceeded max header size {}",
                max_buf));
        }
        // only the first colon separates name from value, and every character
        // up to that point (i.e. the header name) is case insensitive per
        // RFC 5234
        if (!sep_pos.has_value()) {
            if (c == ':') {
                sep_pos = buf.size();
            }
            c = static_cast<char>(std::tolower(c));
        }
        buf.push_back(c);
        if (line_feed.count() == 1) {
            if (buf.size() <= 2) {
                throw std::runtime_error(
                  "Failed to parse MIME header: Unexpected leading CRLF");
            } else if (!sep_pos.has_value()) {
                throw std::runtime_error(
                  "Failed to parse MIME header: Missing name/value separator");
            } else {
                raw_headers.emplace_back(
                  std::make_pair(std::move(buf), sep_pos.value() + 2));
                buf = {};
                sep_pos.reset();
            }
        } else if (line_feed.count() == 2) {
            break;
        }
    }
    if (line_feed.count() != 2 && !raw_headers.empty()) {
        throw std::runtime_error(
          "Failed to parse MIME header: missing trailing 'CRLF CRLF'");
    }
    for (const auto& [hdr, val_start] : raw_headers) {
        std::optional<field> f{};
        if (hdr.starts_with("content-type: ")) {
            f = field::content_type;
        } else if (hdr.starts_with("content-id: ")) {
            f = field::content_id;
        } else if (hdr.starts_with("content-length: ")) {
            f = field::content_length;
        } else if (hdr.starts_with("content-transfer-encoding: ")) {
            f = field::content_transfer_encoding;
        }
        if (f.has_value()) {
            // assume trailing CRLF is always present and strip it off
            auto n = hdr.size() - val_start - 2;
            std::ignore = result._fields.try_emplace(
              f.value(), hdr.substr(val_start, n));
        }
        // quietly ignore anything we don't match for and/or anything malformed
    }
    return result;
}

std::optional<ss::sstring> mime_header::get(field f) const {
    if (auto it = _fields.find(f); it != _fields.end()) {
        return std::make_optional(it->second);
    }
    return std::nullopt;
}

} // namespace cloud_storage_clients::util

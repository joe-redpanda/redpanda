/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "bytes/scattered_message.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/s3_client.h"
#include "cloud_storage_clients/types.h"
#include "hashing/secure.h"
#include "http/client.h"
#include "http/tests/utils.h"
#include "net/dns.h"
#include "net/types.h"
#include "test_utils/fixture.h"
#include "utils/base64.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/request.hh>
#include <seastar/http/routes.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tcp.hh>
#include <seastar/net/tls.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/algorithm/string.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/property_tree/ptree_fwd.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>

namespace {
ss::logger _log{"s3_serializer_test"};

struct parser_visitor {
    template<class ConstBufferSeq>
    void operator()(boost::beast::error_code&, const ConstBufferSeq& seq) {
        // update outbuf with new data from 'seq'
        for (auto it = boost::asio::buffer_sequence_begin(seq);
             it != boost::asio::buffer_sequence_end(seq);
             it++) {
            // generic implementation for header buffers
            boost::asio::const_buffer buffer = *it;
            outbuf.append(
              static_cast<const uint8_t*>(buffer.data()), buffer.size());
        }
        serializer.consume(boost::asio::buffer_size(seq));
    }

    iobuf& outbuf;
    http::http_serializer& serializer;
};
} // namespace

static http::client::request_header clone_failing_header() {
    /* the log for the request which specifically failed
    TRACE 2025-07-23 19:14:30,804 [shard 1:au  ] http -
    /9bd37ecc-1402-4347-876e-4ee60af22568/kafka/migration-test-workload/4_28/29526-30550-3947509-1-v1.log.1.index
    - client.cc:507 - request_stream header check:
    PUT
    /9bd37ecc-1402-4347-876e-4ee60af22568/kafka/migration-test-workload/4_28/29526-30550-3947509-1-v1.log.1.index
    HTTP/1.1
    User-Agent: redpanda.vectorized.io
    Host: panda-bucket-1c8056a0-67f9-11f0-b82f-ca451009663c.minio-s3
    Content-Type: text/plain
    Content-Length: 1098
    x-amz-date: 20250723T191430Z
    x-amz-content-sha256: UNSIGNED-PAYLOAD
    Authorization: AWS4-HMAC-SHA256
    Credential=panda-user/20250723/panda-region/s3/aws4_request,SignedHeaders=content-length;content-type;host;user-agent;x-amz-content-sha256;x-amz-date,Signature=c4ccbf25add62b49c18da10c3defdb032fe92655eac812a408632c8f2511202c*/
    http::client::request_header header{};

    // put
    header.method(boost::beast::http::verb::put);

    // target
    header.target(
      "/9bd37ecc-1402-4347-876e-4ee60af22568/kafka/migration-test-workload/"
      "4_28/29526-30550-3947509-1-v1.log.1.index");

    // http version
    constexpr unsigned http_version = 11;
    header.version(http_version);

    // user agent
    header.insert(
      boost::beast::http::field::user_agent, "redpanda.vectorized.io");

    // host
    header.insert(
      boost::beast::http::field::host,
      "panda-bucket-1c8056a0-67f9-11f0-b82f-ca451009663c.minio-s3");

    // content type
    header.insert(boost::beast::http::field::content_type, "text/plain");

    // content length
    header.insert(
      boost::beast::http::field::content_length, std::to_string(1098));

    // x-amz-date
    header.set("x-amz-date", "20250723T191430Z");

    // x-amz-content-sha256
    header.set("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

    // authorization
    header.insert(
      boost::beast::http::field::authorization,
      "AWS4-HMAC-SHA256 "
      "Credential=panda-user/20250723/panda-region/s3/"
      "aws4_request,SignedHeaders=content-length;content-type;host;user-agent;"
      "x-amz-content-sha256;x-amz-date,Signature="
      "c4ccbf25add62b49c18da10c3defdb032fe92655eac812a408632c8f2511202c");

    return header;
}

SEASTAR_TEST_CASE(test_serializer) {
    auto failing_header = clone_failing_header();
    _log.info("header: {}", failing_header);

    /**
'http::client::request_header' (aka '\
header<true, boost::beast::http::basic_fields<std::allocator<char>>>') to
const boost::beast::http::message<true,
boost::beast::http::basic_string_body<char>>') for 1st argument
    */

    auto message = boost::beast::http::
      message<true, boost::beast::http::basic_string_body<char>>{
        std::move(failing_header)};

    _log.info("message: {}", message);

    http::http_serializer serializer{message};

    boost::beast::error_code error_code{};
    iobuf outbuf{};
    parser_visitor visitor{.outbuf = outbuf, .serializer = serializer};
    serializer.next(error_code, visitor);
    if (error_code) {
        vlog(_log.error, "serialization error {}", error_code);
        throw boost::system::system_error{error_code};
    }

    vassert(
      serializer.is_header_done(),
      "header serialization must complete before sending message");

    iobuf_parser iobuf_parser{outbuf.copy()};

    _log.info(
      "output from the serizalizer was: {}",
      iobuf_parser.peek_string_unsafe(outbuf.size_bytes()));

    _log.info("outbuf size was: {}", outbuf.size_bytes());

    auto scattered = iobuf_as_scattered(std::move(outbuf));

    _log.info("scattered size was: {}", scattered.size());

    auto scattered_replacement = print_scattered(
      "in_my_test ", std::move(scattered));

    std::ignore = print_scattered(
      "also in my test ", std::move(scattered_replacement));

    BOOST_FAIL("womp womp");
    co_return;
}

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

#include "serde/json/parser.h"
#include "serde/json/tests/data.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace experimental::serde::json;

constexpr std::string_view json_checker_pass1 = R"([
    "JSON Test Pattern pass1",
    {"object with 1 member":["array with 1 element"]},
    {},
    [],
    -42,
    true,
    false,
    null,
    {
        "integer": 1234567890,
        "real": -9876.543210,
        "e": 0.123456789e-12,
        "E": 1.234567890E+34,
        "":  23456789012E66,
        "zero": 0,
        "one": 1,
        "space": " ",
        "quote": "\"",
        "backslash": "\\",
        "controls": "\b\f\n\r\t",
        "slash": "/ & \/",
        "alpha": "abcdefghijklmnopqrstuvwyz",
        "ALPHA": "ABCDEFGHIJKLMNOPQRSTUVWYZ",
        "digit": "0123456789",
        "0123456789": "digit",
        "special": "`1~!@#$%^&*()_+-={':[,]}|;.</>?",
        "hex": "\u0123\u4567\u89AB\uCDEF\uabcd\uef4A",
        "true": true,
        "false": false,
        "null": null,
        "array":[  ],
        "object":{  },
        "address": "50 St. James Street",
        "url": "http://www.JSON.org/",
        "comment": "// /* <!-- --",
        "# -- --> */": " ",
        " s p a c e d " :[1,2 , 3

,

4 , 5        ,          6           ,7        ],"compact":[1,2,3,4,5,6,7],
        "jsontext": "{\"object with 1 member\":[\"array with 1 element\"]}",
        "quotes": "&#34; \u0022 %22 0x22 034 &#x22;",
        "\/\\\"\uCAFE\uBABE\uAB98\uFCDE\ubcda\uef4A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?"
: "A key can be any string"
    },
    0.5 ,98.6
,
99.44
,

1066,
1e1,
0.1e1,
1e-1,
1e00,2e+00,2e-00
,"rosebud"])";

// Simple test to ensure the parse doesn't fail on valid sample data. The
// contents and correctness is not verified in this test.
TEST_CORO(json_test_suite, parse) {
    auto parser = experimental::serde::json::parser(
      co_await json_test_suite_sample());

    while (co_await parser.next()) {
        // Do nothing, just drain the parser.
        // The contents and correctness is not verified in this test.
    }

    EXPECT_EQ(parser.token(), token::eof) << "Expected to reach EOF but got: "
                                          << std::to_underlying(parser.token());
}

struct token_seq_test_case {
    std::string_view input;
    std::vector<token> expected_tokens;
};

ss::future<> run_test_case(const token_seq_test_case& tc) {
    auto parser = experimental::serde::json::parser(iobuf::from(tc.input));
    for (const auto& expected : tc.expected_tokens) {
        ASSERT_TRUE_CORO(co_await parser.next())
          << "Expected next() to return true for input: " << tc.input;
        ASSERT_EQ_CORO(parser.token(), expected)
          << "Unexpected token for input: " << tc.input;
    }
    ASSERT_FALSE_CORO(co_await parser.next())
      << "Expected next() to return false after all tokens for input: "
      << tc.input;
}

TEST_CORO(json_parser, parse_empty) {
    constexpr auto empty_documents = std::to_array<std::string_view>(
      {"", " ", "\n", "\t", "\r\n"});

    for (const auto& doc : empty_documents) {
        SCOPED_TRACE(fmt::format("Testing empty document: {}", doc));
        ASSERT_NO_FATAL_FAILURE_CORO(co_await run_test_case({
          .input = doc,
          .expected_tokens = {token::error},
        }));
    }
};

TEST_CORO(json_parser, leading_trailing_whitespace) {
    ASSERT_NO_FATAL_FAILURE_CORO(co_await run_test_case({
        .input = "   [   ]   ",
        .expected_tokens = {
            token::start_array,
            token::end_array,
            token::eof,
        },
    }));
}

TEST_CORO(json_parser, truncated_json_always_errors) {
    for (size_t i = 0; i <= json_checker_pass1.size(); i++) {
        SCOPED_TRACE(fmt::format(
          "Testing truncated JSON at position: {} out of {}",
          i,
          json_checker_pass1.size()));

        auto p = experimental::serde::json::parser(
          iobuf::from(json_checker_pass1.substr(0, i)));

        while (co_await p.next()) {
            // Consume tokens.
        }

        bool should_error = i < json_checker_pass1.size();

        ASSERT_EQ_CORO(p.token(), should_error ? token::error : token::eof);
    }
}

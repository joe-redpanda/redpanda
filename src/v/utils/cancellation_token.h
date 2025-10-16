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

#pragma once

#include "base/seastarx.h"

#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

// quick and dirty cancellation token source
// similar to abort source, but
//     1. yields tokens which can poll cancellation
//     2. token source can be reset
//     3. tokens are safe to use after source destruction
struct cancellation_token_source {
    // constants
    static constexpr auto noop_cb = [] {};

    // types
    struct cancellation_token_inner
      : ss::weakly_referencable<cancellation_token_inner> {
    private:
        using self_t = cancellation_token_inner;

    public:
        // stable pointer only
        cancellation_token_inner() = default;
        cancellation_token_inner(const self_t&) = delete;
        self_t& operator=(const self_t&) = delete;
        cancellation_token_inner(self_t&&) = delete;
        self_t& operator=(self_t&&) = delete;

        ~cancellation_token_inner() { callback(); }

        ss::noncopyable_function<void(void)> callback{noop_cb};
    };

    struct cancellation_token {
        cancellation_token() = default;
        explicit cancellation_token(
          ss::weak_ptr<cancellation_token_inner> inner) noexcept
          : _inner{std::move(inner)} {}
        inline bool is_cancelled() const { return !static_cast<bool>(_inner); }

    private:
        ss::weak_ptr<cancellation_token_inner> _inner;
    };

    struct callback_holder {
    private:
        using self_t = callback_holder;

    public:
        explicit callback_holder(
          ss::weak_ptr<cancellation_token_inner> inner) noexcept
          : _inner{std::move(inner)} {}

        // move only
        callback_holder(const self_t&) = delete;
        self_t& operator=(const self_t&) = delete;
        callback_holder(self_t&&) = default;
        self_t& operator=(self_t&&) = default;

        ~callback_holder() noexcept {
            if (_inner) {
                _inner->callback = {noop_cb};
            }
        }

    private:
        ss::weak_ptr<cancellation_token_inner> _inner;
    };

    // functions
    cancellation_token_source() noexcept
      : _inner{std::make_unique<cancellation_token_inner>()} {}

    // get a cancellation token
    cancellation_token get_cancellation_token() const noexcept {
        if (_inner) {
            return cancellation_token{_inner->weak_from_this()};
        }
        return {};
    }

    // shortcut to check cancellation
    bool is_cancelled() const noexcept { return !static_cast<bool>(_inner); }

    // cancel all tokens sourced from this
    void cancel() noexcept { _inner.reset(); }

    // cancel all tokens sourced from this & re-initialize the source
    void reset() noexcept {
        cancel();
        _inner = std::make_unique<cancellation_token_inner>();
    }

    // register a fucntion to be invoked on cancellation. registering a callback
    // on a cancelled source immediately invokes the callback
    callback_holder register_callback(ss::noncopyable_function<void(void)> cb) {
        if (_inner) {
            _inner->callback = std::move(cb);
            return callback_holder{_inner->weak_from_this()};
        }
        cb();
        return callback_holder{nullptr};
    }

private:
    std::unique_ptr<cancellation_token_inner> _inner;
};

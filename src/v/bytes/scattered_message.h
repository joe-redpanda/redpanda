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
#pragma once
#include "bytes/iobuf.h"

#include <seastar/core/temporary_buffer.hh>

#include <vector>

/// \brief a vector of temporary buffers, typically used to represent an
/// iobuf's fragment structure for zero-copy output operations.
using scattered_buffer = std::vector<ss::temporary_buffer<char>>;

/// \brief converts an iobuf into a vector of shared temporary_buffer<char>,
/// No byte copying occurs, temporary buffers reflect the iobuf's fragment
/// structure and share their underlying buffers (using the usual temporary
/// buffer reference counting).
scattered_buffer iobuf_to_buffer_vector(iobuf b);

/// \brief reassembles a vector of temporary buffers back into an iobuf
/// The iobuf fragment structure may be the same as the input: a heuristic
/// is used to decide whether to copy or share each buffer.
iobuf buffer_vector_to_iobuf(scattered_buffer bufs);

/// \brief returns the total size in bytes of a vector of temporary buffers
size_t scattered_size(const scattered_buffer& bufs);

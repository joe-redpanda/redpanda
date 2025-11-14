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

#include "lsm/io/disk_persistence.h"

#include "base/units.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/io/persistence.h"
#include "utils/file_io.h"
#include "utils/uuid.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <system_error>

namespace lsm::io {

namespace {

class disk_file_reader : public random_access_file_reader {
public:
    disk_file_reader(std::filesystem::path path, ss::file file)
      : _path(std::move(path))
      , _file(std::move(file)) {}

    ss::future<ioarray> read(size_t offset, size_t n) override {
        size_t memory_alignment = _file.memory_dma_alignment();
        size_t disk_alignment = _file.disk_read_dma_alignment();
        size_t adjusted_offset = ss::align_down(offset, disk_alignment);
        size_t offset_delta = offset - adjusted_offset;
        auto array = ioarray::aligned(
          memory_alignment, ss::align_up(n + offset_delta, disk_alignment));
        try {
            size_t amt = co_await _file.dma_read(
              adjusted_offset, array.as_iovec());
            if (amt < offset_delta + n) {
                throw io_error_exception(
                  "short read: failed to read {} bytes from block at offset "
                  "{}, "
                  "got: "
                  "{}",
                  array.size(),
                  adjusted_offset,
                  amt);
            }
            co_return array.share(offset_delta, n);
        } catch (const std::system_error& err) {
            throw io_error_exception(err.code(), "io error reading: {}", err);
        } catch (...) {
            throw io_error_exception(
              "io error reading: {}", std::current_exception());
        }
    }

    ss::future<> close() override {
        try {
            co_await _file.close();
        } catch (const std::system_error& err) {
            throw io_error_exception(err.code(), "io error closing: {}", err);
        } catch (...) {
            throw io_error_exception(
              "io error closing: {}", std::current_exception());
        }
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(it, "{{path={}}}", _path);
    }

private:
    std::filesystem::path _path;
    ss::file _file;
};

class disk_seq_file_writer : public sequential_file_writer {
public:
    disk_seq_file_writer(
      std::filesystem::path path, ss::output_stream<char> stream)
      : _path(std::move(path))
      , _stream(std::move(stream)) {}

    ss::future<> append(iobuf buf) override {
        try {
            for (auto& frag : buf) {
                co_await _stream.write(frag.get(), frag.size());
            }
        } catch (const std::system_error& err) {
            throw io_error_exception(err.code(), "io error writing: {}", err);
        } catch (...) {
            throw io_error_exception(
              "io error writing: {}", std::current_exception());
        }
    }
    ss::future<> close() override {
        try {
            co_await _stream.close();
        } catch (const std::system_error& err) {
            throw io_error_exception(err.code(), "io error closing: {}", err);
        } catch (...) {
            throw io_error_exception(
              "io error closing: {}", std::current_exception());
        }
    }
    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(it, "{{path={}}}", _path);
    }

private:
    std::filesystem::path _path;
    ss::output_stream<char> _stream;
};

class impl
  : public data_persistence
  , public metadata_persistence {
public:
    explicit impl(std::filesystem::path root)
      : _root(std::move(root)) {}
    impl(const impl&) = delete;
    impl(impl&&) = delete;
    impl& operator=(const impl&) = delete;
    impl& operator=(impl&&) = delete;
    ~impl() override = default;

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(internal::file_id id) override {
        try {
            auto filepath = path(internal::sst_file_name(id));
            auto file = co_await ss::open_file_dma(
              filepath.native(), ss::open_flags::ro);
            std::unique_ptr<random_access_file_reader> ptr;
            ptr = std::make_unique<disk_file_reader>(
              std::move(filepath), std::move(file));
            co_return ptr;
        } catch (const std::system_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                co_return std::nullopt;
            }
            throw io_error_exception(
              e.code(), "io error opening file reader: {}", e);
        } catch (...) {
            throw io_error_exception(
              "io error opening file reader: {}", std::current_exception());
        }
    }

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(internal::file_id id) override {
        try {
            auto filepath = path(internal::sst_file_name(id));
            auto file = ss::open_file_dma(
              filepath.native(),
              ss::open_flags::create | ss::open_flags::rw
                | ss::open_flags::truncate);
            auto stream = co_await ss::with_file_close_on_failure(
              std::move(file), [](ss::file& f) {
                  return ss::make_file_output_stream(
                    std::move(f), ss::file_output_stream_options{});
              });
            co_return std::make_unique<disk_seq_file_writer>(
              std::move(filepath), std::move(stream));
        } catch (const std::system_error& e) {
            throw io_error_exception(
              e.code(), "io error opening file writer: {}", e);
        } catch (...) {
            throw io_error_exception(
              "io error opening file writer: {}", std::current_exception());
        }
    }

    ss::future<std::optional<iobuf>> read_manifest() override {
        try {
            co_return co_await read_fully(path("MANIFEST").native());
        } catch (const std::system_error& e) {
            if (e.code() != std::errc::no_such_file_or_directory) {
                throw io_error_exception(
                  e.code(), "io error reading manifest: {}", e);
            }
            co_return std::nullopt;
        } catch (...) {
            throw io_error_exception(
              "io error reading manifest: {}", std::current_exception());
        }
    }

    ss::future<> write_manifest(iobuf b) override {
        auto staging_name = fmt::format(
          "MANIFEST.{}.lsm-staging", uuid_t::create());
        try {
            co_await write_fully(path(staging_name), std::move(b));
            co_await ss::rename_file(
              path(staging_name).native(), path("MANIFEST").native());
            co_await ss::sync_directory(_root.native());
        } catch (const std::system_error& e) {
            throw io_error_exception(
              e.code(),
              "io error writing manifest: {}",
              std::current_exception());
        } catch (...) {
            throw io_error_exception(
              "io error writing manifest: {}", std::current_exception());
        }
    }

    ss::future<> remove_file(internal::file_id id) override {
        try {
            co_await ss::remove_file(
              path(internal::sst_file_name(id)).native());
        } catch (const std::system_error& e) {
            if (e.code() != std::errc::no_such_file_or_directory) {
                throw io_error_exception(
                  e.code(), "io error removing file: {}", e);
            }
        } catch (...) {
            throw io_error_exception(
              "io error removing file: {}", std::current_exception());
        }
    }

    ss::coroutine::experimental::generator<internal::file_id>
    list_files() override {
        ss::file dir;
        std::exception_ptr ep;
        try {
            dir = co_await ss::open_directory(_root.native());
            auto generator = dir.experimental_list_directory();
            while (auto entry = co_await generator()) {
                auto maybe_id = internal::parse_sst_file_name(entry->name);
                if (maybe_id) {
                    co_yield *maybe_id;
                }
            }
        } catch (const std::system_error& e) {
            ep = std::make_exception_ptr(
              io_error_exception(e.code(), "io error listing files: {}", e));
        } catch (...) {
            ep = std::make_exception_ptr(io_error_exception(
              "io error listing files: {}", std::current_exception()));
        }
        if (dir) {
            co_await dir.close();
        }
        if (ep) {
            std::rethrow_exception(ep);
        }
    }

    ss::future<> close() override { co_return; }

private:
    std::filesystem::path path(std::string_view name) { return _root / name; }

    std::filesystem::path _root;
};

ss::future<> cleanup_staging_files(ss::file& dir) {
    auto generator = dir.experimental_list_directory();
    while (auto entry = co_await generator()) {
        if (entry->name.ends_with(".lsm-staging")) {
            co_await ss::remove_file(entry->name);
        }
    }
}

} // namespace

ss::future<std::unique_ptr<data_persistence>>
open_disk_data_persistence(std::filesystem::path directory) {
    try {
        co_await ss::recursive_touch_directory(directory.native());
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "io error touching db dir: {}", e);
    } catch (...) {
        throw io_error_exception(
          "io error touching db dir: {}", std::current_exception());
    }
    co_return std::make_unique<impl>(std::move(directory));
}

ss::future<std::unique_ptr<metadata_persistence>>
open_disk_metadata_persistence(std::filesystem::path directory) {
    try {
        co_await ss::recursive_touch_directory(directory.native());
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "io error touching db dir: {}", e);
    } catch (...) {
        throw io_error_exception(
          "io error touching db dir: {}", std::current_exception());
    }
    try {
        auto dir = co_await ss::open_directory(directory.native());
        co_await cleanup_staging_files(dir).finally(
          [&dir] { return dir.close(); });
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "io error listing files: {}", e);
    } catch (...) {
        throw io_error_exception(
          "io error listing files: {}", std::current_exception());
    }
    co_return std::make_unique<impl>(std::move(directory));
}

} // namespace lsm::io

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

#include "raft/stm_checksum_component.h"

#include "base/vassert.h"
#include "raft/consensus.h"
#include "raft/state_machine_manager.h"
#include "raft/types.h"

namespace raft {
stm_checksum_component::stm_checksum_component(
  ctx_log& log, raft::consensus* raft)
  : _log{&log}
  , _raft{raft}
  , _checksum_controller{raft, _log} {}

stm_checksum_component::apply_token
stm_checksum_component::start_apply(const model::record_batch& record_batch) {
    // update state
    _checksum_controller.on_batch(record_batch);

    if (record_batch.header().type != model::record_batch_type::stm_manager) {
        return {};
    }

    vassert(
      record_batch.header().record_count == 1,
      "checksum record batches should only have one record");

    apply_token out_token{
      .record_offset = record_batch.base_offset(),
      .maybe_checksums_at_offset = std::nullopt,
      .maybe_record_type = std::nullopt};

    record_batch.for_each_record([this, &out_token](model::record record) {
        auto type = serde::from_iobuf<record_type>(record.release_key());
        out_token.maybe_record_type = type;
        switch (type) {
        case record_type::prepare: {
            return;
        }
        case record_type::execute: {
            checksums_at_offset remote_checksums_from_offset{};
            try {
                remote_checksums_from_offset
                  = serde::from_iobuf<checksums_at_offset>(
                    record.release_value());
            } catch (const serde::serde_exception& e) {
                vlog(_log->error, "caught serde exception {}", e);
                _checksum_controller.reset();
                return;
            }
            out_token.maybe_checksums_at_offset = std::move(
              remote_checksums_from_offset.prepared_checksums);
            return;
        }
        }
    });

    return out_token;
}

ss::future<> stm_checksum_component::finish_apply(apply_token&& token) {
    apply_token from_start{std::move(token)};

    if (_checksum_controller.should_replicate_prepare()) {
        vassert(
          !token.maybe_checksums_at_offset.has_value()
            && !token.maybe_record_type.has_value(),
          "state violation, stm_checksum component should never simultaneously "
          "apply a checksum record and intend to prepare");
        co_await replicate_prepare();
        co_return;
    }

    // received batch was not relevant
    if (!from_start.maybe_record_type.has_value()) {
        co_return;
    }

    vassert(
      from_start.maybe_checksums_at_offset.has_value(),
      "checksums must either have been received on batch or provided by stm_m");

    switch (from_start.maybe_record_type.value()) {
    case record_type::prepare:
        co_await do_prepare(
          from_start.record_offset,
          std::move(from_start.maybe_checksums_at_offset.value()));
        co_return;
    case record_type::execute:
        vassert(
          from_start.maybe_checksums_at_offset.has_value(),
          "execute record found but no checksums found");
        checksums_at_offset for_checksum_replication{
          .prepared_at_offset = from_start.record_offset,
          .prepared_checksums = std::move(
            from_start.maybe_checksums_at_offset.value())};
        do_execute(std::move(for_checksum_replication));
        co_return;
    }
    co_return;
}

std::ostream&
operator<<(std::ostream& o, stm_checksum_component::record_type t) {
    switch (t) {
    case stm_checksum_component::record_type::prepare:
        return o << "prepare";
    case stm_checksum_component::record_type::execute:
        return o << "execute";
    }
    __builtin_unreachable();
}

ss::future<> stm_checksum_component::do_prepare(
  model::offset prepare_offset, state_machine_checksums checksums) {
    // might happen but should be infrequent
    if (this->maybe_prepared_checksums.has_value()) {
        vlog(
          _log->info,
          "replacing prepared checksums at offset {} with new prepare at "
          "offset {}",
          maybe_prepared_checksums->prepared_at_offset,
          prepare_offset);
    }

    checksums_at_offset prepared_checksums = {
      .prepared_at_offset = prepare_offset,
      .prepared_checksums = std::move(checksums)};

    // keep a local copy
    this->maybe_prepared_checksums = prepared_checksums;

    if (_checksum_controller.should_replicate_checksum()) {
        return replicate_checksum(std::move(prepared_checksums));
    }

    return ss::now();
}

void stm_checksum_component::do_execute(
  checksums_at_offset remote_checksums_from_offset) {
    _checksum_controller.on_execute_checksum();

    // possible on recovery, prepare may have been truncated
    if (!this->maybe_prepared_checksums.has_value()) {
        vlog(
          _log->info,
          "no local checksum found for offset {}",
          remote_checksums_from_offset.prepared_at_offset);
        return;
    }

    const auto& local_prepared_checksums = maybe_prepared_checksums.value();

    // possible on
    if (
      local_prepared_checksums.prepared_at_offset
      > remote_checksums_from_offset.prepared_at_offset) {
        vlog(
          _log->info,
          "local checksum newer than replicated checksum. local prepared "
          "at offset {}, remote prepared at offset {}",
          local_prepared_checksums.prepared_at_offset,
          remote_checksums_from_offset.prepared_at_offset);
        return;
    }

    if (
      local_prepared_checksums.prepared_at_offset
      < remote_checksums_from_offset.prepared_at_offset) {
        vlog(
          _log->error,
          "local checksum older than replicated checksum. local prepared "
          "at offset {}, remote prepared at offset {}",
          local_prepared_checksums.prepared_at_offset,
          remote_checksums_from_offset.prepared_at_offset);
    }

    // at this point they are equal
    const auto checksum_offset = local_prepared_checksums.prepared_at_offset;

    auto maybe_checksum_error = validate_checksums(
      remote_checksums_from_offset.prepared_checksums);

    // error case is already logged in member function
    if (!maybe_checksum_error.has_value()) {
        vlog(
          _log->info,
          "checksums at offset {} evaluated successfully",
          checksum_offset);
    }

    //  clear out old checksums s.t. overwrites can be properly logged
    maybe_prepared_checksums = std::nullopt;
}

model::record_batch stm_checksum_component::make_prepare_checksum_batch() {
    storage::record_batch_builder builder{
      model::record_batch_type::stm_manager, model::offset(0)};

    builder.add_raw_kv(serde::to_iobuf(record_type::prepare), std::nullopt);

    return std::move(builder).build();
}

model::record_batch stm_checksum_component::make_execute_checksum_batch(
  checksums_at_offset checksums) {
    storage::record_batch_builder builder{
      model::record_batch_type::stm_manager, model::offset(0)};

    builder.add_raw_kv(
      serde::to_iobuf(record_type::execute),
      serde::to_iobuf(std::move(std::move(checksums))));

    return std::move(builder).build();
}

ss::future<> stm_checksum_component::replicate_prepare() {
    auto replicate_result = co_await _raft->replicate(
      make_prepare_checksum_batch(),
      replicate_options{
        raft::consistency_level::quorum_ack, checksum_replication_timeout});

    if (replicate_result.has_error()) {
        vlog(
          _log->warn,
          "error replicating update partition properties command: {} - {}",
          record_type::prepare,
          replicate_result.error().message());

        // something went wrong in replication, reset all state
        // if this was transient (timeout), the stm checksum component
        // will eventually signal a new
        // prepare. if this non-transient, this will prevent reattempting a
        // futile replication
        _checksum_controller.reset();
        co_return;
    }

    // controller will block prepares until term change or until the
    // replicated prepare is applies
    _checksum_controller.on_replicate_prepare();
    co_return;
}

ss::future<>
stm_checksum_component::replicate_checksum(checksums_at_offset checksums) {
    auto replicate_result = co_await _raft->replicate(
      make_execute_checksum_batch(std::move(checksums)),
      replicate_options{
        raft::consistency_level::quorum_ack, checksum_replication_timeout});
    if (replicate_result.has_error()) {
        vlog(
          _log->warn,
          "error replicating stm execute checksums record: {} - {}",
          record_type::execute,
          replicate_result.error().message());

        // something went wrong in replication, reset all state
        // if this was transient (timeout), the stm checkpoint component
        // will eventually signal a new prepare. if this non-transient, this
        // will prevent reattempting a futile replication
        _checksum_controller.reset();
        co_return;
    }

    // controller will block prepares until term change or until the
    // replicated checksum is executed
    _checksum_controller.on_replicate_checksum();
    co_return;
}

std::optional<stm_checksum_component::checksum_validation_error>
stm_checksum_component::validate_checksums(
  const state_machine_checksums& remote) {
    vassert(
      this->maybe_prepared_checksums.has_value(), "invalid optional access");
    const auto& local = maybe_prepared_checksums.value();
    for (const auto& [local_id, local_checksum_entry] :
         local.prepared_checksums) {
        auto it = remote.find(local_id);
        if (it == remote.end()) {
            vlog(_log->info, "remote is missing stm with id {}", local_id);
            continue;
        }
        const auto& remote_checksum_entry = it->second;
        /**
         * Do not try to validate state checksums if the last applied offset
         * does not match. It is possible that the state machine has not been
         * updated yet.
         */
        if (
          remote_checksum_entry.last_applied_offset
          != local_checksum_entry.last_applied_offset) {
            vlog(
              _log->trace,
              "last applied offsets mismatch for {}, expected: {}, actual: {}",
              local_id,
              remote_checksum_entry.last_applied_offset,
              local_checksum_entry.last_applied_offset);
            continue;
        }
        if (remote_checksum_entry.checksum != local_checksum_entry.checksum) {
            vlog(
              _log->error,
              "state checksum mismatch for {}, expected: {}, actual: {}",
              local_id,
              remote_checksum_entry.checksum,
              local_checksum_entry.checksum);
            return checksum_validation_error{
              .name = "",
              .last_applied_offset = local_checksum_entry.last_applied_offset};
        }
    }
    return std::nullopt;
}
} // namespace raft

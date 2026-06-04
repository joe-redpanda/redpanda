/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/vassert.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <queue>
#include <set>
#include <utility>

namespace cloud_topics::l1 {

/// \brief An addressable max-priority queue: holds at most one entry per key,
/// each carrying a `Value`, and supports updating or removing a key's entry
/// *in place*. Entries are ordered by `Compare` applied to their values, with
/// ties broken by key for a strict total order; `top()` is the key of the
/// greatest entry.
///
/// Operations are O(log N) where N is the number of distinct keys; `_pos`
/// addresses a key's slot in the ordered set without a key-comparison search.
template<typename Key, typename Value, typename Compare>
class keyed_priority_queue {
private:
    struct entry {
        Key key;
        Value value;
    };

    struct entry_compare {
        // `cmp` is a strict weak ordering over `Value`; extend it to a
        // strict total order over `entry`. Entries with equivalent values have
        // ties broken by comparing keys (which is required to ensure that we
        // don't silently drop entries with differing keys just because their
        // values are identical).
        bool operator()(const entry& a, const entry& b) const {
            if (cmp(a.value, b.value)) {
                return true;
            }
            if (cmp(b.value, a.value)) {
                return false;
            }
            return a.key < b.key;
        }
        Compare cmp;
    };
    using set_t = std::set<entry, entry_compare>;

public:
    explicit keyed_priority_queue(Compare cmp)
      : _ordered_entries(entry_compare{std::move(cmp)}) {}

    bool empty() const { return _ordered_entries.empty(); }
    size_t size() const { return _ordered_entries.size(); }
    bool contains(const Key& k) const { return _entry_it_by_key.contains(k); }

    /// Insert `key` with `value`, or update it in place if already present.
    void upsert(const Key& key, Value value) {
        auto [pos, inserted] = _entry_it_by_key.try_emplace(key);
        if (!inserted) {
            // `key` was already present: `pos->second` points at its current
            // (now stale) slot. std::set can't re-key an element in place, so
            // drop the old slot before reinserting at the new value.
            _ordered_entries.erase(pos->second);
        }
        pos->second
          = _ordered_entries.insert(entry{key, std::move(value)}).first;
    }

    /// Remove `key` from `_ordered_entries` and `_key_to_slot`.
    void erase(const Key& key) {
        auto it = _entry_it_by_key.find(key);
        vassert(
          it != _entry_it_by_key.end(),
          "keyed_priority_queue::erase of an absent key");
        _ordered_entries.erase(it->second);
        _entry_it_by_key.erase(it);
    }

    /// The greatest-priority (key, value) pair. Precondition: non-empty.
    std::pair<const Key&, const Value&> top() const {
        vassert(
          !_ordered_entries.empty(),
          "keyed_priority_queue::top on an empty queue");
        const auto& e = *_ordered_entries.rbegin();
        return {e.key, e.value};
    }

private:
    set_t _ordered_entries;
    // Maps a key to its slot in `_ordered_entries`. std::set iterators are
    // stable across unrelated inserts/erases, so caching them here is safe.
    chunked_hash_map<Key, typename set_t::iterator> _entry_it_by_key;
};

/// \brief A two-level scheduling queue for leveling jobs that performs a k-way
/// merge across partitions.
///
/// Each partition (CTP) owns an inner priority queue of its `leveling_job`s,
/// ordered best-first by a caller-supplied `leveling_cmp_t`. An addressable
/// outer queue (`keyed_priority_queue`) tracks each partition's *current best*
/// job, so `pop()` returns the globally best job.
class leveling_queue {
public:
    explicit leveling_queue(leveling_cmp_t cmp)
      : _cmp(cmp)
      , _heads(std::move(cmp)) {}

    /// Enqueue `job`. The owning partition is taken from the job's metadata.
    void push(leveling_job_ptr job) {
        const auto& tidp = job->meta->tidp;
        // Inner queues share the comparator; a default-constructed
        // leveling_cmp_t would be an empty std::function, so construct each
        // queue with `_cmp`.
        auto& queue = _queues.try_emplace(tidp, _cmp).first->second;
        // Only the partition's best job is exposed to the outer queue, so we
        // only need to (re)publish its head when this job becomes the best.
        const bool new_best = queue.empty() || _cmp(queue.top(), job);
        queue.push(std::move(job));
        if (new_best) {
            _heads.upsert(tidp, queue.top());
        }
        ++_size;
    }

    /// The globally best job. Precondition: non-empty.
    const leveling_job_ptr& top() const {
        vassert(!empty(), "leveling_queue::top on an empty queue");
        return _heads.top().second;
    }

    /// Remove the globally best job. No-op if empty.
    void pop() {
        if (_heads.empty()) {
            return;
        }
        const auto tidp = _heads.top().first;
        auto queue_it = _queues.find(tidp);
        vassert(
          queue_it != _queues.end() && !queue_it->second.empty(),
          "leveling_queue head {} has no backing job",
          tidp);
        auto& queue = queue_it->second;
        queue.pop();
        if (queue.empty()) {
            _queues.erase(queue_it);
            _heads.erase(tidp);
        } else {
            // Advance this queue: publish its new best to the outer queue.
            _heads.upsert(tidp, queue.top());
        }
        --_size;
    }

    /// Drop all queued jobs for `tidp` (e.g. before rebuilding the partition
    /// from a fresh metastore sample). No-op if the partition has none queued.
    void clear(const model::topic_id_partition& tidp) {
        auto queue_it = _queues.find(tidp);
        if (queue_it == _queues.end()) {
            return;
        }
        _size -= queue_it->second.size();
        _queues.erase(queue_it);
        _heads.erase(tidp);
    }

    /// Total number of queued jobs across all partitions.
    size_t size() const { return _size; }
    bool empty() const { return _size == 0; }
    /// Number of partitions with at least one queued job.
    size_t partition_count() const { return _heads.size(); }

private:
    using inner_pq = std::priority_queue<
      leveling_job_ptr,
      chunked_vector<leveling_job_ptr>,
      leveling_cmp_t>;

    leveling_cmp_t _cmp;
    chunked_hash_map<model::topic_id_partition, inner_pq> _queues;
    keyed_priority_queue<
      model::topic_id_partition,
      leveling_job_ptr,
      leveling_cmp_t>
      _heads;
    size_t _size{0};
};

} // namespace cloud_topics::l1

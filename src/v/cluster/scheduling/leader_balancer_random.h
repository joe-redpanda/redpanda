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

#include "base/vassert.h"
#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_strategy.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "random/generators.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <variant>

namespace cluster::leader_balancer_types {

/*
 * Given a `shard_index` this class will generate every possible reassignment.
 */
class random_reassignments {
public:
    explicit random_reassignments(const index_type& si)
      : _replicas() {
        for (const auto& [bs, leaders] : si) {
            for (const auto& [group, replicas] : leaders) {
                _current_leaders[group] = bs;

                for (const auto& replica : replicas) {
                    _replicas.push_back({group, replica});
                }
            }
        }
    }

    /*
     * This function randomly selects a reassignment from the set of all
     * possible reassignments for a given shard_index. It will only return
     * a given reassignment once, and when all possible reassignments have
     * been returned it'll return a std::nullopt.
     */
    std::optional<reassignment> generate_reassignment() {
        while (_replicas_begin < _replicas.size()) {
            // If all reassignments are consumed doing the random shuffling
            // incrementally is slower than using std::shuffle in the
            // constructor. However, its assumed that only a small minority
            // of the reassignments will be consumed. Hence the incremental
            // approach is taken.

            auto ri = random_generators::get_int<size_t>(
              _replicas_begin, _replicas.size() - 1);

            std::swap(_replicas[_replicas_begin], _replicas[ri]);

            const auto& replica = _replicas[_replicas_begin];
            _replicas_begin += 1;

            auto replica_leader_it = _current_leaders.find(replica.group_id);
            vassert(
              replica_leader_it != _current_leaders.end(),
              "replica_leader_it == _current_leaders.end()");
            const auto& replica_leader = replica_leader_it->second;
            if (replica_leader == replica.broker_shard) {
                continue;
            }

            return {{replica.group_id, replica_leader, replica.broker_shard}};
        }

        return std::nullopt;
    }

    void reset() { _replicas_begin = 0; }

    void update_index(const reassignment& r) {
        _current_leaders[r.group] = r.to;
        reset();
    }

private:
    struct replica {
        raft::group_id group_id;
        model::broker_shard broker_shard;
    };
    chunked_hash_map<raft::group_id, model::broker_shard> _current_leaders;

    using replicas_t = chunked_vector<replica>;
    replicas_t _replicas;
    size_t _replicas_begin{0};
};

template<typename T>
concept climbing_strategy_impl = requires(T& t, const reassignment& r) {
    typename T::reassignment_score;
    {
        t.get_reassignment_score(r)
    } -> std::same_as<std::optional<typename T::reassignment_score>>;
    { t.generate_reassignment() } -> std::same_as<std::optional<reassignment>>;
};

template<typename Impl>
class climbing_strategy_base : public leader_balancer_strategy {
protected:
    using base = climbing_strategy_base<Impl>;

public:
    climbing_strategy_base(
      index_type index,
      group_id_to_topic_id g_to_topic,
      muted_index mi,
      std::optional<preference_index> preference_idx)
      : _mi(std::make_unique<muted_index>(std::move(mi)))
      , _group2topic(
          std::make_unique<group_id_to_topic_id>(std::move(g_to_topic)))
      , _si(std::make_unique<shard_index>(std::move(index)))
      , _reassignments(_si->shards())
      , _etdc(*_group2topic, *_si, *_mi)
      , _eslc(*_si, *_mi)
      , _enlc(*_si) {
        static_assert(
          climbing_strategy_impl<Impl>,
          "Impl must satisfy climbing_strategy_impl concept");
        if (preference_idx) {
            _pinning_constr.emplace(
              *_group2topic, std::move(preference_idx.value()));
        }
    }

    double error() const override { return _eslc.error() + _etdc.error(); }

    void apply_movement(const reassignment& reassignment) override {
        _etdc.update_index(reassignment);
        _eslc.update_index(reassignment);
        _enlc.update_index(reassignment);
        _mi->update_index(reassignment);
        _si->update_index(reassignment);
        _reassignments.update_index(reassignment);
    }

    /*
     * Return current strategy stats.
     */
    std::vector<shard_load> stats() const override { return _eslc.stats(); }

protected:
    static constexpr double error_jitter = 0.000001;

    struct reassignment_with_score {
        reassignment reassignment;
        Impl::reassignment_score score;
    };

    std::optional<reassignment> do_find_movement_without_score(
      const leader_balancer_types::muted_groups_t& skip) {
        return do_find_movement_with_score(skip).transform(
          [](auto&& s) { return s.reassignment; });
    }

    std::optional<reassignment_with_score> do_find_movement_with_score(
      const leader_balancer_types::muted_groups_t& skip) {
        for (;;) {
            auto reassignment_opt
              = static_cast<Impl&>(*this).generate_reassignment();

            if (!reassignment_opt) {
                return std::nullopt;
            }

            auto reassignment = *reassignment_opt;
            if (
              skip.contains(static_cast<uint64_t>(reassignment.group))
              || _mi->muted_nodes().contains(reassignment.from.node_id)
              || _mi->muted_nodes().contains(reassignment.to.node_id)) {
                continue;
            }

            if (
              auto maybe_score = static_cast<Impl&>(*this)
                                   .get_reassignment_score(reassignment)) {
                return {{reassignment, *maybe_score}};
            }
        }

        return std::nullopt;
    }

private:
    std::unique_ptr<muted_index> _mi;
    std::unique_ptr<group_id_to_topic_id> _group2topic;
    std::unique_ptr<shard_index> _si;

protected:
    random_reassignments _reassignments;
    std::optional<pinning_constraint> _pinning_constr;
    even_topic_distribution_constraint _etdc;
    even_shard_load_constraint _eslc;
    even_node_load_constraint _enlc;
};

/// A greedy random-walk hill-climbing strategy for leader rebalancing.
///
/// On each call to find_movement the strategy draws a random leader
/// reassignment from the full set of possible moves and returns the first one
/// that strictly reduces the combined error across a hierarchy of scores:
///   1. Pinning (leader-preference compliance)
///   2. Shard-level load balance (even topic distribution + even shard load)
///   3. Node-level load balance
/// Each score is only considered if all higher-level scores are not impacted by
/// the move (i.e. changes are within a small jitter threshold).
/// Each reassignment is only considered once.
class random_hill_climbing_strategy final
  : public climbing_strategy_base<random_hill_climbing_strategy> {
    friend base;

public:
    using base::base;

    std::optional<reassignment>
    find_movement(const leader_balancer_types::muted_groups_t& skip) override {
        return do_find_movement_without_score(skip);
    }

private:
    using reassignment_score = std::monostate;

    // returns score if reassignment is acceptable, nullopt otherwise
    std::optional<reassignment_score>
    get_reassignment_score(const reassignment& r) {
        // Hierarchical optimization: first check if the proposed
        // reassignment improves the pinning objective (makes the leaders
        // distribution better conform to the provided pinning
        // configuration). If the pinning objective remains at the same
        // level, check balancing objectives.
        if (_pinning_constr) {
            auto pinning_diff = _pinning_constr->evaluate(r);
            if (pinning_diff < -error_jitter) {
                return std::nullopt;
            } else if (pinning_diff > error_jitter) {
                return std::monostate{};
            }
        }

        auto shard_load_diff = _etdc.evaluate(r) + _eslc.evaluate(r);
        if (shard_load_diff < -error_jitter) {
            return std::nullopt;
        } else if (shard_load_diff > error_jitter) {
            return std::monostate{};
        }

        auto node_load_diff = _enlc.evaluate(r);
        if (node_load_diff < -error_jitter) {
            return std::nullopt;
        } else if (node_load_diff > error_jitter) {
            return std::monostate{};
        }

        return std::nullopt;
    }

    std::optional<reassignment> generate_reassignment() {
        return this->_reassignments.generate_reassignment();
    }
};

} // namespace cluster::leader_balancer_types

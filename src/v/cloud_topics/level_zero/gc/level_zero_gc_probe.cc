/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/gc/level_zero_gc_probe.h"

#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_types.hh>
#include <seastar/core/shared_ptr.hh>

namespace cloud_topics {

level_zero_gc_probe::level_zero_gc_probe(bool disable) {
    setup_internal_metrics(disable);
}

void level_zero_gc_probe::setup_internal_metrics(bool disable) {
    if (disable) {
        return;
    }

    namespace sm = ss::metrics;
    std::vector<sm::label_instance> labels;

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_l0_gc"),
      {
        sm::make_counter(
          "objects_deleted_total",
          [this] { return objects_deleted_; },
          sm::description(
            "Number of L0 objects deleted by garbage collection."),
          labels),
        sm::make_counter(
          "list_errors_total",
          [this] { return list_errors_; },
          sm::description(
            "Number of LIST errors during L0 garbage collection."),
          labels),
        sm::make_counter(
          "delete_errors_total",
          [this] { return delete_errors_; },
          sm::description(
            "Number of DELETE errors during L0 garbage collection."),
          labels),
        sm::make_gauge(
          "epoch_lag",
          [this] { return epoch_lag(); },
          sm::description(
            "Difference between max GC eligible epoch and oldest epoch being "
            "deleted."),
          labels),
      });
}

void level_zero_gc_probe::report_deletion_epoch(cluster_epoch epoch) {
    if (reset_deletion_epoch_) {
        min_deletion_epoch_ = epoch;
        reset_deletion_epoch_ = false;
    } else {
        min_deletion_epoch_ = std::min(
          min_deletion_epoch_.value_or(epoch), epoch);
    }
}

cloud_topics::cluster_epoch::type level_zero_gc_probe::epoch_lag() const {
    // If max_gc_eligible_epoch_ is:
    //   - negative: no epoch is GC eligible, globally
    //   - nullopt: we haven't made a GC pass yet to populate it
    // in either case, there is no lag to compute as such, so we call it 0 lag
    if (max_gc_eligible_epoch_ < cloud_topics::cluster_epoch{0}) {
        return 0;
    }
    if (!min_deletion_epoch_.has_value()) {
        return max_gc_eligible_epoch_.value()();
    }
    return max_gc_eligible_epoch_.value()() - min_deletion_epoch_.value()();
}

} // namespace cloud_topics

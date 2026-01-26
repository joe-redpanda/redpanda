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

#include "base/format_to.h"
#include "cloud_storage_clients/bucket_name_parts.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/detail/registry.h"
#include "cloud_storage_clients/upstream.h"

#include <seastar/core/sharded.hh>

namespace cloud_storage_clients {

/// Key identifying an upstream service instance.
struct upstream_key {
    cloud_roles::aws_region_name region;
    cloud_storage_clients::endpoint_url endpoint;

    /// Takes precedence over endpoint if set. When using virtual host
    /// addressing, this address includes the bucket name as a prefix.
    ///
    /// For Remote Read Replicas with endpoint overrides, this creates a unique
    /// upstream key per bucket, ensuring clients connect to correct servers.
    ///
    /// For Remote Read Replicas without endpoint overrides, this remains
    /// std::nullopt and for now follow original behavior of using the default
    /// bucket server address for the TCP connection but custom HOST header.
    std::optional<net::unresolved_address> server_addr;

    auto operator<=>(const upstream_key&) const = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "upstream_key{{region={}, endpoint={}, server_addr={}}}",
          region().empty() ? "<default>" : region(),
          endpoint().empty() ? "<default>" : endpoint(),
          server_addr.has_value() ? fmt::format("{}", *server_addr)
                                  : "<default>");
    }
};

upstream_key make_upstream_key(
  const client_configuration& config, const bucket_name_parts& bucket);

/// Registry for upstream cloud storage clients.
class upstream_registry final
  : public detail::basic_registry<upstream, upstream_key, upstream_registry>
  , public ss::peering_sharded_service<upstream_registry> {
public:
    explicit upstream_registry(client_configuration config);

    ss::future<> start();

    ss::shared_ptr<client_probe> probe() const noexcept { return _probe; }

protected:
    ss::future<>
    start_svc(sharded_constructor& ctor, const upstream_key&) final;

private:
    client_configuration _config;
    ss::shared_ptr<ss::tls::certificate_credentials> _tls_credentials;
    ss::shared_ptr<client_probe> _probe;
};

extern template class detail::
  basic_registry<upstream, upstream_key, upstream_registry>;

} // namespace cloud_storage_clients

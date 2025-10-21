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

#include "redpanda/admin/services/broker.h"

#include "features/feature_table.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"
#include "version/version.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger brlog{"admin_api_server/broker_service"};

} // namespace

broker_service_impl::broker_service_impl(
  admin::proxy::client client,
  std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services,
  ss::sharded<kafka_connections_service>& kafka_connections_service,
  ss::sharded<features::feature_table>& feature_table)
  : _proxy_client(std::move(client))
  , _services(services)
  , _kafka_connections_service(kafka_connections_service)
  , _feature_table(feature_table) {}

ss::future<proto::admin::get_broker_response> broker_service_impl::get_broker(
  serde::pb::rpc::context ctx, proto::admin::get_broker_request req) {
    auto target = model::node_id(req.get_node_id());
    if (target != -1 && target != _proxy_client.self_node_id()) {
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::broker_service_client>(target)
          .get_broker(ctx, std::move(req));
    }
    proto::admin::get_broker_response resp;
    resp.set_broker(self_broker());
    co_return resp;
}

ss::future<proto::admin::list_brokers_response>
broker_service_impl::list_brokers(
  serde::pb::rpc::context ctx, proto::admin::list_brokers_request) {
    proto::admin::list_brokers_response list_resp;
    list_resp.get_brokers().push_back(self_broker());
    auto clients
      = _proxy_client
          .make_clients_for_other_nodes<proto::admin::broker_service_client>();
    for (auto& [node_id, client] : clients) {
        proto::admin::get_broker_request req;
        req.set_node_id(node_id);
        auto get_resp = co_await client.get_broker(ctx, std::move(req));
        list_resp.get_brokers().push_back(std::move(get_resp.get_broker()));
    }
    co_return list_resp;
}

proto::admin::broker broker_service_impl::self_broker() const {
    proto::admin::broker b;
    b.set_node_id(_proxy_client.self_node_id());
    b.get_build_info().set_version(ss::sstring(redpanda_git_version()));
    b.get_build_info().set_build_sha(ss::sstring(redpanda_git_revision()));
    for (auto& service : *_services) {
        for (auto& route : service->all_routes()) {
            proto::rpc_route r;
            r.set_name(
              fmt::format("{}.{}", route.service_name, route.method_name));
            r.set_http_route(ss::sstring(route.path));
            b.get_admin_server().get_routes().push_back(std::move(r));
        }
    }
    return b;
}

ss::future<proto::admin::list_kafka_connections_response>
broker_service_impl::list_kafka_connections(
  serde::pb::rpc::context ctx,
  proto::admin::list_kafka_connections_request req) {
    vlog(brlog.trace, "list_kafka_connections: {}", req);

    utils::check_license(_feature_table.local());

    // Proxy to the target node id specified in the request
    auto target = model::node_id{req.get_node_id()};
    if (target != -1 && target != _proxy_client.self_node_id()) {
        vlog(brlog.debug, "Redirecting to target node id {}", target);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::broker_service_client>(target)
          .list_kafka_connections(ctx, std::move(req));
    }

    auto resp = co_await _kafka_connections_service.local()
                  .list_kafka_connections_local(std::move(req));

    vlog(
      brlog.trace,
      "list_kafka_connections: response connections: {} ({}b), total matching: "
      "{}",
      resp.get_connections().size(),
      resp.get_connections().memory_size(),
      resp.get_total_size());

    co_return resp;
}

} // namespace admin

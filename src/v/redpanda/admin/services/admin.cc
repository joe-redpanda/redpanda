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

#include "redpanda/admin/services/admin.h"

#include "version/version.h"

namespace proto {
using namespace proto::admin;
}

namespace admin {

admin_service_impl::admin_service_impl(
  std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services)
  : _services(services) {}

ss::future<proto::get_version_response>
admin::admin_service_impl::get_version(proto::get_version_request) {
    proto::get_version_response resp;
    resp.set_version(ss::sstring(redpanda_git_version()));
    resp.set_build_sha(ss::sstring(redpanda_git_revision()));
    co_return resp;
}

ss::future<proto::get_routes_response>
admin::admin_service_impl::get_routes(proto::get_routes_request) {
    proto::get_routes_response resp;
    for (auto& service : *_services) {
        for (auto& route : service->all_routes()) {
            proto::get_routes_response_route r;
            r.set_name(std::move(route.name));
            r.set_http_route(fmt::format("/v2{}", route.path));
            resp.get_routes().push_back(std::move(r));
        }
    }
    co_return resp;
}

} // namespace admin

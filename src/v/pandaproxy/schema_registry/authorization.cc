/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "pandaproxy/schema_registry/authorization.h"

#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/types.h"
#include "security/acl.h"
#include "security/authorizer.h"

#include <seastar/util/variant_utils.hh>

namespace pandaproxy::schema_registry::enterprise {

namespace detail {

template<typename T>
concept no_auth = std::is_same_v<T, auth::none>
                  || std::is_same_v<T, auth::deferred>;

template<typename T>
concept requires_auth = !no_auth<T>;

struct auth_params {
    security::acl_principal principal;
    security::acl_host host;

    explicit auth_params(const server::request_t& rq)
      : principal{security::principal_type::user, rq.user.name}
      , host{rq.req->get_client_address().addr()} {}
};

} // namespace detail

namespace {

auth::resource
extract_resource_from_request(const server::request_t& rq, const auth& auth) {
    auto resource = auth.get_resource();
    ss::visit(
      resource,
      [&rq](subject& sub) {
          sub = parse::request_param<subject>(*rq.req, "subject");
      },
      [](const auto&) {});
    return resource;
}

void throw_unauthorized() {
    throw ss::httpd::base_exception(
      "Forbidden (missing required ACLs)",
      ss::http::reply::status_type::forbidden);
}

void check_authenticated(request_auth_result& auth_result) {
    try {
        auth_result.require_authenticated();
    } catch (...) {
        // TODO(CORE-12275): audit failure
        throw;
    }
}

} // namespace

void handle_authz(
  const server::request_t& rq,
  const auth& auth,
  request_auth_result& auth_result) {
    auto params = detail::auth_params{rq};
    auto op = auth.get_op().value_or(security::acl_operation::all);

    auto resource = extract_resource_from_request(rq, auth);

    ss::visit(
      resource,
      [&](const auth::none&) { auth_result.pass(); },
      [&](const auto&) { check_authenticated(auth_result); });

    // Check Authorization
    auto authz_result = ss::visit(
      resource,
      [&](const detail::requires_auth auto& resource_name) {
          return rq.service().authorizor().authorized(
            resource_name, op, params.principal, params.host);
      },
      [&](const detail::no_auth auto&) {
          return security::auth_result::authz_disabled(
            params.principal, params.host, op, registry_resource{});
      });

    if (authz_result.is_authorized()) {
        // TODO(CORE-12275): audit success
    } else {
        // TODO(CORE-12275): audit failure
        throw_unauthorized();
    }
}

} // namespace pandaproxy::schema_registry::enterprise

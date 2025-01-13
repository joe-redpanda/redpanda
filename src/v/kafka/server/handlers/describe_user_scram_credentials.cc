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
#include "kafka/server/handlers/describe_user_scram_credentials.h"

#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/security.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/scram_credential.h"

namespace kafka {

namespace {
credential_info
scram_credential_to_credential_info(const security::scram_credential& c) {
    return {
      .mechanism = details::key_size_to_mechanism(c.stored_key().size()),
      .iterations = c.iterations(),
    };
};
} // namespace
template<>
ss::future<response_ptr> describe_user_scram_credentials_handler::handle(
  request_context ctx, ss::smp_service_group) {
    describe_user_scram_credentials_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_user_scram_credentials_response res;

    if (!ctx.authorized(
          security::acl_operation::describe, security::default_cluster_name)) {
        res.data.error_code = error_code::cluster_authorization_failed;
        res.data.error_message = ss::sstring{
          error_code_to_str(error_code::cluster_authorization_failed)};
        return ctx.respond(std::move(res));
    }

    if (!ctx.audit()) {
        res.data.error_code = error_code::broker_not_available;
        res.data.error_message = "Broker not available - audit system failure";
        return ctx.respond(std::move(res));
    }

    const auto list_all_users = !request.data.users.has_value()
                                || request.data.users.value().empty();

    if (list_all_users) {
        for (const auto& c : ctx.credentials().range(
               security::credential_store::is_not_ephemeral)) {
            const auto& creds = std::get<security::scram_credential>(c.second);
            res.data.results.emplace_back(
              describe_user_scram_credentials_result{
                .user = scram_user_name{c.first},
                .credential_infos = {
                  scram_credential_to_credential_info(creds)}});
        }
    }

    return ctx.respond(std::move(res));
}
} // namespace kafka

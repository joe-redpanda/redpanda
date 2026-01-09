
#pragma once

#include "cluster/types.h"

#include <expected>

namespace cluster {

// interface for cluster configuration
class config_i {
public:
    virtual std::expected<config_version, std::error_code>
    get_config() const noexcept = 0;

    virtual ~config_i() = default;
};
} // namespace cluster

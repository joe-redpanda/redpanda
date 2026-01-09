
#pragma once

#include "cluster/config/config_i.h"
#include "cluster/config_manager.h"

namespace cluster {

class cluster_config : public config_i {
public:
    explicit cluster_config(
      ss::sharded<config_manager>& config_manager) noexcept;
    std::expected<config_version, std::error_code>
    get_config() const noexcept override;

private:
    ss::sharded<config_manager>& _config_manager;
};
} // namespace cluster

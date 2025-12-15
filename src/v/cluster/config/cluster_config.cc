
#include "cluster/config/cluster_config.h"

#include "cluster/config_manager.h"
#include "cluster/types.h"

namespace cluster {
cluster_config::cluster_config(
  ss::sharded<config_manager>& config_manager) noexcept
  : _config_manager(config_manager) {}

std::expected<config_version, std::error_code>
cluster_config::get_config() const noexcept {
    vassert(
      ss::this_shard_id() == cluster::config_manager::shard,
      "This operation can only be invoked on the config_manager shard");
    return _config_manager.local().get_version();
}

} // namespace cluster

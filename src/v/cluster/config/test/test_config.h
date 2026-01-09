
#pragma once

#include "cluster/config/config_i.h"
#include "cluster/types.h"

namespace cluster::test {
class test_config : public config_i {
public:
    using functor_t
      = std::function<std::expected<config_version, std::error_code>()>;

    explicit test_config(functor_t function) noexcept
      : _function(std::move(function)) {}

    std::expected<config_version, std::error_code>
    get_config() const noexcept override {
        return _function();
    }

    static inline std::unique_ptr<config_i> make_default() {
        return std::make_unique<test_config>([] {
            return std::expected<config_version, std::error_code>{
              config_version{0}};
        });
    }

private:
    functor_t _function;
};
} // namespace cluster::test

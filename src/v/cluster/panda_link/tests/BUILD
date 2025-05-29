load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "panda_link_test_lib",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    implementation_deps = [
        "//src/v/cluster",
    ],
    include_prefix = "cluster/panda_link/tests",
    deps = [
        "//src/v/model",
        "//src/v/panda_link/model",
    ],
)

redpanda_cc_gtest(
    name = "panda_link_table_test",
    timeout = "short",
    srcs = [
        "panda_link_table_test.cc",
    ],
    deps = [
        ":panda_link_test_lib",
        "//src/v/cluster",
        "//src/v/panda_link/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

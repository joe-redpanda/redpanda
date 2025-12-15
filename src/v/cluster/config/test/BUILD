load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "test_config",
    hdrs = [
        "test_config.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
    ],
)

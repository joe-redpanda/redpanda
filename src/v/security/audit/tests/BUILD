load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    srcs = ["audit_test_utils.cc"],
    hdrs = ["audit_test_utils.h"],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
    ],
)

redpanda_cc_btest(
    name = "audit_test",
    timeout = "short",
    srcs = ["audit_log_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        ":utils",
        "//src/v/cluster",
        "//src/v/kafka/client/test:fixture",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

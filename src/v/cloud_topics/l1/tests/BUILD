load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "state_test",
    timeout = "short",
    srcs = [
        "state_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics/l1:state",
        "//src/v/serde",
        "//src/v/serde:uuid",
        "//src/v/serde:named_type",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "state_update_test",
    timeout = "short",
    srcs = [
        "state_update_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/l1:state",
        "//src/v/cloud_topics/l1:state_update",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
    ],
)

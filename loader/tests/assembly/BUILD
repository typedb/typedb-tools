# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//visibility:__subpackages__"])

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test-loader",
    srcs = [
        "test_loader.rs",
    ],
    deps = [
        "//tool/runner:typedb_binary_runner",

        # External dependencies
        "@crates//:duckdb",
    ],
    env = select({
        "@typedb_bazel_distribution//platform:is_linux_x86_64" : {
            "TYPEDB_LOADER_ARCHIVE": "$(rootpath //loader:assemble-linux-x86_64-targz)",
            "TYPEDB_SERVER_ARCHIVE": "$(rootpath @typedb_artifact_linux-x86_64//file)",
            "SCHEMA_PATH": "$(rootpath schema.tql)",
            "QUERY_PATH": "$(rootpath insert.tql)",
            "DATA_PATH": "$(rootpath data.csv)",
            "DATA_WITH_REJECTS_PATH": "$(rootpath data-with-rejects.csv)",
            "SCHEMA_WITH_FRIENDSHIPS_PATH": "$(rootpath schema-with-friendships.tql)",
            "QUERY_FRIENDSHIP_PATH": "$(rootpath insert-friendship.tql)",
            "DATA_FRIENDSHIPS_PATH": "$(rootpath friendships.csv)",
         },
        "@typedb_bazel_distribution//platform:is_linux_arm64" : {
            "TYPEDB_LOADER_ARCHIVE": "$(rootpath //loader:assemble-linux-arm64-targz)",
            "TYPEDB_SERVER_ARCHIVE": "$(rootpath @typedb_artifact_linux-arm64//file)",
            "SCHEMA_PATH": "$(rootpath schema.tql)",
            "QUERY_PATH": "$(rootpath insert.tql)",
            "DATA_PATH": "$(rootpath data.csv)",
            "DATA_WITH_REJECTS_PATH": "$(rootpath data-with-rejects.csv)",
            "SCHEMA_WITH_FRIENDSHIPS_PATH": "$(rootpath schema-with-friendships.tql)",
            "QUERY_FRIENDSHIP_PATH": "$(rootpath insert-friendship.tql)",
            "DATA_FRIENDSHIPS_PATH": "$(rootpath friendships.csv)",
        },
        "@typedb_bazel_distribution//platform:is_mac_x86_64" : {
            "TYPEDB_LOADER_ARCHIVE": "$(rootpath //loader:assemble-mac-x86_64-zip)",
            "TYPEDB_SERVER_ARCHIVE": "$(rootpath @typedb_artifact_mac-x86_64//file)",
            "SCHEMA_PATH": "$(rootpath schema.tql)",
            "QUERY_PATH": "$(rootpath insert.tql)",
            "DATA_PATH": "$(rootpath data.csv)",
            "DATA_WITH_REJECTS_PATH": "$(rootpath data-with-rejects.csv)",
            "SCHEMA_WITH_FRIENDSHIPS_PATH": "$(rootpath schema-with-friendships.tql)",
            "QUERY_FRIENDSHIP_PATH": "$(rootpath insert-friendship.tql)",
            "DATA_FRIENDSHIPS_PATH": "$(rootpath friendships.csv)",
        },
        "@typedb_bazel_distribution//platform:is_mac_arm64" : {
            "TYPEDB_LOADER_ARCHIVE": "$(rootpath //loader:assemble-mac-arm64-zip)",
            "TYPEDB_SERVER_ARCHIVE": "$(rootpath @typedb_artifact_mac-arm64//file)",
            "SCHEMA_PATH": "$(rootpath schema.tql)",
            "QUERY_PATH": "$(rootpath insert.tql)",
            "DATA_PATH": "$(rootpath data.csv)",
            "DATA_WITH_REJECTS_PATH": "$(rootpath data-with-rejects.csv)",
            "SCHEMA_WITH_FRIENDSHIPS_PATH": "$(rootpath schema-with-friendships.tql)",
            "QUERY_FRIENDSHIP_PATH": "$(rootpath insert-friendship.tql)",
            "DATA_FRIENDSHIPS_PATH": "$(rootpath friendships.csv)",
        },
        "@typedb_bazel_distribution//platform:is_windows_x86_64" : {
            "TYPEDB_LOADER_ARCHIVE": "$(rootpath //loader:assemble-windows-x86_64-zip)",
            "TYPEDB_SERVER_ARCHIVE": "$(rootpath @typedb_artifact_windows-x86_64//file)",
            "SCHEMA_PATH": "$(rootpath schema.tql)",
            "QUERY_PATH": "$(rootpath insert.tql)",
            "DATA_PATH": "$(rootpath data.csv)",
            "DATA_WITH_REJECTS_PATH": "$(rootpath data-with-rejects.csv)",
            "SCHEMA_WITH_FRIENDSHIPS_PATH": "$(rootpath schema-with-friendships.tql)",
            "QUERY_FRIENDSHIP_PATH": "$(rootpath insert-friendship.tql)",
            "DATA_FRIENDSHIPS_PATH": "$(rootpath friendships.csv)",
        },
    }),
    data = [
        "schema.tql",
        "insert.tql",
        "data.csv",
        "data-with-rejects.csv",
        "schema-with-friendships.tql",
        "insert-friendship.tql",
        "friendships.csv",
    ]
    + select({
        "@typedb_bazel_distribution//platform:is_linux_x86_64": [
            "//loader:assemble-linux-x86_64-targz",
            "@typedb_artifact_linux-x86_64//file",
        ],
        "@typedb_bazel_distribution//platform:is_linux_arm64" :  [
            "//loader:assemble-linux-arm64-targz",
            "@typedb_artifact_linux-arm64//file",
        ],
        "@typedb_bazel_distribution//platform:is_mac_x86_64" : [
            "//loader:assemble-mac-x86_64-zip",
            "@typedb_artifact_mac-x86_64//file",
        ],
        "@typedb_bazel_distribution//platform:is_mac_arm64" : [
            "//loader:assemble-mac-arm64-zip",
            "@typedb_artifact_mac-arm64//file",
        ],
        "@typedb_bazel_distribution//platform:is_windows_x86_64" : [
            "//loader:assemble-windows-x86_64-zip",
            "@typedb_artifact_windows-x86_64//file",
        ],
    }),
    use_libtest_harness = True,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["**/*.rs", "BUILD"]),
    exclude = [
        "schema.tql",
        "insert.tql",
        "data.csv",
        "data-with-rejects.csv",
        "schema-with-friendships.tql",
        "insert-friendship.tql",
        "friendships.csv",
    ],
    license_type = "mpl-header",
)

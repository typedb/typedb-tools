# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//visibility:__subpackages__"])

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test-migration-cli",
    srcs = ["test_migration_cli.rs"],
    deps = [
        "@typedb_protocol//grpc/rust:typedb_protocol",

        # External dependencies
        "@crates//:duckdb",
        "@crates//:prost",
    ],
    data = ["//loader:loader-native"],
    env = {"TYPEDB_LOADER_BIN": "$(rootpath //loader:loader-native)"},
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)

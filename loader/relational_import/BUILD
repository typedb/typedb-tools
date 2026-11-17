# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")

rust_library(
    name = "typedb_relational_import",
    srcs = glob(["*.rs"]),
    crate_root = "lib.rs",
    deps = [
        "//common:typedb_cli_common",
        "@typedb_protocol//grpc/rust:typedb_protocol",
        "@typeql//rust:typeql",

        # External dependencies
        "@crates//:duckdb",
        "@crates//:prost",
        "@crates//:zstd",
    ],
    visibility = ["//visibility:public"],
    tags = ["crate-name=typedb-relational-import"],
)

rust_test(
    name = "unit-tests",
    crate = ":typedb_relational_import",
)

rust_test(
    name = "test-relational-import",
    srcs = ["tests/relational_import.rs"],
    deps = [
        ":typedb_relational_import",
        "@typedb_protocol//grpc/rust:typedb_protocol",

        # External dependencies
        "@crates//:duckdb",
        "@crates//:prost",
        "@crates//:zstd",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    exclude = glob(["Cargo.*", "target/**"]) + ["README.md"],
    license_type = "mpl-header",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [":typedb_relational_import"],
    size = "small",
)

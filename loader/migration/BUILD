# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")

rust_library(
    name = "typedb_migration",
    srcs = glob(["*.rs"]),
    crate_root = "lib.rs",
    deps = [
        "@typedb_protocol//grpc/rust:typedb_protocol",
        "@typeql//rust:typeql",

        # External dependencies
        "@crates//:duckdb",
        "@crates//:prost",
    ],
    visibility = ["//visibility:public"],
    tags = ["crate-name=typedb-migration"],
)

rust_test(
    name = "unit-tests",
    crate = ":typedb_migration",
)

rust_test(
    name = "test-migration",
    srcs = ["tests/migration.rs"],
    deps = [
        ":typedb_migration",
        "@typedb_protocol//grpc/rust:typedb_protocol",

        # External dependencies
        "@crates//:duckdb",
        "@crates//:prost",
    ],
)

# Schema-coverage harness over an external corpus: skips unless SCHEMAS_DIR is set, so it is
# run by hand rather than in CI (see the file's header).
rust_test(
    name = "behaviour-schemas",
    srcs = ["tests/behaviour_schemas.rs"],
    deps = [
        ":typedb_migration",

        # External dependencies
        "@crates//:duckdb",
    ],
    tags = ["manual"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    exclude = glob(["Cargo.*", "target/**"]) + ["README.md"],
    license_type = "mpl-header",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [":typedb_migration"],
    size = "small",
)

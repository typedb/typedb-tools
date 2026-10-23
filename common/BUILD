# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

load("@rules_rust//rust:defs.bzl", "rust_library")

rust_library(
    name = "typedb_cli_common",
    srcs = glob(["*.rs"]),
    crate_root = "lib.rs",
    deps = [
        "@typedb_driver//rust:typedb_driver",

        # External dependencies
        "@crates//:clap",
    ],
    visibility = ["//visibility:public"],
    tags = ["crate-name=typedb_cli_common"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    exclude = glob(["Cargo.*", "target/**"]),
    license_type = "mpl-header",
)

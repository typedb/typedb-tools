# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//visibility:__subpackages__"])

load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@typedb_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@typedb_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip")
load("@typedb_dependencies//distribution:deployment.bzl", "deployment")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@typedb_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")

load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test", "rustfmt_test")

rust_binary(
    name = "loader-native",
    srcs = glob(["*.rs"]),
    deps = [
        "//common:typedb_cli_common",
        "//loader/migration:typedb_migration",
        "@typedb_driver//rust:typedb_driver",
        "@typeql//rust:typeql",

        # External dependencies
        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:clap",
        "@crates//:csv",
        "@crates//:futures",
        "@crates//:rpassword",
        "@crates//:serde",
        "@crates//:serde_json",
        "@crates//:sha2",
        "@crates//:tokio",
    ],
    compile_data = ["//:VERSION"],
    tags = [
        "crate-name=typedb-loader"
    ],
    visibility = ["//visibility:public"],
)

rust_test(
    name = "loader-unit-tests",
    crate = ":loader-native",
)

pkg_tar(
    name = "loader-artifact-native",
    files = {":loader-native" : "loader/typedb_loader_bin"},
    extension = "tar.gz",
    visibility = ["//visibility:public"]
)

assemble_targz(
    name = "assemble-linux-x86_64-targz",
    output_filename = "typedb-loader-linux-x86_64",
    targets = [":loader-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_linux_x86_64,
)

assemble_targz(
    name = "assemble-linux-arm64-targz",
    output_filename = "typedb-loader-linux-arm64",
    targets = [":loader-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_linux_arm64
)

assemble_zip(
    name = "assemble-mac-x86_64-zip",
    output_filename = "typedb-loader-mac-x86_64",
    targets = [":loader-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_mac_x86_64
)

assemble_zip(
    name = "assemble-mac-arm64-zip",
    output_filename = "typedb-loader-mac-arm64",
    targets = [":loader-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_mac_arm64
)

assemble_zip(
    name = "assemble-windows-x86_64-zip",
    output_filename = "typedb-loader-windows-x86_64",
    targets = [":loader-artifact-native", "//binary:assemble-bat-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_win_x86_64
)

deploy_artifact(
    name = "deploy-linux-x86_64-targz",
    target = ":assemble-linux-x86_64-targz",
    artifact_group = "typedb-loader-linux-x86_64",
    artifact_name = "typedb-loader-linux-x86_64-{version}.tar.gz",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-linux-arm64-targz",
    target = ":assemble-linux-arm64-targz",
    artifact_group = "typedb-loader-linux-arm64",
    artifact_name = "typedb-loader-linux-arm64-{version}.tar.gz",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-mac-x86_64-zip",
    target = ":assemble-mac-x86_64-zip",
    artifact_group = "typedb-loader-mac-x86_64",
    artifact_name = "typedb-loader-mac-x86_64-{version}.zip",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-mac-arm64-zip",
    target = ":assemble-mac-arm64-zip",
    artifact_group = "typedb-loader-mac-arm64",
    artifact_name = "typedb-loader-mac-arm64-{version}.zip",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-windows-x86_64-zip",
    target = ":assemble-windows-x86_64-zip",
    artifact_group = "typedb-loader-windows-x86_64",
    artifact_name = "typedb-loader-windows-x86_64-{version}.zip",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"], exclude = ["tests/**"]),
    exclude = glob([
        "*.md",
        "Cargo.*"
    ]),
    license_type = "mpl-header",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [":loader-native"],
    size = "small",
)

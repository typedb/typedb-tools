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

load("@rules_rust//rust:defs.bzl", "rust_binary", "rustfmt_test")

rust_binary(
    name = "console-native",
    srcs = glob(["**/*.rs"], exclude = ["tests/**"]),
    deps = [
        "//common:typedb_cli_common",
        "@typedb_driver//rust:typedb_driver",
        "@typeql//rust:typeql",

        # External dependencies
        "@crates//:clap",
        "@crates//:futures",
        "@crates//:glob",
        "@crates//:home",
        "@crates//:itertools",
        "@crates//:rpassword",
        "@crates//:rustyline",
        "@crates//:serde_json",
        "@crates//:sha2",
        "@crates//:ureq",
        "@crates//:tokio",
    ],
    compile_data = ["//:VERSION"],
    tags = [
        "crate-name=typedb-console"
    ],
    visibility = ["//visibility:public"],
)

pkg_tar(
    name = "console-artifact-native",
    files = {":console-native" : "console/typedb_console_bin"},
    extension = "tar.gz",
    visibility = ["//visibility:public"]
)

assemble_targz(
    name = "assemble-linux-x86_64-targz",
    output_filename = "typedb-console-linux-x86_64",
    targets = [":console-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_linux_x86_64,
)

assemble_targz(
    name = "assemble-linux-arm64-targz",
    output_filename = "typedb-console-linux-arm64",
    targets = [":console-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_linux_arm64
)

assemble_zip(
    name = "assemble-mac-x86_64-zip",
    output_filename = "typedb-console-mac-x86_64",
    targets = [":console-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_mac_x86_64
)

assemble_zip(
    name = "assemble-mac-arm64-zip",
    output_filename = "typedb-console-mac-arm64",
    targets = [":console-artifact-native", "//binary:assemble-bash-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_mac_arm64
)

assemble_zip(
    name = "assemble-windows-x86_64-zip",
    output_filename = "typedb-console-windows-x86_64",
    targets = [":console-artifact-native", "//binary:assemble-bat-targz"],
    visibility = ["//visibility:public"],
    target_compatible_with = constraint_win_x86_64
)

deploy_artifact(
    name = "deploy-linux-x86_64-targz",
    target = ":assemble-linux-x86_64-targz",
    artifact_group = "typedb-console-linux-x86_64",
    artifact_name = "typedb-console-linux-x86_64-{version}.tar.gz",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-linux-arm64-targz",
    target = ":assemble-linux-arm64-targz",
    artifact_group = "typedb-console-linux-arm64",
    artifact_name = "typedb-console-linux-arm64-{version}.tar.gz",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-mac-x86_64-zip",
    target = ":assemble-mac-x86_64-zip",
    artifact_group = "typedb-console-mac-x86_64",
    artifact_name = "typedb-console-mac-x86_64-{version}.zip",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-mac-arm64-zip",
    target = ":assemble-mac-arm64-zip",
    artifact_group = "typedb-console-mac-arm64",
    artifact_name = "typedb-console-mac-arm64-{version}.zip",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

deploy_artifact(
    name = "deploy-windows-x86_64-zip",
    target = ":assemble-windows-x86_64-zip",
    artifact_group = "typedb-console-windows-x86_64",
    artifact_name = "typedb-console-windows-x86_64-{version}.zip",
    snapshot = deployment['artifact']['snapshot']['upload'],
    release = deployment['artifact']['release']['upload'],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["**/*.rs", "BUILD"], exclude = ["tests/**"]),
    exclude = glob(["Cargo.*"]),
    license_type = "mpl-header",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [":console-native"],
    size = "small",
)

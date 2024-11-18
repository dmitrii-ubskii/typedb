# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def typedb_bazel_distribution():
    git_repository(
        name = "typedb_bazel_distribution",
        remote = "https://github.com/typedb/bazel-distribution",
        commit = "94c4f7b1dda39bf187f73c6ea035971c4c91528b",
    )

def typedb_dependencies():
    git_repository(
        name = "typedb_dependencies",
        remote = "https://github.com/typedb/typedb-dependencies",
        commit = "70e8b662d2b3f10bba64befcc2c5183949eb9efa", # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    native.local_repository(
        name = "typeql",
        path = "../typeql-all/typeql",
    )
    git_repository(
        name = "typeql_",
        remote = "https://github.com/krishnangovindraj/typeql",
        commit = "32ffea3e919b189475177d0850b0c8d19b40b157",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typeql
    )


def typedb_common():
    git_repository(
        name = "typedb_common",
        remote = "https://github.com/typedb/typedb-common",
        tag = "2.25.3",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_common
    )

def typedb_protocol():
    git_repository(
        name = "typedb_protocol",
        remote = "https://github.com/typedb/typedb-protocol",
        commit = "8e316bb783a6315c881f3cba54d9afe52fef5edb",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    native.local_repository(
        name = "typedb_behaviour",
        path = "../typedb-behaviour",
    )
    git_repository(
        name = "typedb_behaviour_",
        remote = "https://github.com/krishnangovindraj/typedb-behaviour",
        commit = "6e873ffed35134b281fbbfb2f6d16876128d4004",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )

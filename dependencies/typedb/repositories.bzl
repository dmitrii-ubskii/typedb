# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def typedb_bazel_distribution():
    git_repository(
        name = "typedb_bazel_distribution",
        remote = "https://github.com/typedb/bazel-distribution",
        commit = "056a8d7ede9b552d23dcfdc2d47b9395510652f4",
    )

def typedb_dependencies():
    # TODO: Return typedb
    git_repository(
        name = "typedb_dependencies",
        remote = "https://github.com/farost/typedb-dependencies",
        commit = "a86993d232a9f050d9fa63b93a1164f4ea31d549",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )
#    git_repository(
#        name = "typedb_dependencies",
#        remote = "https://github.com/typedb/typedb-dependencies",
#        commit = "3364a72f9ba384865e3b6ac950012d427726c8f0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
#    )

def typeql():
    # TODO: Return typedb
    git_repository(
        name = "typeql",
        remote = "https://github.com/farost/typeql",
        commit = "00504c09beb591b0d1c27633c3fe29520691489b",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typeql
    )
#    git_repository(
#        name = "typeql",
#        remote = "https://github.com/typedb/typeql",
#        commit = "c3651b6d3b82f6dc8a0db499592cb22e60e79b49"
#    )

def typedb_protocol():
    git_repository(
        name = "typedb_protocol",
        remote = "https://github.com/farost/typedb-protocol",
        commit = "7df6a6bfbbbd29940558ae7940b2065e1e2ba0b1",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )
#    git_repository(
#        name = "typedb_protocol",
#        remote = "https://github.com/typedb/typedb-protocol",
#        tag = "3.0.0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
#    )

def typedb_behaviour():
    # TODO: Return typedb
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/farost/typedb-behaviour",
        commit = "9f5b1d33a1d3b64b0a57f17c2bae9d16fc0cd574",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
#    git_repository(
#        name = "typedb_behaviour",
#        remote = "https://github.com/typedb/typedb-behaviour",
#        commit = "a5ca738d691e7e7abec0a69e68f6b06310ac2168",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
#    )

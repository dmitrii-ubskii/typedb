# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  # TODO: Remove this

  Background:
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection has 0 databases

  Scenario: one database, one <type> transaction
    Given connection opens with default authentication
#    When connection create database: typedb
#    Given connection open <type> transaction for database: typedb
#    Then transaction is open: true
#    Then transaction has type: <type>
#    Examples:
#      | type   |
#      | read   |
#      | write  |
#      | schema |

  Scenario: one database, one committed <type> transaction is closed
    Given connection opens with default authentication
#    When connection create database: typedb
#    Given connection open <type> transaction for database: typedb
#    Then transaction commits
#    Then transaction is open: false
#    Examples:
#      | type   |
#      | write  |
#      | schema |

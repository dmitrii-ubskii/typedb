# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  # TODO: Remove this

  Background: Open connection / create driver, create database
    Given typedb starts
    Given connection is open: false
    Given connection opens with default authentication
    Given connection is open: true
    Given connection has 0 databases
    Given connection create database: typedb
    Given connection has database: typedb



  Scenario: Driver can open a schema transaction when a parallel schema lock is released
    When set transaction option transaction_timeout_millis to: 50000
    When set transaction option schema_lock_acquire_timeout_millis to: 1000
    When in background, connection open schema transaction for database: typedb
    Then transaction is open: false
    Then connection open schema transaction for database: typedb; fails with a message containing: "timeout"
    Then transaction is open: false
    Then typeql schema query; fails with a message containing: "no open transaction"
      """
      match entity $x;
      """
    When wait 5 seconds
    When set transaction option transaction_timeout_millis to: 1000
    When set transaction option schema_lock_acquire_timeout_millis to: 5000
    When in background, connection open schema transaction for database: typedb
    Then transaction is open: false
    When connection open schema transaction for database: typedb
    Then transaction is open: true
    Then transaction has type: schema
    Then typeql schema query
      """
      match entity $x;
      """

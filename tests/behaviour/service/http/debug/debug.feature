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



  Scenario: Driver processes ok query answers correctly
    Given connection open schema transaction for database: typedb
    When get answers of typeql schema query
      """
      define entity person, owns name; attribute name, value string;
      """
    When get answers of typeql write query
      """
      insert $p isa person, has $j; $j isa name "John";
      """
    When get answers of typeql read query
      """
      match $p sub person;
      """
#    Then answer type is: ok
#    Then answer type is not: concept rows
#    Then answer type is not: concept documents
#    Then answer unwraps as ok
#    Then transaction commits
#


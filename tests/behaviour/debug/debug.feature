# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

#noinspection CucumberUndefinedStep
Feature: TypeQL Match Clause

  Background: Open connection and create a simple extensible schema
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true

  Scenario: Simple query
    Given connection open read transaction for database: typedb
    When get answers of typeql read query
      """
      match entity $x;
      """
    Then uniquely identify answer concepts
      | x            |
      | label:WAREHOUSE |
      | label:ITEM |

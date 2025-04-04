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



  Scenario Outline: Driver processes attributes of type <value-type> correctly
    Given connection open schema transaction for database: typedb
    Given typeql schema query
      """
      define entity person, owns typed; attribute typed, value <value-type>;
      """
    Given transaction commits
    Given connection open write transaction for database: typedb
    Given typeql write query
      """
      insert $p isa person, has typed <value>;
      """
    When get answers of typeql read query
      """
      match $_ isa person, has $a;
      """
    Then answer type is: concept rows
    Then answer query type is: read
    Then answer size is: 1

    Then answer get row(0) get variable(a) is type: false
    Then answer get row(0) get variable(a) is instance: true
    Then answer get row(0) get variable(a) is value: false
    Then answer get row(0) get variable(a) is entity type: false
    Then answer get row(0) get variable(a) is relation type: false
    Then answer get row(0) get variable(a) is attribute type: false
    Then answer get row(0) get variable(a) is role type: false
    Then answer get row(0) get variable(a) is entity: false
    Then answer get row(0) get variable(a) is relation: false
    Then answer get row(0) get variable(a) is attribute: true

    Then answer get row(0) get variable(a) as attribute
    Then answer get row(0) get variable(a) get label: typed
    Then answer get row(0) get instance(a) get label: typed
    Then answer get row(0) get instance(a) get type get label: typed
    Then answer get row(0) get attribute(a) get label: typed
    Then answer get row(0) get attribute(a) get type get label: typed
    Then answer get row(0) get attribute(a) get type is attribute: false
    Then answer get row(0) get attribute(a) get type is entity type: false
    Then answer get row(0) get attribute(a) get type is relation type: false
    Then answer get row(0) get attribute(a) get type is attribute type: true
    Then answer get row(0) get attribute(a) get type is role type: false

    Then answer get row(0) get attribute(a) get type get value type: <value-type>
    Then answer get row(0) get attribute(a) get <value-type>
    Then answer get row(0) get attribute(a) is boolean: <is-boolean>
    Then answer get row(0) get attribute(a) is integer: <is-integer>
    Then answer get row(0) get attribute(a) is double: <is-double>
    Then answer get row(0) get attribute(a) is decimal: <is-decimal>
    Then answer get row(0) get attribute(a) is string: <is-string>
    Then answer get row(0) get attribute(a) is date: <is-date>
    Then answer get row(0) get attribute(a) is datetime: <is-datetime>
    Then answer get row(0) get attribute(a) is datetime-tz: <is-datetime-tz>
    Then answer get row(0) get attribute(a) is duration: <is-duration>
    Then answer get row(0) get attribute(a) is struct: false
    Then answer get row(0) get attribute(a) try get value is: <value>
    Then answer get row(0) get attribute(a) try get <value-type> is: <value>
    Then answer get row(0) get attribute(a) try get value is not: <not-value>
    Then answer get row(0) get attribute(a) try get <value-type> is not: <not-value>
    Then answer get row(0) get attribute(a) get value is: <value>
    Then answer get row(0) get attribute(a) get <value-type> is: <value>
    Then answer get row(0) get attribute(a) get value is not: <not-value>
    Then answer get row(0) get attribute(a) get <value-type> is not: <not-value>
    Examples:
      | value-type  | value                                        | not-value                            | is-boolean | is-integer | is-double | is-decimal | is-string | is-date | is-datetime | is-datetime-tz | is-duration |
#      | boolean     | true                                         | false                                | true       | false      | false     | false      | false     | false   | false       | false          | false       |
#      | integer     | 12345090                                     | 0                                    | false      | true       | false     | false      | false     | false   | false       | false          | false       |
#      | double      | 0.0000000000000000001                        | 0.000000000000000001                 | false      | false      | true      | false      | false     | false   | false       | false          | false       |
#      | double      | 2.01234567                                   | 2.01234568                           | false      | false      | true      | false      | false     | false   | false       | false          | false       |
      | decimal     | 1234567890.0001234567890dec                  | 1234567890.001234567890dec           | false      | false      | false     | true       | false     | false   | false       | false          | false       |
#      | decimal     | 0.0000000000000000001dec                     | 0.000000000000000001dec              | false      | false      | false     | true       | false     | false   | false       | false          | false       |
#      | string      | "John \"Baba Yaga\" Wick"                    | "John Baba Yaga Wick"                | false      | false      | false     | false      | true      | false   | false       | false          | false       |
#      | date        | 2024-09-20                                   | 2025-09-20                           | false      | false      | false     | false      | false     | true    | false       | false          | false       |
#      | datetime    | 1999-02-26T12:15:05                          | 1999-02-26T12:15:00                  | false      | false      | false     | false      | false     | false   | true        | false          | false       |
#      | datetime    | 1999-02-26T12:15:05.000000001                | 1999-02-26T12:15:05.00000001         | false      | false      | false     | false      | false     | false   | true        | false          | false       |
#      | datetime-tz | 2024-09-20T16:40:05 America/New_York         | 2024-06-20T15:40:05 America/New_York | false      | false      | false     | false      | false     | false   | false       | true           | false       |
#      | datetime-tz | 2024-09-20T16:40:05.000000001 Europe/London  | 2024-09-20T16:40:05.000000001 UTC    | false      | false      | false     | false      | false     | false   | false       | true           | false       |
#      | datetime-tz | 2024-09-20T16:40:05.000000001 Europe/Belfast | 2024-09-20T16:40:05 Europe/Belfast   | false      | false      | false     | false      | false     | false   | false       | true           | false       |
#      | datetime-tz | 2024-09-20T16:40:05.000000001+0100           | 2024-09-20T16:40:05.000000001-0100   | false      | false      | false     | false      | false     | false   | false       | true           | false       |
#      | datetime-tz | 2024-09-20T16:40:05.000000001+1115           | 2024-09-20T16:40:05.000000001+0000   | false      | false      | false     | false      | false     | false   | false       | true           | false       |
#      | datetime-tz | 2024-09-20T16:40:05.000000001+0000           | 2024-09-20T16:40:05+0000             | false      | false      | false     | false      | false     | false   | false       | true           | false       |
#      | duration    | P1Y10M7DT15H44M5.00394892S                   | P1Y10M7DT15H44M5.0394892S            | false      | false      | false     | false      | false     | false   | false       | false          | true        |
#      | duration    | P66W                                         | P67W                                 | false      | false      | false     | false      | false     | false   | false       | false          | true        |

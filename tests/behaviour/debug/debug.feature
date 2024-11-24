# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  Background: Set up database
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection has 0 databases
    Given connection create database: typedb

    Given connection open schema transaction for database: typedb
    Given typeql schema query
      """
      define

      entity person, owns name, owns ref @key;
      attribute ref value long;
      attribute name value string;
      """
    Given transaction commits



  Scenario: Functions can return single instances.
    Given connection open schema transaction for database: typedb
    Given typeql write query
    """
    insert
    $p1 isa person, has ref 0, has name "Alice";
    $p2 isa person, has ref 1, has name "Bob";
    """
    Given transaction commits

    Given connection open schema transaction for database: typedb
    Given typeql schema query
    """
    define
    fun person_of_name($name: name) -> person:
    match
      $p isa person, has name $name;
    return first $p;

    fun name_value_of_person($p: person) -> string:
    match
      $p isa person, has name $name_attr;
      $name_value = $name_attr;
    return first $name_value;

    """
    Given transaction commits
    When connection open read transaction for database: typedb
    When get answers of typeql read query
    """
    match
      $name "Bob" isa name;
      $person = person_of_name($name);
    """
    Then uniquely identify answer concepts
      | person    | name            |
      | key:ref:1 | attr:name:Bob   |
    Given transaction closes

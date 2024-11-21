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
    entity person, owns name;
    attribute name, value string;
    """
    Given transaction commits


  Scenario: If a modification of a function causes a caller function to become invalid, the modification is blocked.
    Given connection open schema transaction for database: typedb
    Given typeql schema query
    """
    define
    attribute nickname, value string;
    person owns nickname;
    """
    Given transaction commits

    Given connection open schema transaction for database: typedb
    Given typeql schema query
    """
    define
    fun nickname_of($p: person) -> { nickname }:
    match
      $nickname in default_nickname($p);
    return { $nickname };

    fun default_nickname($p: person) -> { nickname }:
    match
      $nickname "Steve" isa nickname;
    return { $nickname };
    """
    Given transaction commits
    Given connection open schema transaction for database: typedb
    Given typeql schema query
    """
    define
    fun default_nickname($p: person) -> { string }:
    match
      $nickname_attr "Steve" isa nickname;
      $nickname_value = $nickname_attr;
    return { $nickname_value };
    """
    Then transaction commits; fails with a message containing: "TODO: Add message when we support redefine"

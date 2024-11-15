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
      entity person
        plays friendship:friend,
        plays employment:employee,
        owns name @card(0..),
        owns age @card(0..),
        owns ref @key;
      entity company
        plays employment:employer,
        owns name @card(0..),
        owns ref @key;
      relation friendship
        relates friend @card(0..),
        owns ref @key;
      relation employment
        relates employee @card(0..),
        relates employer @card(0..),
        owns ref @key;
      attribute name value string;
      attribute age @independent, value long;
      attribute ref value long;
      attribute email value string;
      """
    Given transaction commits

    Given connection open write transaction for database: typedb
    Given typeql write query
      """
      insert
      $p1 isa person, has name "Alice", has name "Allie", has age 10, has ref 0;
      $p2 isa person, has name "Bob", has ref 1;
      $p3 isa person, has name "Charlie", has ref 2;
      """
    Given transaction commits


  Scenario: a function with negated disjunctions considers every branch
    Given connection open schema transaction for database: typedb
    Given typeql schema query
      """
      define
      fun not_alice_or_bob() -> { person }:
      match
        $p isa person;
        not { { $p has name "Alice"; } or { $p has name "Bob"; }; };
      return { $p };
      """
    Given transaction commits

    Given connection open read transaction for database: typedb
    Given get answers of typeql read query
    """
    match $p in not_alice_or_bob();
    """
    Then uniquely identify answer concepts
      | p         |
      | key:ref:2 |

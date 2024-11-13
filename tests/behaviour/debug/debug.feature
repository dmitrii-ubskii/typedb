# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.


  Background: Debug
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection open write transaction for database: tpcc

  Scenario: Debug scenario
    Given typeql write query
      """
  match
  $d isa DISTRICT, has D_ID 11;
  $o links (district: $d), isa ORDER, has O_ID 1, has O_NEW_ORDER $status;
  delete $status of $order;
  insert $o has O_NEW_ORDER true;
      """
    Given transaction commits


# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debug

  Background: Open connection
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true


  ##################
  # SCHEMA QUERIES #
  ##################

  Scenario: Debug
    Given connection open write transaction for database: tpcc
    Given typeql write query
      """
 match
  $c isa CUSTOMER, has C_ID 60001, has C_BALANCE $c_balance;
  $c_balance_new = $c_balance + 8;
  $o links (customer: $c), isa ORDER, has O_ID 8, has O_NEW_ORDER $o_new_order, has O_CARRIER_ID $o_carrier_id;
  delete
  $o_new_order of $o;
  $o_carrier_id of $o;
  $c_balance of $c;
  insert
  $o has O_NEW_ORDER false, has O_CARRIER_ID 1;
  $c has C_BALANCE == $c_balance_new;
  select $o;
  match
  $ol links  (order: $o), isa ORDER_LINE;
  insert
  $ol has OL_DELIVERY_D 2024-11-12T17:03:47.522;
  """
    Given transaction commits

  Scenario: Debug scenario
    Given connection open write transaction for database: tpcc
    Given typeql write query
      """
  match
  $d isa DISTRICT, has D_ID 11;
  $o links (district: $d), isa ORDER, has O_ID 1, has O_NEW_ORDER $status;
  delete $status of $order;
  insert $o has O_NEW_ORDER true;
      """
    Given transaction commits
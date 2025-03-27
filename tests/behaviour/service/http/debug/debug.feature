# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  # TODO: Remove this

  Scenario: User's name is retrievable only by admin
    Given typedb starts
    Given connection opens with username 'admin', password 'password'
    When create user with username 'user', password 'password'
    When create user with username 'user2', password 'password'
    Then get user(user) get name: user
    Then get user(user2) get name: user2
    Then get user(admin) get name: admin
    When connection closes

    When connection opens with username 'user', password 'password'
    Then get user(user) get name: user
    Then get user: user2; fails with a message containing: "The user is not permitted to execute the operation"
    Then get user: admin; fails with a message containing: "The user is not permitted to execute the operation"
@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM needs to be called such that software installed
REM by Chocolatey in prepare.bat is accessible
CALL refreshenv

REM Build the server binary with Cargo
cargo build --profile=release --features server/published
if errorlevel 1 exit /b 1
copy target\release\typedb_server_bin.exe .\

REM Assemble the distribution zip and deploy to Cloudsmith.
REM See deploy_snapshot.bat for rationale on bypassing Bazel for Windows.
SET DEPLOY_ARTIFACT_USERNAME=%REPO_TYPEDB_USERNAME%
SET DEPLOY_ARTIFACT_PASSWORD=%REPO_TYPEDB_PASSWORD%
python .circleci\windows\assemble_and_deploy.py release
if errorlevel 1 exit /b 1


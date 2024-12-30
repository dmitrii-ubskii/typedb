@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM needs to be called such that software installed
REM by Chocolatey in prepare.bat is accessible
CALL refreshenv

git rev-parse HEAD > version_snapshot.txt
set /p VER=<version_snapshot.txt

REM build typedb-all-windows archive
echo "ADD: --compilation_mode=opt"
@REM bazel --output_user_root=C:/bzl run --experimental_ui_max_stdouterr_bytes=-1 --enable_runfiles --jobs=8 --define version=%VER% //:deploy-typedb-server -- snapshot
bazel --output_user_root=C:/bzl build --jobs=8 --define version=%VER% //:assemble-all-windows-x86_64-zip
:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%

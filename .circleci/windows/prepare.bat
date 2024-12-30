@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM uninstall Java 12 installed by CircleCI
choco uninstall openjdk --limit-output --yes --no-progress

REM install dependencies needed for build
choco install .circleci\windows\dependencies.config  --limit-output --yes --no-progress --allow-downgrade

REM create a symlink python3.exe and make it available in %PATH%
mkdir python37
curl -L https://www.python.org/ftp/python/3.7.8/python-3.7.8-embed-amd64.zip --output python37.zip
tar -xf python37.zip -C python37

REM install runtime dependency for the build
python37\python.exe -m pip install wheel

REM permanently set variables for Bazel build
@REM SETX BAZEL_SH "C:\Program Files\Git\usr\bin\bash.exe"
SETX BAZEL_PYTHON %cd%\python37\python3.exe
@REM TODO: Update?
@REM SETX BAZEL_VC "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC"

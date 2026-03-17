#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Assemble the Windows distribution zip and deploy it to Cloudsmith.

This script replaces the Bazel-based assembly and deploy for Windows builds.
Bazel's cargo_build_script_runner creates symlinks with mixed path separators
on Windows, which causes MSVC cl.exe to fail when resolving relative #include
paths in native crate build scripts (e.g. ring, librocksdb-sys). Since the
Rust binary is already built by Cargo, we assemble the zip and deploy directly.
"""

import os
import re
import sys
import json
import time
import zipfile
import urllib.request
import urllib.error

ARTIFACT_GROUP = "typedb-all-windows-x86_64"
ARTIFACT_NAME_TEMPLATE = "typedb-all-windows-x86_64-{version}.zip"
SNAPSHOT_URL = "cloudsmith://typedb/public-snapshot"
RELEASE_URL = "cloudsmith://typedb/public-release"


def assemble_zip(output_path, server_binary, console_artifact, typedb_bat, config_yml, license_file):
    """Create the distribution zip with the expected directory layout."""
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        base = "typedb-all-windows-x86_64"
        # Server binary
        zf.write(server_binary, os.path.join(base, "server", "typedb_server_bin.exe"))
        # Launcher script
        zf.write(typedb_bat, os.path.join(base, "typedb.bat"))
        # Config
        zf.write(config_yml, os.path.join(base, "server", "config.yml"))
        # License
        zf.write(license_file, os.path.join(base, "LICENSE"))
        # Empty data directory (add a placeholder)
        zf.mkdir(os.path.join(base, "server", "data"))
        # Console artifact (nested zip/archive)
        if console_artifact and os.path.exists(console_artifact):
            # Extract console into the zip
            import tempfile
            with tempfile.TemporaryDirectory() as tmp:
                with zipfile.ZipFile(console_artifact, 'r') as cz:
                    cz.extractall(tmp)
                # Find the extracted contents and add to our zip
                for root, dirs, files in os.walk(tmp):
                    for f in files:
                        full = os.path.join(root, f)
                        arcname = os.path.join(base, os.path.relpath(full, tmp))
                        zf.write(full, arcname)
    print("Assembled: %s" % output_path)


def cloudsmith_upload(username, password, repo_url, artifact_group, version, artifact_path, filename):
    """Upload artifact to Cloudsmith using their API (no external dependencies)."""
    res = re.search(r"cloudsmith://([^/]+)/([^/]+)/?", repo_url)
    if res is None:
        raise ValueError("Invalid cloudsmith URL: %s" % repo_url)
    repo_owner = res.group(1)
    repo = res.group(2)

    import base64
    auth_header = "Basic " + base64.b64encode(("%s:%s" % (username, password)).encode()).decode()
    headers_common = {"Authorization": auth_header}

    # Step 1: Upload file
    print("Uploading file: %s" % filename)
    upload_url = "https://upload.cloudsmith.io/%s/%s/%s" % (repo_owner, repo, filename)
    with open(artifact_path, 'rb') as f:
        data = f.read()
    req = urllib.request.Request(upload_url, data=data, method='PUT', headers=headers_common)
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        raise RuntimeError("Upload failed [%d]: %s" % (e.code, e.read().decode()))
    resp_json = json.loads(resp.read().decode())
    uploaded_id = resp_json["identifier"]
    print("- Upload success (id: %s)" % uploaded_id)

    # Step 2: Create package metadata
    print("Creating package metadata...")
    metadata_url = "https://api-prd.cloudsmith.io/v1/packages/%s/%s/upload/raw/" % (repo_owner, repo)
    metadata = json.dumps({
        "package_file": uploaded_id,
        "name": artifact_group,
        "version": version,
    }).encode()
    headers_json = dict(headers_common)
    headers_json["Content-Type"] = "application/json"
    req = urllib.request.Request(metadata_url, data=metadata, method='POST', headers=headers_json)
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        raise RuntimeError("Metadata post failed [%d]: %s" % (e.code, e.read().decode()))
    slug = json.loads(resp.read().decode())["slug_perm"]
    print("- Metadata success (slug: %s)" % slug)

    # Step 3: Wait for sync
    print("Waiting for sync...")
    sync_url = "https://api.cloudsmith.io/v1/packages/%s/%s/%s/status/" % (repo_owner, repo, slug)
    for attempt in range(100):
        req = urllib.request.Request(sync_url, headers=headers_common)
        resp = urllib.request.urlopen(req)
        status = json.loads(resp.read().decode())
        if status.get("is_sync_completed") or status.get("is_sync_failed"):
            if status.get("is_sync_completed"):
                print("- Sync complete!")
                return True
            else:
                raise RuntimeError("Sync failed for slug: %s" % slug)
        if not status.get("is_sync_in_progress", True):
            break
        time.sleep(3)
    raise RuntimeError("Sync timed out after 100 attempts")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("snapshot", "release"):
        print("Usage: assemble_and_deploy.py <snapshot|release>", file=sys.stderr)
        sys.exit(1)

    repo_type = sys.argv[1]
    username = os.environ.get("DEPLOY_ARTIFACT_USERNAME")
    password = os.environ.get("DEPLOY_ARTIFACT_PASSWORD")
    if not username or not password:
        raise ValueError("DEPLOY_ARTIFACT_USERNAME and DEPLOY_ARTIFACT_PASSWORD must be set")

    # Determine version
    if repo_type == "snapshot":
        import subprocess
        version = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    else:
        with open("VERSION") as f:
            version = f.read().strip()

    print("Version: %s" % version)
    print("Repo type: %s" % repo_type)

    # Paths
    server_binary = "typedb_server_bin.exe"
    typedb_bat = os.path.join("binary", "typedb.bat")
    config_yml = os.path.join("server", "config.yml")
    license_file = "LICENSE"

    if not os.path.exists(server_binary):
        raise FileNotFoundError("Server binary not found: %s" % server_binary)

    # Assemble zip (without console for now - console is optional)
    artifact_name = ARTIFACT_NAME_TEMPLATE.format(version=version)
    assemble_zip(artifact_name, server_binary, None, typedb_bat, config_yml, license_file)

    # Deploy
    repo_url = SNAPSHOT_URL if repo_type == "snapshot" else RELEASE_URL
    cloudsmith_upload(username, password, repo_url, ARTIFACT_GROUP, version, artifact_name, artifact_name)

    print("Deploy complete!")


if __name__ == "__main__":
    main()

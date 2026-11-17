#!/usr/bin/env python3
#
# Copyright © 2026 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Checks that SECURITY.md covers the releases.

The release line to check depends on what is checked out:
  --branch main          the latest release line, from the X.Y.Z tags of the remote
  --branch release-X.Y   the X.Y line
  --tag X.Y.Z            the X.Y line
An older line is only checked while SECURITY.md lists it as supported.

SECURITY.md must list the release line as supported, and "< oldest supported version" as unsupported.

Run from the repository root.
"""

import argparse
import os
import re
import subprocess
import sys

SECURITY = "SECURITY.md"

errors = []


def error(message):
    errors.append(message)


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def to_line(match):
    """The release line (X, Y) of a regex match with X and Y as its first two groups."""
    return int(match[1]), int(match[2])


def name(line):
    return f"{line[0]}.{line[1]}"


def release_lines(remote):
    """The release lines that have an X.Y.Z tag on the remote, e.g. {(0, 11), (0, 12)}."""
    lines = set()
    for ref in git("ls-remote", "--tags", "--refs", remote).split():
        match = re.fullmatch(r"refs/tags/(\d+)\.(\d+)\.\d+", ref)
        if match:
            lines.add(to_line(match))
    return lines


def markdown_table(text):
    """The rows of the first Markdown table in the text, as dicts keyed by the header cells."""
    table_lines = []
    for text_line in text.splitlines():
        if text_line.lstrip().startswith("|"):
            table_lines.append(text_line)
        elif table_lines:
            break
    header, _separator, *rows = [[cell.strip() for cell in line.strip().strip("|").split("|")] for line in table_lines]
    return [dict(zip(header, row)) for row in rows]


def check_security(line, supported_versions, unsupported_versions, supported):
    for version in supported_versions:
        if not re.fullmatch(r"\d+\.\d+\.x", version):
            error(f"must list supported versions as X.Y.x, found '{version}'")
    if line not in supported:
        error(f"must list {name(line)}.x as supported")
    if supported and f"< {name(min(supported))}" not in unsupported_versions:
        error(f"must list < {name(min(supported))} as unsupported, {name(min(supported))}.x is the oldest supported version")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    checked_out = parser.add_mutually_exclusive_group(required=True)
    checked_out.add_argument("--branch", help="the checked-out branch: main or release-X.Y")
    checked_out.add_argument("--tag", help="the checked-out tag")
    parser.add_argument("--remote", default="origin", help="the remote to read the release tags from")
    args = parser.parse_args()

    lines = release_lines(args.remote)
    if not lines:
        sys.exit(f"No X.Y.Z release tags found on {args.remote}")
    latest = max(lines)

    # The release line to check
    if args.tag:
        match = re.fullmatch(r"(\d+)\.(\d+)\.\d+", args.tag)
        if not match:
            print(f"{args.tag} is not a release tag, nothing to check")
            return 0
        line = to_line(match)
    elif match := re.fullmatch(r"release-(\d+)\.(\d+)", args.branch):
        line = to_line(match)
    else:
        line = latest

    security = markdown_table(open(SECURITY, encoding="utf-8").read()) if os.path.exists(SECURITY) else []
    supported_versions = [row["Version"] for row in security if ":white_check_mark:" in row["Supported"]]
    unsupported_versions = [row["Version"] for row in security if ":x:" in row["Supported"]]
    supported = []
    for version in supported_versions:
        if match := re.fullmatch(r"(\d+)\.(\d+)\.x", version):
            supported.append(to_line(match))

    if line < latest and line not in supported:
        print(f"{name(line)} is neither the latest release line ({name(latest)}) nor supported, nothing to check")
        return 0

    print(f"Checking {SECURITY} for the {name(line)} release line")
    check_security(line, supported_versions, unsupported_versions, supported)

    for message in errors:
        print(f"::error file={SECURITY}::{message}" if os.environ.get("GITHUB_ACTIONS") else f"ERROR {SECURITY}: {message}")
    if errors:
        print(f"{len(errors)} problem(s) found, see RELEASE.md")
        return 1
    print("SECURITY.md is up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())

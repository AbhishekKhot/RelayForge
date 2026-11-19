"""Run the full PR acceptance suite when executable or configuration files change."""

import argparse
import os
import re
import subprocess
from pathlib import Path


SAFE_PREFIXES = ("docs/", "load-tests/")
SAFE_FILES = {"README.md"}


def requires_full_acceptance(paths):
    return not paths or any(path not in SAFE_FILES
                            and not path.startswith(SAFE_PREFIXES) for path in paths)


def changed_paths(base_sha, head_sha):
    for value in (base_sha, head_sha):
        if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
            raise ValueError("Expected a 40-character commit SHA")
    result = subprocess.run(["git", "diff", "--no-renames", "--name-only", "-z",
                             base_sha, head_sha],
                            capture_output=True, check=True)
    return [os.fsdecode(path) for path in result.stdout.split(b"\0") if path]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_sha")
    parser.add_argument("head_sha")
    args = parser.parse_args()
    paths = changed_paths(args.base_sha, args.head_sha)
    required = requires_full_acceptance(paths)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as stream:
            stream.write(f"full_acceptance={str(required).lower()}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary and not required:
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write("Full Docker acceptance skipped: this PR changes only "
                         "documentation or load-test tools. Generator and security checks still run.\n")
    print("Full Docker acceptance required" if required
          else "Full Docker acceptance skipped: documentation/load-test-only change")


if __name__ == "__main__":
    main()

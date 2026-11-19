"""Guard the PR paths that may skip the Docker acceptance suite."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ci_pr_scope import requires_full_acceptance


class PullRequestScopeTest(unittest.TestCase):
    def test_only_documentation_and_load_tools_skip_full_acceptance(self):
        for paths in (["README.md"], ["docs/benchmarks/j3-plan.md"],
                      ["load-tests/setup.mjs", "load-tests/load.test.mjs"]):
            with self.subTest(paths=paths):
                self.assertFalse(requires_full_acceptance(paths))

    def test_runtime_configuration_workflow_and_mixed_changes_run_full_acceptance(self):
        for paths in (["relayforge-ingestion/src/main/java/Event.java"],
                      ["deploy/ingestion-ha/compose.yaml"], ["compose.yaml"],
                      [".env.example"], [".github/workflows/bootstrap.yml"],
                      ["scripts/verify-j1.py"], ["load-tests/setup.mjs", "pom.xml"]):
            with self.subTest(paths=paths):
                self.assertTrue(requires_full_acceptance(paths))

    def test_empty_diff_runs_full_acceptance(self):
        self.assertTrue(requires_full_acceptance([]))

    def test_moving_runtime_file_into_docs_still_runs_full_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory)

            def git(*args):
                return subprocess.run(["git", *args], cwd=repository, check=True,
                                      capture_output=True, text=True).stdout.strip()

            git("init", "-q")
            (repository / "runtime.java").write_text("class Runtime {}\n")
            git("add", "runtime.java")
            commit = ("-c", "user.name=CI", "-c", "user.email=ci@example.test",
                      "-c", "commit.gpgsign=false", "commit", "-qm")
            git(*commit, "initial")
            base = git("rev-parse", "HEAD")
            (repository / "docs").mkdir()
            (repository / "runtime.java").rename(repository / "docs" / "runtime.java")
            git("add", "-A")
            git(*commit, "move")
            head = git("rev-parse", "HEAD")
            output = repository / "scope.output"
            subprocess.run([sys.executable, str(Path(__file__).with_name("ci_pr_scope.py")),
                            base, head], cwd=repository, check=True, capture_output=True,
                           env={**os.environ, "GITHUB_OUTPUT": str(output)})
            self.assertEqual(output.read_text(), "full_acceptance=true\n")


if __name__ == "__main__":
    unittest.main()

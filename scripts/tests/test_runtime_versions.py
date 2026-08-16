from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts import runtime_versions


def multica_release(version: str) -> dict[str, object]:
    return {
        "draft": False,
        "prerelease": False,
        "tag_name": f"v{version}",
        "assets": [
            {
                "name": f"multica-cli-{version}-linux-amd64.tar.gz",
                "size": 1,
                "state": "uploaded",
            },
            {
                "name": f"multica-cli-{version}-linux-arm64.tar.gz",
                "size": 1,
                "state": "uploaded",
            },
            {"name": "checksums.txt", "size": 1, "state": "uploaded"},
        ],
    }


class RuntimeVersionTests(unittest.TestCase):
    def test_cli_requires_an_explicit_absolute_target_root(self) -> None:
        with self.assertRaises(runtime_versions.ResolverError):
            runtime_versions.activate_target_root(Path("relative"))

    def test_no_cli_update_does_not_bump_release_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = root / "runtime-versions.env"
            version = root / "VERSION"
            env.write_text("MULTICA_CLI_VERSION=0.4.26\n", encoding="utf-8")
            version.write_text("0.3.21\n", encoding="ascii")

            result = runtime_versions.update_files(
                env,
                version,
                runtime_versions.fixture_fetcher(
                    {"multica": multica_release("0.4.26")}
                ),
            )

            self.assertEqual(result["updates"], [])
            self.assertIsNone(result["release_version"])
            self.assertEqual(version.read_text(encoding="ascii"), "0.3.21\n")

    def test_cli_update_and_release_patch_move_together(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = root / "runtime-versions.env"
            version = root / "VERSION"
            env.write_text("MULTICA_CLI_VERSION=0.4.26\n", encoding="utf-8")
            version.write_text("0.3.21\n", encoding="ascii")

            result = runtime_versions.update_files(
                env,
                version,
                runtime_versions.fixture_fetcher(
                    {"multica": multica_release("0.4.27")}
                ),
            )

            self.assertEqual(
                result["updates"],
                [{"field": "MULTICA_CLI_VERSION", "from": "0.4.26", "to": "0.4.27"}],
            )
            self.assertEqual(
                result["release_version"], {"from": "0.3.21", "to": "0.3.22"}
            )
            self.assertEqual(env.read_text(encoding="utf-8"), "MULTICA_CLI_VERSION=0.4.27\n")
            self.assertEqual(version.read_text(encoding="ascii"), "0.3.22\n")

    def test_build_args_include_release_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = root / "runtime-versions.env"
            version = root / "VERSION"
            env.write_text("MULTICA_CLI_VERSION=0.4.26\n", encoding="utf-8")
            version.write_text("0.3.21\n", encoding="ascii")
            revision = "a" * 40

            result = runtime_versions.build_args(env, version, revision)

            self.assertEqual(result["MULTICA_CLI_VERSION"], "0.4.26")
            self.assertEqual(result["VERSION"], "0.3.21")
            self.assertEqual(result["COMMIT"], revision)

    def test_automation_diff_requires_env_and_version_together(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", "-b", "develop"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.name", "test"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=root, check=True)
            (root / "build").mkdir()
            (root / "build/runtime-versions.env").write_text(
                "MULTICA_CLI_VERSION=0.4.26\n", encoding="utf-8"
            )
            (root / "VERSION").write_text("0.3.21\n", encoding="ascii")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(["git", "commit", "-qm", "base"], cwd=root, check=True)
            (root / "build/runtime-versions.env").write_text(
                "MULTICA_CLI_VERSION=0.4.27\n", encoding="utf-8"
            )
            (root / "VERSION").write_text("0.3.22\n", encoding="ascii")

            previous = runtime_versions.PROJECT_ROOT
            try:
                runtime_versions.activate_target_root(root.resolve())
                result = runtime_versions.validate_repository("HEAD", automation_diff=True)
            finally:
                runtime_versions.activate_target_root(previous)

            self.assertEqual(result["changed_fields"], ["MULTICA_CLI_VERSION"])
            self.assertEqual(result["release_version"], {"from": "0.3.21", "to": "0.3.22"})

    def test_automation_diff_rejects_other_runtime_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", "-b", "develop"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.name", "test"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=root, check=True)
            (root / "build").mkdir()
            (root / "build/runtime-versions.env").write_text(
                "MULTICA_CLI_VERSION=0.4.26\nCODEX_VERSION=0.147.0\n", encoding="utf-8"
            )
            (root / "VERSION").write_text("0.3.21\n", encoding="ascii")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(["git", "commit", "-qm", "base"], cwd=root, check=True)
            (root / "build/runtime-versions.env").write_text(
                "MULTICA_CLI_VERSION=0.4.27\nCODEX_VERSION=0.148.0\n", encoding="utf-8"
            )
            (root / "VERSION").write_text("0.3.22\n", encoding="ascii")

            previous = runtime_versions.PROJECT_ROOT
            try:
                runtime_versions.activate_target_root(root.resolve())
                with self.assertRaises(runtime_versions.ResolverError):
                    runtime_versions.validate_repository("HEAD", automation_diff=True)
            finally:
                runtime_versions.activate_target_root(previous)

    def test_offline_cli_update_writes_both_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "build").mkdir()
            (root / "build/runtime-versions.env").write_text(
                "MULTICA_CLI_VERSION=0.4.26\n", encoding="utf-8"
            )
            (root / "VERSION").write_text("0.3.21\n", encoding="ascii")
            fixture = root / "fixture.json"
            fixture.write_text(
                json.dumps({"multica": multica_release("0.4.27")}), encoding="utf-8"
            )

            exit_code = runtime_versions.main(
                [
                    "--root",
                    str(root.resolve()),
                    "update",
                    "--offline-fixture",
                    str(fixture),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertEqual((root / "VERSION").read_text(encoding="ascii"), "0.3.22\n")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts import runtime_versions


TEST_SECRET_REFERENCE = "${{ secrets.TEST_TOKEN }}"


def write_action_workflows(
    directory: Path,
    *,
    release_secrets: tuple[tuple[str, str], ...] = (),
    runtime_update_secrets: tuple[tuple[str, str], ...] = (),
) -> None:
    def secret_step(name: str, secrets: tuple[tuple[str, str], ...]) -> str:
        if not secrets:
            return ""
        environment = "\n".join(f"          {key}: {value}" for key, value in secrets)
        return f"""\
      - name: {name}
        env:
{environment}
        run: printf 'exercise secret reference\n'
"""

    workflows = {
        "runtime-version-update.yml": f"""\
name: Runtime version update
on:
  schedule:
    - cron: '0 0 * * *'
  workflow_dispatch:
permissions: {{}}
jobs:
  update:
    permissions: {{}}
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo 'build/runtime-versions.env VERSION'
          echo 'gh pr create'
          echo 'pulls/$PR_NUMBER/merge'
          echo 'event_type:"create-develop-to-main-pr"'
{secret_step("Unexpected secret consumer", runtime_update_secrets)}""",
        "create-develop-to-main-pr.yml": """\
name: Create develop to main PR
on:
  push:
    branches: [develop]
  pull_request:
permissions: {}
jobs:
  create:
    permissions: {}
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo 'types: [automation-ci, create-develop-to-main-pr]'
          echo '-f name="verify"'
          echo '-f name="runtime-image"'
          echo 'git merge --no-edit origin/main'
          echo 'prepare-release --base-ref origin/main'
          echo '--base main --head develop'
          echo 'gh pr create --base main --head develop'
""",
        "release.yml": f"""\
name: Release
on:
  push:
    branches: [main]
permissions: {{}}
jobs:
  release:
    permissions: {{}}
    runs-on: ubuntu-latest
    steps:
      - run: |
          VERSION=$(cat VERSION)
          TAG="$VERSION"
          echo 'docker/build-push-action@'
          echo 'platform: linux/amd64'
          echo 'platform: linux/arm64'
          echo 'docker buildx imagetools create'
          gh release view "$TAG"
          git ls-remote --tags origin "refs/tags/$TAG"
          echo "tag=$TAG"
          gh release create "$TAG"
{secret_step("Unexpected secret consumer", release_secrets)}""",
    }
    for name, text in workflows.items():
        (directory / name).write_text(text, encoding="utf-8")


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
    def initialize_release_repository(
        self, root: Path, *, base_version: str, current_version: str | None = None
    ) -> None:
        subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.name", "test"], cwd=root, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"], cwd=root, check=True
        )
        (root / "VERSION").write_text(f"{base_version}\n", encoding="ascii")
        subprocess.run(["git", "add", "VERSION"], cwd=root, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=root, check=True)
        subprocess.run(["git", "checkout", "-qb", "develop"], cwd=root, check=True)
        if current_version is not None:
            (root / "VERSION").write_text(f"{current_version}\n", encoding="ascii")

    def test_prepare_release_version_bumps_patch_from_base(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.initialize_release_repository(root, base_version="0.3.23")

            previous = runtime_versions.PROJECT_ROOT
            try:
                runtime_versions.activate_target_root(root.resolve())
                result = runtime_versions.prepare_release_version("main")
            finally:
                runtime_versions.activate_target_root(previous)

            self.assertEqual(
                result,
                {
                    "base_ref": "main",
                    "changed": True,
                    "release_version": {"from": "0.3.23", "to": "0.3.24"},
                },
            )
            self.assertEqual((root / "VERSION").read_text(encoding="ascii"), "0.3.24\n")

    def test_prepare_release_version_is_idempotent_after_bump(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.initialize_release_repository(root, base_version="0.3.23")

            previous = runtime_versions.PROJECT_ROOT
            try:
                runtime_versions.activate_target_root(root.resolve())
                runtime_versions.prepare_release_version("main")
                result = runtime_versions.prepare_release_version("main")
            finally:
                runtime_versions.activate_target_root(previous)

            self.assertEqual(
                result,
                {
                    "base_ref": "main",
                    "changed": False,
                    "release_version": {"from": "0.3.23", "to": "0.3.24"},
                },
            )
            self.assertEqual((root / "VERSION").read_text(encoding="ascii"), "0.3.24\n")

    def test_prepare_release_version_rejects_versions_outside_allowed_transition(
        self,
    ) -> None:
        for current_version in ("0.3.22", "0.3.25"):
            with (
                self.subTest(current_version=current_version),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                self.initialize_release_repository(
                    root, base_version="0.3.23", current_version=current_version
                )

                previous = runtime_versions.PROJECT_ROOT
                try:
                    runtime_versions.activate_target_root(root.resolve())
                    with self.assertRaises(runtime_versions.ResolverError):
                        runtime_versions.prepare_release_version("main")
                finally:
                    runtime_versions.activate_target_root(previous)

                self.assertEqual(
                    (root / "VERSION").read_text(encoding="ascii"),
                    f"{current_version}\n",
                )

    def test_prepare_release_cli_writes_version_and_reports_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.initialize_release_repository(root, base_version="0.3.23")

            completed = subprocess.run(
                [
                    "python3",
                    str(Path(runtime_versions.__file__).resolve()),
                    "--root",
                    str(root.resolve()),
                    "prepare-release",
                    "--base-ref",
                    "main",
                ],
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(
                json.loads(completed.stdout),
                {
                    "base_ref": "main",
                    "changed": True,
                    "release_version": {"from": "0.3.23", "to": "0.3.24"},
                },
            )
            self.assertEqual((root / "VERSION").read_text(encoding="ascii"), "0.3.24\n")

    def test_validate_actions_accepts_secret_free_workflows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workflows = Path(directory)
            write_action_workflows(workflows)

            try:
                result = runtime_versions.validate_actions(workflows)
            except runtime_versions.ResolverError as error:
                self.fail(f"secret-free workflows should be accepted: {error}")

            self.assertEqual(result["workflows"], 3)

    def test_validate_actions_requires_unprefixed_runtime_release_tag(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workflows = Path(directory)
            write_action_workflows(workflows)
            release_workflow = workflows / "release.yml"
            release_workflow.write_text(
                release_workflow.read_text(encoding="utf-8").replace(
                    'TAG="$VERSION"', 'TAG="v$VERSION"'
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                runtime_versions.ResolverError,
                "^workflow_release_tag_must_match_version$",
            ):
                runtime_versions.validate_actions(workflows)

    def test_validate_actions_rejects_any_secret_reference(self) -> None:
        cases = (
            (
                "release workflow secret",
                (("TEST_TOKEN", TEST_SECRET_REFERENCE),),
                (),
                "release.yml",
            ),
            (
                "runtime update workflow secret",
                (),
                (("TEST_TOKEN", TEST_SECRET_REFERENCE),),
                "runtime-version-update.yml",
            ),
        )
        for name, release_secrets, runtime_update_secrets, file_name in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                workflows = Path(directory)
                write_action_workflows(
                    workflows,
                    release_secrets=release_secrets,
                    runtime_update_secrets=runtime_update_secrets,
                )

                with self.assertRaisesRegex(
                    runtime_versions.ResolverError,
                    rf"^workflow_privilege_contract_failed file={file_name}$",
                ):
                    runtime_versions.validate_actions(workflows)

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

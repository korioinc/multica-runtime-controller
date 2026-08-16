from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
from urllib import error as urlerror

from scripts import runtime_versions


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
ENV_BYTES = (ROOT / "build" / "runtime-versions.env").read_bytes()


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class RuntimeVersionTests(unittest.TestCase):
    def test_cli_uses_only_an_explicit_absolute_target_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target_root = Path(directory)
            (target_root / "build").mkdir()
            (target_root / "build" / "runtime-versions.env").write_bytes(ENV_BYTES)
            command = [
                "python3",
                "-I",
                str(ROOT / "scripts" / "runtime_versions.py"),
                "--root",
                str(target_root),
                "check",
                "--offline-fixture",
                str(FIXTURES / "upstreams-current.json"),
            ]

            completed = subprocess.run(command, check=False, text=True, capture_output=True)

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout)["updates"], [])

            command[4] = "."
            rejected = subprocess.run(command, check=False, text=True, capture_output=True)
            self.assertEqual(rejected.returncode, 1)
            self.assertIn("target_root_not_absolute", rejected.stderr)

    def test_current_sources_are_a_successful_noop(self) -> None:
        current = runtime_versions.parse_env_bytes(ENV_BYTES)
        result = runtime_versions.resolve_versions(
            current, runtime_versions.fixture_fetcher(fixture("upstreams-current.json"))
        )

        self.assertEqual(result["updates"], [])
        self.assertEqual(result["current"], result["latest"])

    def test_update_preserves_env_layout_and_never_reads_or_writes_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env_path = root / "runtime-versions.env"
            env_path.write_bytes(ENV_BYTES)
            before_lines = env_path.read_text(encoding="utf-8").splitlines()

            result = runtime_versions.update_files(
                env_path,
                runtime_versions.fixture_fetcher(fixture("upstreams-new.json")),
            )

            self.assertEqual(
                [update["field"] for update in result["updates"]],
                ["MULTICA_CLI_VERSION", "CODEX_VERSION", "PI_VERSION"],
            )
            self.assertNotIn("release", result)
            self.assertFalse((root / "VERSION").exists())
            after_lines = env_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(before_lines), len(after_lines))
            for before, after in zip(before_lines, after_lines, strict=True):
                if before.startswith(("MULTICA_CLI_VERSION=", "CODEX_VERSION=", "PI_VERSION=")):
                    continue
                self.assertEqual(after, before)
            self.assertTrue(env_path.read_bytes().endswith(b"\n"))
            self.assertEqual(env_path.stat().st_mode & 0o777, 0o644)

    def test_build_args_include_every_env_assignment_and_release_identity(self) -> None:
        args = runtime_versions.build_args(
            ROOT / "build" / "runtime-versions.env", ROOT / "VERSION", "a" * 40
        )

        self.assertEqual(args["MULTICA_CLI_VERSION"], "0.4.26")
        self.assertEqual(args["CODEX_VERSION"], "0.147.0")
        self.assertEqual(args["PI_VERSION"], "0.84.2")
        self.assertEqual(args["VERSION"], (ROOT / "VERSION").read_text().strip())
        self.assertEqual(args["COMMIT"], "a" * 40)

        explicit = runtime_versions.build_args(
            ROOT / "build" / "runtime-versions.env",
            ROOT / "missing-version-file",
            "b" * 40,
            version="0.3.21",
        )
        self.assertEqual(explicit["VERSION"], "0.3.21")
        self.assertEqual(explicit["COMMIT"], "b" * 40)
        ci = runtime_versions.build_args(
            ROOT / "build" / "runtime-versions.env",
            ROOT / "missing-version-file",
            "c" * 40,
            version="ci",
        )
        self.assertEqual(ci["VERSION"], "ci")

    def test_phase_a_legacy_build_args_emits_bounded_fallback_marker(self) -> None:
        completed = subprocess.run(
            [
                "python3",
                "-I",
                str(ROOT / "scripts" / "runtime_versions.py"),
                "--root",
                str(ROOT),
                "build-args",
                "--revision",
                "d" * 40,
            ],
            check=False,
            text=True,
            capture_output=True,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout)["VERSION"], (ROOT / "VERSION").read_text().strip())
        self.assertEqual(
            completed.stderr,
            "::warning title=Phase-A compatibility::VERSION fallback is deprecated; pass --version\n",
        )

    def test_repair_internal_canaries_are_seal_bound_and_do_not_deadlock(self) -> None:
        for name in ("release-repair.yml", "release-repair-guard.yml"):
            with self.subTest(workflow=name):
                workflow = (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
                self.assertIn("inputs.mode == 'canary'", workflow)
                self.assertIn("if [ \"$EVENT_ACTOR\" = 'github-actions[bot]' ]; then", workflow)
                self.assertIn('--arg issue "$ISSUE_NUMBER"', workflow)
                self.assertIn('issue_number:$issue', workflow)
                self.assertIn('body_hash:$body_hash', workflow)
                self.assertIn('seal_id:$seal_id', workflow)
                self.assertIn('seal_hash:$seal_hash', workflow)
                self.assertIn("Clean deterministic canary residue before retry", workflow)
                self.assertIn('"refs/heads/automation/release-repair-canary-" + $nonce', workflow)
                self.assertIn("Recognize a merged repair PR even after branch deletion", workflow)

    def test_action_validator_rejects_unexpected_workflow_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            for workflow in (ROOT / ".github" / "workflows").glob("*.yml"):
                (target / workflow.name).write_bytes(workflow.read_bytes())
            (target / "unexpected.yml").write_text(
                "name: Unexpected\non: workflow_dispatch\npermissions: {}\n"
                "jobs:\n  noop:\n    runs-on: ubuntu-latest\n    permissions: {}\n"
                "    steps:\n      - run: 'true'\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(runtime_versions.ResolverError, "unexpected_workflow"):
                runtime_versions.validate_actions(target)
            (target / "unexpected.yml").unlink()
            ci = target / "ci.yml"
            ci.write_text(
                ci.read_text(encoding="utf-8")
                + "\n  injected-write:\n    runs-on: ubuntu-latest\n"
                + "    permissions:\n      contents: write\n"
                + "    steps:\n      - run: 'true'\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                runtime_versions.ResolverError, "workflow_job_inventory_mismatch"
            ):
                runtime_versions.validate_actions(target)

    def test_release_identity_cli_proposes_patch_without_a_version_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target_root = Path(directory)
            subprocess.run(["git", "init", "-q", "-b", "main"], cwd=target_root, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"],
                cwd=target_root,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Release Test"], cwd=target_root, check=True
            )
            (target_root / "README").write_text("fixture\n", encoding="utf-8")
            subprocess.run(["git", "add", "README"], cwd=target_root, check=True)
            subprocess.run(["git", "commit", "-q", "-m", "fixture"], cwd=target_root, check=True)
            revision = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=target_root,
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            completed = subprocess.run(
                [
                    "python3",
                    "-I",
                    str(ROOT / "scripts" / "runtime_versions.py"),
                    "--root",
                    str(target_root),
                    "release-identity",
                    "--revision",
                    revision,
                    "--offline-fixture",
                    str(FIXTURES / "release-surfaces-terminal.json"),
                ],
                check=False,
                text=True,
                capture_output=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertFalse((target_root / "VERSION").exists())
            self.assertEqual(
                json.loads(completed.stdout),
                {
                    "allocation": "proposed",
                    "next_action": "build_native",
                    "revision": revision,
                    "state": "pending",
                    "target_kind": "normal",
                    "version": "0.3.21",
                },
            )


class RuntimeVersionFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.current = runtime_versions.parse_env_bytes(ENV_BYTES)

    def test_timeout_is_retried_and_then_succeeds(self) -> None:
        current_fixture = fixture("upstreams-current.json")
        calls: dict[str, int] = {}

        def fetch(source: str, attempt: int) -> dict:
            calls[source] = calls.get(source, 0) + 1
            if source == "multica" and attempt < 3:
                raise TimeoutError("fixture timeout")
            return current_fixture[source]

        result = runtime_versions.resolve_versions(self.current, fetch, sleep=lambda _: None)

        self.assertEqual(result["updates"], [])
        self.assertEqual(calls["multica"], 3)

    def test_server_error_retries_three_times_without_writing(self) -> None:
        calls = 0

        def fetch(_source: str, _attempt: int) -> dict:
            nonlocal calls
            calls += 1
            raise urlerror.HTTPError("https://example.invalid", 503, "fixture", {}, None)

        with self.assertRaisesRegex(runtime_versions.ResolverError, "http_5xx"):
            runtime_versions.resolve_versions(self.current, fetch, sleep=lambda _: None)
        self.assertEqual(calls, 3)

    def test_client_error_is_not_retried(self) -> None:
        calls = 0

        def fetch(_source: str, _attempt: int) -> dict:
            nonlocal calls
            calls += 1
            raise urlerror.HTTPError("https://example.invalid", 404, "fixture", {}, None)

        with self.assertRaisesRegex(runtime_versions.ResolverError, "http_4xx"):
            runtime_versions.resolve_versions(self.current, fetch, sleep=lambda _: None)
        self.assertEqual(calls, 1)

    def test_malformed_prerelease_downgrade_and_missing_assets_are_rejected(self) -> None:
        cases = []
        malformed = fixture("upstreams-current.json")
        malformed["pi"]["dist-tags"]["latest"] = "latest"
        cases.append(("malformed", malformed))
        prerelease = fixture("upstreams-current.json")
        prerelease["pi"]["dist-tags"]["latest"] = "0.85.0-beta.1"
        cases.append(("prerelease", prerelease))
        downgrade = fixture("upstreams-current.json")
        downgrade["pi"]["dist-tags"]["latest"] = "0.80.0"
        cases.append(("downgrade", downgrade))
        missing_asset = fixture("upstreams-current.json")
        missing_asset["multica"]["assets"] = missing_asset["multica"]["assets"][:1]
        cases.append(("missing_asset", missing_asset))
        codex_mismatch = fixture("upstreams-current.json")
        codex_mismatch["codex"]["dist-tags"]["linux-arm64"] = "0.146.0-linux-arm64"
        cases.append(("codex_mismatch", codex_mismatch))

        for name, data in cases:
            with self.subTest(name=name):
                with self.assertRaises(runtime_versions.ResolverError):
                    runtime_versions.resolve_versions(
                        self.current, runtime_versions.fixture_fetcher(data), sleep=lambda _: None
                    )

    def test_replace_failure_restores_the_original_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env_path = root / "runtime-versions.env"
            env_path.write_bytes(ENV_BYTES)
            original_env = env_path.read_bytes()

            def fail_replace(
                _source: os.PathLike[str] | str, _target: os.PathLike[str] | str
            ) -> None:
                raise OSError("injected replace failure")

            with mock.patch("scripts.runtime_versions.os.replace", side_effect=fail_replace):
                with self.assertRaisesRegex(runtime_versions.ResolverError, "atomic_write"):
                    runtime_versions.update_files(
                        env_path,
                        runtime_versions.fixture_fetcher(fixture("upstreams-new.json")),
                    )

            self.assertEqual(env_path.read_bytes(), original_env)


class ReleaseProvenanceTests(unittest.TestCase):
    revision = "a" * 40
    head = "b" * 40
    base = "c" * 40

    def pull_request(self, *, head_ref: str, body: str = "") -> dict:
        return {
            "number": 42,
            "state": "closed",
            "merged": True,
            "body": body,
            "base": {
                "ref": "main",
                "sha": self.base,
                "repo": {"full_name": runtime_versions.REPOSITORY},
            },
            "head": {
                "ref": head_ref,
                "sha": self.head,
                "repo": {"full_name": runtime_versions.REPOSITORY},
            },
            "user": {"login": "github-actions[bot]"},
            "merge_commit_sha": self.revision,
            "merged_by": {"login": "jskorlol"},
        }

    def reviews(self) -> list[dict]:
        return [
            {
                "user": {"login": "jskorlol"},
                "state": "APPROVED",
                "commit_id": self.head,
            }
        ]

    def test_normal_and_replacement_targets_require_exact_human_provenance(self) -> None:
        normal = runtime_versions.classify_release_provenance(
            self.pull_request(head_ref="develop"),
            self.reviews(),
            self.revision,
            merge_parent=self.base,
        )
        self.assertEqual(normal, {"eligible": True, "origin_kind": "normal", "pr_number": 42})

        nonce = "replace-abc123"
        replacement_pr = self.pull_request(
            head_ref=f"automation/release-repair-{nonce}",
            body=(
                f"release-repair issue=17 nonce={nonce} mode=replacement "
                f"target={self.base} source={'d' * 40}"
            ),
        )
        # GitHub may report a moving base ref after merge; the immutable commit parent is binding.
        replacement_pr["base"]["sha"] = self.revision
        replacement = runtime_versions.classify_release_provenance(
            replacement_pr,
            self.reviews(),
            self.revision,
            merge_parent=self.base,
        )
        self.assertEqual(
            replacement,
            {"eligible": True, "origin_kind": "replacement", "pr_number": 42},
        )

    def test_control_repairs_are_classified_but_never_become_release_targets(self) -> None:
        cases = (
            ("control", "control-abc123", "automation/release-repair-"),
            ("guard-repair", "guard-abc1234", "automation/release-repair-"),
            ("primary-repair", "primary-abc123", "automation/release-repair-guard-"),
        )
        for mode, nonce, prefix in cases:
            with self.subTest(mode=mode):
                result = runtime_versions.classify_release_provenance(
                    self.pull_request(
                        head_ref=f"{prefix}{nonce}",
                        body=(
                            f"release-repair issue=18 nonce={nonce} mode={mode} "
                            f"target={'d' * 40} source={'e' * 40}"
                        ),
                    ),
                    self.reviews(),
                    self.revision,
                    merge_parent=self.base,
                )
                self.assertEqual(
                    result,
                    {"eligible": False, "origin_kind": "control", "pr_number": 42},
                )

    def test_stale_approval_and_unbound_repair_marker_fail_closed(self) -> None:
        stale_reviews = self.reviews()
        stale_reviews[0]["commit_id"] = "f" * 40
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "exact_head_approval_missing"):
            runtime_versions.classify_release_provenance(
                self.pull_request(head_ref="develop"),
                stale_reviews,
                self.revision,
                merge_parent=self.base,
            )

        nonce = "replace-abc123"
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "repair_base_target_mismatch"):
            runtime_versions.classify_release_provenance(
                self.pull_request(
                    head_ref=f"automation/release-repair-{nonce}",
                    body=(
                        f"release-repair issue=19 nonce={nonce} mode=replacement "
                        f"target={'f' * 40} source={'d' * 40}"
                    ),
                ),
                self.reviews(),
                self.revision,
                merge_parent=self.base,
            )

        fork = self.pull_request(head_ref="develop")
        fork["head"]["repo"]["full_name"] = "someone-else/fork"
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "pull_request_provenance_invalid"):
            runtime_versions.classify_release_provenance(
                fork, self.reviews(), self.revision, merge_parent=self.base
            )


class ReleaseStateMachineTests(unittest.TestCase):
    revision = "a" * 40
    digest = "sha256:" + "b" * 64

    def base(self) -> dict:
        return {
            "git_tags": ["v0.3.13"],
            "github_releases": [],
            "registry_tags": ["0.3.13"],
            "package": {
                "name": "multica-runtime-controller",
                "package_type": "container",
                "visibility": "public",
            },
            "version_image": None,
            "git_tag": None,
            "github_release": None,
            "latest": None,
        }

    def image(self) -> dict:
        return {
            "revision": self.revision,
            "version": "0.3.15",
            "digest": self.digest,
            "platforms": ["linux/amd64", "linux/arm64"],
            "source": "https://github.com/korioinc/multica-runtime-controller",
        }

    def release(self) -> dict:
        return {"revision": self.revision, "digest": self.digest, "tag_name": "v0.3.15"}

    def native_digests(self) -> dict:
        return {
            "amd64": {
                "digest": "sha256:" + "c" * 64,
                "platform": "linux/amd64",
                "runner": "ubuntu-24.04",
                "revision": self.revision,
                "source": runtime_versions.IMAGE_SOURCE,
            },
            "arm64": {
                "digest": "sha256:" + "d" * 64,
                "platform": "linux/arm64",
                "runner": "ubuntu-24.04-arm",
                "revision": self.revision,
                "source": runtime_versions.IMAGE_SOURCE,
            },
        }

    def canary_proofs(self) -> dict:
        return {
            "primary": {
                "conclusion": "success",
                "launcher_blob_sha": "1" * 40,
                "ruleset_digest": "sha256:" + "e" * 64,
            },
            "guard": {
                "conclusion": "success",
                "launcher_blob_sha": "2" * 40,
                "ruleset_digest": "sha256:" + "e" * 64,
            },
        }

    def reservable(self) -> dict:
        return self.base() | {
            "native_digests": self.native_digests(),
            "canary_proofs": self.canary_proofs(),
            "launcher_blobs": {"primary": "1" * 40, "guard": "2" * 40},
            "ruleset_digest": "sha256:" + "e" * 64,
        }

    def test_native_proof_and_reservation_precede_numeric_candidate(self) -> None:
        self.assertEqual(
            runtime_versions.plan_release_state(self.base(), "0.3.15", self.revision),
            {"state": "pending", "next_action": "build_native"},
        )
        self.assertEqual(
            runtime_versions.plan_release_state(self.reservable(), "0.3.15", self.revision),
            {"state": "native_built", "next_action": "reserve_tag"},
        )
        reserved = self.reservable() | {"git_tag": self.release()}
        self.assertEqual(
            runtime_versions.plan_release_state(reserved, "0.3.15", self.revision),
            {"state": "reserved", "next_action": "publish_numeric_candidate"},
        )
        expired = self.base() | {"git_tag": self.release()}
        self.assertEqual(
            runtime_versions.plan_release_state(expired, "0.3.15", self.revision),
            {"state": "reserved", "next_action": "rebuild_native"},
        )

        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "candidate_without_reservation"):
            runtime_versions.plan_release_state(
                self.base() | {"version_image": self.image()}, "0.3.15", self.revision
            )

    def test_reservation_requires_exact_native_and_current_canary_proofs(self) -> None:
        missing_arch = self.reservable() | {"native_digests": {"amd64": self.native_digests()["amd64"]}}
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "native_digest_set_mismatch"):
            runtime_versions.plan_release_state(missing_arch, "0.3.15", self.revision)

        stale_canary = self.reservable()
        stale_canary["canary_proofs"]["guard"]["launcher_blob_sha"] = "3" * 40
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "canary_blob_mismatch"):
            runtime_versions.plan_release_state(stale_canary, "0.3.15", self.revision)

        wrong_tag = self.reservable() | {
            "git_tag": self.release() | {"revision": "f" * 40}
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "tag_identity_mismatch"):
            runtime_versions.plan_release_state(wrong_tag, "0.3.15", self.revision)

    def test_identity_is_monotonic_and_replacement_stops_at_reservation(self) -> None:
        terminal = self.base() | {
            "git_tags": ["v0.3.20"],
            "github_releases": [{"tag_name": "v0.3.20"}],
            "registry_tags": ["0.3.20"],
        }
        proposed = runtime_versions.plan_release_identity(terminal, self.revision)
        self.assertEqual(proposed["version"], "0.3.21")
        self.assertEqual(proposed["target_kind"], "normal")
        self.assertEqual(proposed["state"], "pending")

        replacement_without_reservation = runtime_versions.plan_release_identity(
            terminal, self.revision, target_kind="replacement"
        )
        self.assertEqual(replacement_without_reservation["allocation"], "proposed")
        self.assertEqual(replacement_without_reservation["version"], "0.3.21")
        self.assertEqual(replacement_without_reservation["state"], "pending")

        recovery_without_reservation = runtime_versions.plan_release_identity(
            terminal, self.revision, target_kind="recovery"
        )
        self.assertEqual(recovery_without_reservation["allocation"], "proposed")
        self.assertEqual(recovery_without_reservation["version"], "0.3.21")
        self.assertEqual(recovery_without_reservation["state"], "pending")

        active = terminal | {
            "active_target": {
                "revision": "b" * 40,
                "version": "0.3.21",
                "state": "pending",
            }
        }
        replacement = runtime_versions.plan_release_identity(
            active, self.revision, target_kind="replacement"
        )
        self.assertEqual(replacement["version"], "0.3.21")
        self.assertEqual(replacement["revision"], self.revision)

        reserved = active | {
            "active_target": active["active_target"] | {"state": "reserved"},
            "git_tag": {
                "revision": "b" * 40,
                "tag_name": "v0.3.21",
            },
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "replacement_after_reservation"):
            runtime_versions.plan_release_identity(
                reserved, self.revision, target_kind="replacement"
            )

    def test_external_nonterminal_identity_and_wrong_recovery_target_fail_closed(self) -> None:
        unresolved = self.base() | {
            "git_tags": ["v0.3.20", "v0.3.21"],
            "github_releases": [{"tag_name": "v0.3.20"}],
            "registry_tags": ["0.3.20"],
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "external_nonterminal_identity"):
            runtime_versions.plan_release_identity(unresolved, self.revision)

        active = self.base() | {
            "active_target": {
                "revision": "b" * 40,
                "version": "0.3.15",
                "state": "reserved",
            },
            "git_tag": {"revision": "b" * 40, "tag_name": "v0.3.15"},
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "recovery_target_mismatch"):
            runtime_versions.plan_release_identity(
                active, self.revision, target_kind="recovery"
            )

    def test_release_inventory_distinguishes_terminal_from_active_target(self) -> None:
        terminal = self.base() | {
            "git_tags": ["v0.3.20"],
            "github_releases": [{"tag_name": "v0.3.20"}],
            "registry_tags": ["0.3.20"],
        }
        self.assertEqual(
            runtime_versions.plan_release_inventory(terminal),
            {
                "active": False,
                "state": "terminal",
                "terminal_maximum": "0.3.20",
            },
        )
        active = terminal | {
            "active_target": {
                "revision": self.revision,
                "version": "0.3.21",
                "state": "reserved",
            }
        }
        self.assertEqual(
            runtime_versions.plan_release_inventory(active),
            {
                "active": True,
                "revision": self.revision,
                "state": "reserved",
                "terminal_maximum": "0.3.20",
                "version": "0.3.21",
            },
        )

    def test_states_resume_at_the_earliest_incomplete_step(self) -> None:
        absent = self.base()
        self.assertEqual(
            runtime_versions.plan_release_state(absent, "0.3.15", self.revision),
            {"state": "pending", "next_action": "build_native"},
        )
        private_candidate = self.base() | {
            "version_image": self.image(),
            "git_tag": self.release(),
            "registry_tags": ["0.3.13", "0.3.15"],
            "package": {
                "name": "multica-runtime-controller",
                "package_type": "container",
                "visibility": "private",
            },
        }
        self.assertEqual(
            runtime_versions.plan_release_state(private_candidate, "0.3.15", self.revision),
            {
                "state": "candidate_private",
                "next_action": "publish_package_visibility",
                "digest": self.digest,
            },
        )
        candidate = private_candidate | {
            "package": {
                "name": "multica-runtime-controller",
                "package_type": "container",
                "visibility": "public",
            }
        }
        self.assertEqual(
            runtime_versions.plan_release_state(candidate, "0.3.15", self.revision),
            {"state": "candidate_verified", "next_action": "finalize_release", "digest": self.digest},
        )
        released = candidate | {"git_tag": self.release(), "github_release": self.release()}
        self.assertEqual(
            runtime_versions.plan_release_state(released, "0.3.15", self.revision),
            {"state": "release_verified", "next_action": "promote_latest", "digest": self.digest},
        )
        complete = released | {"latest": {"digest": self.digest}}
        self.assertEqual(
            runtime_versions.plan_release_state(complete, "0.3.15", self.revision),
            {"state": "latest_digest_matched", "next_action": "already_published", "digest": self.digest},
        )

    def test_impossible_ordering_and_identity_mismatch_are_terminal_conflicts(self) -> None:
        release_without_image = self.base() | {
            "git_tag": self.release(),
            "github_release": self.release(),
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "release_without_candidate"):
            runtime_versions.plan_release_state(release_without_image, "0.3.15", self.revision)

        mismatched = self.base() | {
            "version_image": self.image() | {"revision": "c" * 40},
            "git_tag": self.release(),
            "registry_tags": ["0.3.13", "0.3.15"],
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "candidate_identity_mismatch"):
            runtime_versions.plan_release_state(mismatched, "0.3.15", self.revision)

        wrong_source = self.base() | {
            "version_image": self.image() | {"source": "https://github.com/other/repository"},
            "git_tag": self.release(),
            "registry_tags": ["0.3.13", "0.3.15"],
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "candidate_source_mismatch"):
            runtime_versions.plan_release_state(wrong_source, "0.3.15", self.revision)

        wrong_package = self.base() | {
            "version_image": self.image(),
            "git_tag": self.release(),
            "registry_tags": ["0.3.13", "0.3.15"],
            "package": {
                "name": "other-package",
                "package_type": "container",
                "visibility": "public",
            },
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "package_identity_mismatch"):
            runtime_versions.plan_release_state(wrong_package, "0.3.15", self.revision)

    def test_preflight_modes_are_mutually_exclusive_and_identity_bound(self) -> None:
        surfaces = self.base()
        runtime_versions.preflight_release(
            surfaces, "0.3.15", self.revision, require_absent=True, allow_same_identity=False
        )
        candidate = surfaces | {
            "version_image": self.image(),
            "git_tag": self.release(),
            "registry_tags": ["0.3.13", "0.3.15"],
        }
        runtime_versions.preflight_release(
            candidate, "0.3.15", self.revision, require_absent=False, allow_same_identity=True
        )
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "recovery_identity_absent"):
            runtime_versions.preflight_release(
                surfaces,
                "0.3.15",
                self.revision,
                require_absent=False,
                allow_same_identity=True,
            )
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "mode"):
            runtime_versions.preflight_release(
                surfaces, "0.3.15", self.revision, require_absent=True, allow_same_identity=True
            )
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "already_exists"):
            runtime_versions.preflight_release(
                candidate, "0.3.15", self.revision, require_absent=True, allow_same_identity=False
            )

        ahead = surfaces | {"git_tags": ["v0.3.16"]}
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "next_patch=0.3.17"):
            runtime_versions.preflight_release(
                ahead, "0.3.15", self.revision, require_absent=True, allow_same_identity=False
            )

    def test_github_release_inventory_reads_every_page(self) -> None:
        first = [{"tag_name": f"v1.0.{index}"} for index in range(100)]
        final = [{"tag_name": "v2.0.0"}]

        with mock.patch(
            "scripts.runtime_versions._github_json", side_effect=[first, final]
        ) as github_json:
            releases = runtime_versions._github_paginated_releases()

        self.assertEqual(len(releases), 101)
        self.assertEqual(releases[-1]["tag_name"], "v2.0.0")
        self.assertEqual(
            [call.args[0] for call in github_json.call_args_list],
            ["releases?per_page=100&page=1", "releases?per_page=100&page=2"],
        )

    def test_github_package_inventory_uses_the_organization_packages_endpoint(self) -> None:
        package = {
            "name": "multica-runtime-controller",
            "package_type": "container",
            "visibility": "public",
        }

        with mock.patch(
            "scripts.runtime_versions._http_json", return_value=package
        ) as http_json:
            result = runtime_versions._github_package()

        self.assertEqual(result, package)
        http_json.assert_called_once_with(
            "https://api.github.com/orgs/korioinc/packages/container/"
            "multica-runtime-controller",
            source="github_package",
        )

    def test_ghcr_tag_inventory_reads_every_page(self) -> None:
        first = [f"0.3.{index}" for index in range(100)]
        final = ["1.0.0"]

        with mock.patch(
            "scripts.runtime_versions._registry_request",
            side_effect=[
                (json.dumps({"name": runtime_versions.IMAGE_REPOSITORY, "tags": first}).encode(), {}),
                (json.dumps({"name": runtime_versions.IMAGE_REPOSITORY, "tags": final}).encode(), {}),
            ],
        ) as registry_request:
            tags = runtime_versions._ghcr_tags()

        self.assertEqual(tags, first + final)
        self.assertEqual(
            [call.args[0] for call in registry_request.call_args_list],
            ["tags/list?n=100", "tags/list?n=100&last=0.3.99"],
        )

    def test_ghcr_registry_uses_ephemeral_github_credentials(self) -> None:
        token_response = mock.MagicMock()
        token_response.__enter__.return_value.read.return_value = b'{"token":"registry-token"}'
        registry_response = mock.MagicMock()
        registry_response.__enter__.return_value.read.return_value = b'{"tags":[]}'
        registry_response.__enter__.return_value.headers.items.return_value = []

        with (
            mock.patch.dict(
                os.environ,
                {"GITHUB_ACTOR": "release-actor", "GH_TOKEN": "ephemeral-token"},
                clear=False,
            ),
            mock.patch(
                "scripts.runtime_versions.urlrequest.urlopen",
                side_effect=[token_response, registry_response],
            ) as urlopen,
        ):
            raw, _ = runtime_versions._registry_request("tags/list?n=100")

        token_request = urlopen.call_args_list[0].args[0]
        registry_request = urlopen.call_args_list[1].args[0]
        self.assertEqual(raw, b'{"tags":[]}')
        self.assertEqual(
            token_request.full_url,
            "https://ghcr.io/token?service=ghcr.io&scope="
            "repository:korioinc%2Fmultica-runtime-controller:pull",
        )
        self.assertEqual(
            token_request.get_header("Authorization"),
            "Basic cmVsZWFzZS1hY3RvcjplcGhlbWVyYWwtdG9rZW4=",
        )
        self.assertEqual(
            registry_request.full_url,
            "https://ghcr.io/v2/korioinc/multica-runtime-controller/tags/list?n=100",
        )
        self.assertEqual(registry_request.get_header("Authorization"), "Bearer registry-token")

    def test_live_inventory_skips_ghcr_until_the_package_exists(self) -> None:
        command_result = mock.MagicMock(stdout="", returncode=1)
        with (
            mock.patch("scripts.runtime_versions._run", return_value=command_result),
            mock.patch("scripts.runtime_versions._github_paginated_releases", return_value=[]),
            mock.patch("scripts.runtime_versions._github_package", return_value=None),
            mock.patch("scripts.runtime_versions._github_json", return_value=None),
            mock.patch("scripts.runtime_versions._ghcr_tags") as ghcr_tags,
        ):
            surfaces = runtime_versions.live_release_surfaces("0.3.15")

        ghcr_tags.assert_not_called()
        self.assertIsNone(surfaces["package"])
        self.assertEqual(surfaces["registry_tags"], [])
        self.assertIsNone(surfaces["version_image"])

    def test_live_identity_does_not_hide_latest_digest_drift_as_terminal(self) -> None:
        version_image = self.image() | {"version": "0.3.20"}
        release = {
            "digest": version_image["digest"],
            "revision": self.revision,
            "tag_name": "v0.3.20",
        }
        live = {
            "git_tags": ["v0.3.20"],
            "github_releases": [{"tag_name": "v0.3.20"}],
            "registry_tags": ["0.3.20", "latest"],
            "package": {
                "name": "multica-runtime-controller",
                "package_type": "container",
                "visibility": "public",
            },
            "version_image": version_image,
            "git_tag": release,
            "github_release": release,
            "latest": {
                "digest": "sha256:" + "f" * 64,
                "revision": self.revision,
                "version": "0.3.20",
            },
        }
        command = mock.MagicMock(stdout="v0.3.20\n")
        with (
            mock.patch("scripts.runtime_versions._run", return_value=command),
            mock.patch(
                "scripts.runtime_versions._github_paginated_releases",
                return_value=[{"tag_name": "v0.3.20", "draft": False, "prerelease": False}],
            ),
            mock.patch(
                "scripts.runtime_versions._github_package",
                return_value={
                    "name": "multica-runtime-controller",
                    "package_type": "container",
                    "visibility": "public",
                },
            ),
            mock.patch("scripts.runtime_versions._ghcr_tags", return_value=["0.3.20", "latest"]),
            mock.patch(
                "scripts.runtime_versions._registry_image",
                return_value=live["latest"],
            ),
            mock.patch("scripts.runtime_versions.live_release_surfaces", return_value=live),
        ):
            surfaces = runtime_versions.live_identity_surfaces(self.revision)

        self.assertEqual(
            surfaces["active_target"],
            {"revision": self.revision, "state": "release_verified", "version": "0.3.20"},
        )

    def test_duplicate_terminal_target_reuses_identity_without_blocking_next_target(self) -> None:
        version_image = self.image() | {"version": "0.3.20"}
        release = {
            "digest": version_image["digest"],
            "revision": self.revision,
            "tag_name": "v0.3.20",
        }
        live = {
            "git_tags": ["v0.3.20"],
            "github_releases": [{"tag_name": "v0.3.20"}],
            "registry_tags": ["0.3.20", "latest"],
            "package": {
                "name": "multica-runtime-controller",
                "package_type": "container",
                "visibility": "public",
            },
            "version_image": version_image,
            "git_tag": release,
            "github_release": release,
            "latest": version_image,
        }
        command = mock.MagicMock(stdout="v0.3.20\n")
        patches = (
            mock.patch("scripts.runtime_versions._run", return_value=command),
            mock.patch(
                "scripts.runtime_versions._github_paginated_releases",
                return_value=[{"tag_name": "v0.3.20", "draft": False, "prerelease": False}],
            ),
            mock.patch(
                "scripts.runtime_versions._github_package",
                return_value={
                    "name": "multica-runtime-controller",
                    "package_type": "container",
                    "visibility": "public",
                },
            ),
            mock.patch("scripts.runtime_versions._ghcr_tags", return_value=["0.3.20", "latest"]),
            mock.patch("scripts.runtime_versions._registry_image", return_value=version_image),
            mock.patch("scripts.runtime_versions.live_release_surfaces", return_value=live),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            duplicate = runtime_versions.live_identity_surfaces(self.revision)
        self.assertEqual(duplicate["active_target"]["state"], "latest_digest_matched")
        duplicate_plan = runtime_versions.plan_release_identity(duplicate, self.revision)
        self.assertEqual(duplicate_plan["version"], "0.3.20")
        self.assertEqual(duplicate_plan["next_action"], "already_published")

        with (
            mock.patch("scripts.runtime_versions._run", return_value=command),
            mock.patch(
                "scripts.runtime_versions._github_paginated_releases",
                return_value=[{"tag_name": "v0.3.20", "draft": False, "prerelease": False}],
            ),
            mock.patch(
                "scripts.runtime_versions._github_package",
                return_value={
                    "name": "multica-runtime-controller",
                    "package_type": "container",
                    "visibility": "public",
                },
            ),
            mock.patch("scripts.runtime_versions._ghcr_tags", return_value=["0.3.20", "latest"]),
            mock.patch("scripts.runtime_versions._registry_image", return_value=version_image),
            mock.patch("scripts.runtime_versions.live_release_surfaces", return_value=live),
        ):
            next_target = runtime_versions.live_identity_surfaces("0" * 40)
        self.assertIsNone(next_target["active_target"])


class RepairTransactionTests(unittest.TestCase):
    def base_state(self, stage: str) -> dict:
        source = fixture("repair-transaction-base.json")
        request = source["request"] | {"body_hash": "", "issue_number": 71, "seal_hash": "", "seal_id": 0}
        checkpoint = runtime_versions.make_repair_checkpoint(
            request=source["request"],
            workflows=source["workflows"],
            quiesced_workflow_ids=source["quiesced_workflow_ids"],
            ruleset_digest=source["ruleset_digest"],
            initial_surface=source["surface"],
        )
        body = runtime_versions.canonical_repair_json(checkpoint)
        request["body_hash"] = runtime_versions.repair_sha256(body)
        issue = {
            "author": "github-actions[bot]",
            "body": body,
            "created_at": "2026-08-17T00:00:00Z",
            "number": 71,
            "state": "open",
            "updated_at": "2026-08-17T00:00:00Z",
        }
        current_states = {str(item["workflow_id"]): item["original_state"] for item in source["workflows"]}
        state = {
            "actor": source["actor"],
            "active_runs": 0,
            "canaries": {"guard": "pending", "primary": "pending"},
            "changed_files": [".github/workflows/release-repair-guard.yml"],
            "checkpoint_created_before_mutation": True,
            "checkpoints": [issue],
            "current_workflow_states": current_states,
            "drain_started_before_disable": False,
            "gate_dominated": True,
            "gate_enabled": False,
            "launcher": source["launcher"],
            "permission_status": 200,
            "repair_pr_state": "absent",
            "request": request,
            "ruleset_digest": source["ruleset_digest"],
            "seals": [],
            "surface": source["surface"],
            "workflow_inventory": source["workflows"],
        }
        quiesced = [str(item) for item in source["quiesced_workflow_ids"]]
        if stage in {"partial_disable", "draining", "sealed", "staged", "partial_restore", "restored"}:
            state["current_workflow_states"][quiesced[0]] = "disabled_manually"
        if stage in {"draining", "sealed", "staged", "partial_restore", "restored"}:
            for workflow_id in quiesced:
                state["current_workflow_states"][workflow_id] = "disabled_manually"
        if stage == "draining":
            state["active_runs"] = 2
        if stage in {"sealed", "staged", "partial_restore", "restored"}:
            baseline = source["surface"] | {
                "workflow_states": [
                    {"original_state": item["original_state"], "workflow_id": item["workflow_id"]}
                    for item in source["workflows"]
                ]
            }
            seal_value = runtime_versions.make_repair_seal(
                issue_number=71,
                nonce=source["request"]["nonce"],
                body_hash=request["body_hash"],
                baseline=baseline,
                main_revision=source["request"]["main_revision"],
                ruleset_digest=source["ruleset_digest"],
            )
            seal_body = runtime_versions.canonical_repair_json(seal_value)
            request["seal_id"] = 801
            request["seal_hash"] = runtime_versions.repair_sha256(seal_body)
            state["seals"] = [
                {
                    "author": "github-actions[bot]",
                    "body": seal_body,
                    "created_at": "2026-08-17T00:05:00Z",
                    "id": 801,
                    "updated_at": "2026-08-17T00:05:00Z",
                }
            ]
        if stage in {"staged", "partial_restore", "restored"}:
            state["repair_pr_state"] = "merged"
            state["canaries"] = {"guard": "success", "primary": "pending"}
            state["current_workflow_states"]["104"] = "active"
        if stage == "partial_restore":
            state["canaries"] = {"guard": "success", "primary": "success"}
            state["current_workflow_states"]["101"] = "active"
        if stage == "restored":
            state["canaries"] = {"guard": "success", "primary": "success"}
            state["current_workflow_states"] = {
                str(item["workflow_id"]): item["original_state"] for item in source["workflows"]
            }
        return state

    def test_crash_resume_uses_one_checkpoint_seal_and_original_mapping(self) -> None:
        cases = {
            "checkpoint": "disable_and_drain",
            "partial_disable": "disable_and_drain",
            "draining": "cancel_and_drain",
            "sealed": "propose_repair",
            "staged": "run_peer_canaries",
            "partial_restore": "restore_exact_mapping",
            "restored": "close_checkpoint",
        }
        for stage, expected in cases.items():
            with self.subTest(stage=stage):
                result = runtime_versions.plan_repair_resume(self.base_state(stage))
                self.assertEqual(result["next_action"], expected)
                self.assertEqual(result["issue_number"], 71)
                if stage in {"sealed", "staged", "partial_restore", "restored"}:
                    self.assertEqual(result["seal_id"], 801)

        drained = self.base_state("sealed")
        drained["seals"] = []
        drained["request"]["seal_id"] = 0
        drained["request"]["seal_hash"] = ""
        self.assertEqual(
            runtime_versions.plan_repair_resume(drained)["next_action"],
            "create_quiesced_seal",
        )

        canary_residue = self.base_state("sealed")
        nonce = canary_residue["request"]["nonce"]
        canary_residue["surface"]["refs"].append(
            {
                "ref": f"refs/heads/automation/release-repair-canary-{nonce}-guard",
                "sha": "c" * 40,
            }
        )
        canary_residue["surface"]["pull_requests"].append(
            {
                "base": "main",
                "head": f"automation/release-repair-canary-{nonce}-guard",
                "number": 88,
                "sha": "c" * 40,
            }
        )
        self.assertEqual(
            runtime_versions.plan_repair_resume(canary_residue)["next_action"],
            "propose_repair",
        )

        merged = self.base_state("sealed")
        merged["repair_pr_state"] = "merged"
        main_ref = next(
            item for item in merged["surface"]["refs"] if item["ref"] == "refs/heads/main"
        )
        main_ref["sha"] = "d" * 40
        merged["surface"]["check_runs"] = [
            {
                "app_id": 15368,
                "conclusion": "success",
                "head_sha": "d" * 40,
                "id": 9002,
                "name": "verify",
                "status": "completed",
            },
            {
                "app_id": 15368,
                "conclusion": "success",
                "head_sha": "d" * 40,
                "id": 9003,
                "name": "runtime-image",
                "status": "completed",
            },
        ]
        self.assertEqual(
            runtime_versions.plan_repair_resume(merged)["next_action"],
            "run_peer_canaries",
        )

        prematurely_closed = self.base_state("restored")
        prematurely_closed["checkpoints"][0]["state"] = "closed"
        self.assertEqual(
            runtime_versions.plan_repair_resume(prematurely_closed)["next_action"],
            "reopen_checkpoint",
        )

    def test_repair_tamper_order_delta_and_gate_negatives_fail_closed(self) -> None:
        mutations = {
            "other_actor": lambda state: state.update(actor="intruder"),
            "checkpoint_after_mutation": lambda state: state.update(checkpoint_created_before_mutation=False),
            "issue_body_edit": lambda state: state["checkpoints"][0].update(body=state["checkpoints"][0]["body"] + "\n"),
            "issue_hash_edit": lambda state: state["request"].update(body_hash="0" * 64),
            "duplicate_nonce": lambda state: state["checkpoints"].append(dict(state["checkpoints"][0], number=72)),
            "seal_edit": lambda state: state["seals"][0].update(updated_at="2026-08-17T00:06:00Z"),
            "seal_hash_edit": lambda state: state["request"].update(seal_hash="0" * 64),
            "duplicate_seal": lambda state: state["seals"].append(dict(state["seals"][0], id=802)),
            "permission_403": lambda state: state.update(permission_status=403),
            "drain_before_disable": lambda state: state.update(drain_started_before_disable=True),
            "nonzero_run_after_seal": lambda state: state.update(active_runs=1),
            "baseline_release_mutation": lambda state: state["surface"].update(releases=[]),
            "unexpected_ref": lambda state: state["surface"]["refs"].append({"ref": "refs/heads/intruder", "sha": "f" * 40}),
            "gate_on": lambda state: state.update(gate_enabled=True),
            "gate_bypass": lambda state: state.update(gate_dominated=False),
            "self_edit": lambda state: state["changed_files"].append(".github/workflows/release-repair.yml"),
            "dual_launcher_edit": lambda state: state["changed_files"].extend([".github/workflows/release-repair.yml"]),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                state = self.base_state("sealed")
                mutate(state)
                with self.assertRaises(runtime_versions.RepairConflict):
                    runtime_versions.plan_repair_resume(state)

        failed = self.base_state("staged")
        failed["canaries"]["guard"] = "failure"
        self.assertEqual(
            runtime_versions.plan_repair_resume(failed)["next_action"],
            "re_disable_staged_launcher",
        )

        bad_check = self.base_state("sealed")
        main_ref = next(
            item for item in bad_check["surface"]["refs"] if item["ref"] == "refs/heads/main"
        )
        main_ref["sha"] = "d" * 40
        bad_check["surface"]["check_runs"] = [
            {
                "app_id": 15368,
                "conclusion": "success",
                "head_sha": "d" * 40,
                "id": 9002,
                "name": "untrusted",
                "status": "completed",
            }
        ]
        with self.assertRaisesRegex(runtime_versions.RepairConflict, "check_run"):
            runtime_versions.plan_repair_resume(bad_check)

        missing_checks = self.base_state("sealed")
        missing_main = next(
            item
            for item in missing_checks["surface"]["refs"]
            if item["ref"] == "refs/heads/main"
        )
        missing_main["sha"] = "d" * 40
        missing_checks["surface"]["check_runs"] = []
        with self.assertRaisesRegex(runtime_versions.RepairConflict, "check_run"):
            runtime_versions.plan_repair_resume(missing_checks)


if __name__ == "__main__":
    unittest.main()

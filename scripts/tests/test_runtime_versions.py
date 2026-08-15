from __future__ import annotations

import json
import os
from pathlib import Path
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
    def test_current_sources_are_a_successful_noop(self) -> None:
        current = runtime_versions.parse_env_bytes(ENV_BYTES)
        result = runtime_versions.resolve_versions(
            current, runtime_versions.fixture_fetcher(fixture("upstreams-current.json"))
        )

        self.assertEqual(result["updates"], [])
        self.assertEqual(result["current"], result["latest"])

    def test_update_preserves_env_layout_and_bumps_release_patch_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env_path = root / "runtime-versions.env"
            version_path = root / "VERSION"
            env_path.write_bytes(ENV_BYTES)
            version_path.write_text("0.3.15\n", encoding="utf-8")
            before_lines = env_path.read_text(encoding="utf-8").splitlines()

            result = runtime_versions.update_files(
                env_path,
                version_path,
                runtime_versions.fixture_fetcher(fixture("upstreams-new.json")),
            )

            self.assertEqual(
                [update["field"] for update in result["updates"]],
                ["MULTICA_CLI_VERSION", "CODEX_VERSION", "PI_VERSION"],
            )
            self.assertEqual(version_path.read_text(encoding="utf-8"), "0.3.16\n")
            after_lines = env_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(before_lines), len(after_lines))
            for before, after in zip(before_lines, after_lines, strict=True):
                if before.startswith(("MULTICA_CLI_VERSION=", "CODEX_VERSION=", "PI_VERSION=")):
                    continue
                self.assertEqual(after, before)
            self.assertTrue(env_path.read_bytes().endswith(b"\n"))
            self.assertEqual(env_path.stat().st_mode & 0o777, 0o644)
            self.assertEqual(version_path.stat().st_mode & 0o777, 0o644)

    def test_build_args_include_every_env_assignment_and_release_identity(self) -> None:
        args = runtime_versions.build_args(
            ROOT / "build" / "runtime-versions.env", ROOT / "VERSION", "a" * 40
        )

        self.assertEqual(args["MULTICA_CLI_VERSION"], "0.4.26")
        self.assertEqual(args["CODEX_VERSION"], "0.147.0")
        self.assertEqual(args["PI_VERSION"], "0.84.2")
        self.assertEqual(args["VERSION"], (ROOT / "VERSION").read_text().strip())
        self.assertEqual(args["COMMIT"], "a" * 40)


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

    def test_second_replace_failure_restores_both_original_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env_path = root / "runtime-versions.env"
            version_path = root / "VERSION"
            env_path.write_bytes(ENV_BYTES)
            version_path.write_text("0.3.15\n", encoding="utf-8")
            original_env = env_path.read_bytes()
            original_version = version_path.read_bytes()
            real_replace = os.replace
            replacements = 0

            def fail_second(source: os.PathLike[str] | str, target: os.PathLike[str] | str) -> None:
                nonlocal replacements
                replacements += 1
                if replacements == 2:
                    raise OSError("injected second replace failure")
                real_replace(source, target)

            with mock.patch("scripts.runtime_versions.os.replace", side_effect=fail_second):
                with self.assertRaisesRegex(runtime_versions.ResolverError, "atomic_write"):
                    runtime_versions.update_files(
                        env_path,
                        version_path,
                        runtime_versions.fixture_fetcher(fixture("upstreams-new.json")),
                    )

            self.assertEqual(env_path.read_bytes(), original_env)
            self.assertEqual(version_path.read_bytes(), original_version)


class ReleaseStateMachineTests(unittest.TestCase):
    revision = "a" * 40
    digest = "sha256:" + "b" * 64

    def base(self) -> dict:
        return {
            "git_tags": ["v0.3.13"],
            "github_releases": [],
            "docker_tags": ["0.3.13"],
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
        }

    def release(self) -> dict:
        return {"revision": self.revision, "digest": self.digest, "tag_name": "v0.3.15"}

    def test_states_resume_at_the_earliest_incomplete_step(self) -> None:
        absent = self.base()
        self.assertEqual(
            runtime_versions.plan_release_state(absent, "0.3.15", self.revision),
            {"state": "absent", "next_action": "build_candidate"},
        )
        candidate = self.base() | {"version_image": self.image(), "docker_tags": ["0.3.13", "0.3.15"]}
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
            "docker_tags": ["0.3.13", "0.3.15"],
        }
        with self.assertRaisesRegex(runtime_versions.ReleaseConflict, "candidate_identity_mismatch"):
            runtime_versions.plan_release_state(mismatched, "0.3.15", self.revision)

    def test_preflight_modes_are_mutually_exclusive_and_identity_bound(self) -> None:
        surfaces = self.base()
        runtime_versions.preflight_release(
            surfaces, "0.3.15", self.revision, require_absent=True, allow_same_identity=False
        )
        candidate = surfaces | {
            "version_image": self.image(),
            "docker_tags": ["0.3.13", "0.3.15"],
        }
        runtime_versions.preflight_release(
            candidate, "0.3.15", self.revision, require_absent=False, allow_same_identity=True
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


if __name__ == "__main__":
    unittest.main()

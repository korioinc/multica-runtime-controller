#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib import error as urlerror
from urllib import request as urlrequest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / "build/runtime-versions.env"
VERSION_PATH = PROJECT_ROOT / "VERSION"

REQUESTED_FIELDS = ("MULTICA_CLI_VERSION",)
SOURCE_URL = "https://api.github.com/repos/multica-ai/multica/releases/latest"
ASSIGNMENT_PATTERN = re.compile(r"([A-Z][A-Z0-9_]*)=([^\s]+)")
SEMVER_PATTERN = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)")
REVISION_PATTERN = re.compile(r"[0-9a-f]{40}")
ACTION_USES_PATTERN = re.compile(r"^\s*uses:\s*([^\s#]+)\s*(?:#\s*(.*))?$")
HELM_REPOSITORY_SECRET = "${{ secrets.HELM_REPOSITORY_TOKEN }}"


class ResolverError(RuntimeError):
    pass


@total_ordering
@dataclass(frozen=True)
class SemVer:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, raw: str, *, field: str = "version") -> "SemVer":
        match = SEMVER_PATTERN.fullmatch(raw)
        if match is None:
            raise ResolverError(f"invalid_semver field={field} value={raw!r}")
        return cls(*(int(part) for part in match.groups()))

    def bump_patch(self) -> "SemVer":
        return SemVer(self.major, self.minor, self.patch + 1)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (
            other.major,
            other.minor,
            other.patch,
        )


def activate_target_root(root: Path) -> None:
    if not root.is_absolute():
        raise ResolverError("target root must be an absolute path")
    global PROJECT_ROOT, ENV_PATH, VERSION_PATH
    PROJECT_ROOT = root
    ENV_PATH = root / "build/runtime-versions.env"
    VERSION_PATH = root / "VERSION"


def _run(*command: str, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd or PROJECT_ROOT,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def parse_env_bytes(content: bytes) -> dict[str, str]:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ResolverError("runtime_versions_not_utf8") from exc
    assignments: dict[str, str] = {}
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = ASSIGNMENT_PATTERN.fullmatch(line)
        if match is None:
            raise ResolverError(f"invalid_assignment line={line_number}")
        name, value = match.groups()
        if name in assignments:
            raise ResolverError(f"duplicate_assignment field={name}")
        assignments[name] = value
    for field in REQUESTED_FIELDS:
        if field not in assignments:
            raise ResolverError(f"missing_assignment field={field}")
        SemVer.parse(assignments[field], field=field)
    return assignments


def read_version(path: Path = VERSION_PATH) -> SemVer:
    content = path.read_bytes()
    if not content.endswith(b"\n") or content.count(b"\n") != 1:
        raise ResolverError("VERSION must contain one newline-terminated stable SemVer")
    try:
        value = content[:-1].decode("ascii")
    except UnicodeDecodeError as exc:
        raise ResolverError("VERSION must be ASCII") from exc
    return SemVer.parse(value, field="VERSION")


def fixture_fetcher(data: Mapping[str, Any]) -> Callable[[str, int], dict[str, Any]]:
    def fetch(source: str, _attempt: int) -> dict[str, Any]:
        try:
            value = data[source]
        except KeyError as exc:
            raise ResolverError(f"fixture_missing_source source={source}") from exc
        if not isinstance(value, dict):
            raise ResolverError(f"fixture_invalid_source source={source}")
        return json.loads(json.dumps(value))

    return fetch


def _http_json(timeout: float = 15.0) -> dict[str, Any]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "multica-runtime-controller-version-resolver/2",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urlrequest.Request(SOURCE_URL, headers=headers)
    with urlrequest.urlopen(request, timeout=timeout) as response:
        payload = response.read()
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ResolverError("malformed_json source=multica") from exc
    if not isinstance(value, dict):
        raise ResolverError("malformed_schema source=multica")
    return value


def live_fetcher(source: str, _attempt: int) -> dict[str, Any]:
    if source != "multica":
        raise ResolverError(f"unsupported_source source={source}")
    return _http_json()


def _fetch_with_retry(
    source: str,
    fetch: Callable[[str, int], dict[str, Any]],
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    delays = (2.0, 8.0)
    for attempt in range(1, 4):
        try:
            return fetch(source, attempt)
        except urlerror.HTTPError as exc:
            status = exc.code
            exc.close()
            if 400 <= status < 500:
                raise ResolverError(f"http_4xx source={source} status={status}") from exc
            terminal = ResolverError(
                f"http_5xx source={source} status={status} attempts={attempt}"
            )
        except (TimeoutError, urlerror.URLError) as exc:
            terminal = ResolverError(f"transport_timeout source={source} attempts={attempt}")
        except ResolverError:
            raise
        if attempt == 3:
            raise terminal
        sleep(delays[attempt - 1])
    raise AssertionError("unreachable retry state")


def _parse_multica(payload: Mapping[str, Any]) -> SemVer:
    if payload.get("draft") is not False or payload.get("prerelease") is not False:
        raise ResolverError("multica_release_not_stable")
    tag = payload.get("tag_name")
    if not isinstance(tag, str) or not tag.startswith("v"):
        raise ResolverError("multica_tag_schema")
    version = SemVer.parse(tag[1:], field="MULTICA_CLI_VERSION")
    assets = payload.get("assets")
    if not isinstance(assets, list):
        raise ResolverError("multica_assets_schema")
    expected = {
        f"multica-cli-{version}-linux-amd64.tar.gz",
        f"multica-cli-{version}-linux-arm64.tar.gz",
        "checksums.txt",
    }
    uploaded = {
        asset.get("name")
        for asset in assets
        if isinstance(asset, dict)
        and isinstance(asset.get("size"), int)
        and asset["size"] > 0
        and asset.get("state") == "uploaded"
    }
    missing = expected - uploaded
    if missing:
        raise ResolverError(f"multica_missing_assets names={','.join(sorted(missing))}")
    return version


def resolve_versions(
    current: Mapping[str, str],
    fetch: Callable[[str, int], dict[str, Any]] = live_fetcher,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    payload = _fetch_with_retry("multica", fetch, sleep)
    latest = _parse_multica(payload)
    existing = SemVer.parse(current["MULTICA_CLI_VERSION"], field="MULTICA_CLI_VERSION")
    if latest < existing:
        raise ResolverError(f"downgrade field=MULTICA_CLI_VERSION current={existing} latest={latest}")
    updates: list[dict[str, str]] = []
    if latest > existing:
        updates.append(
            {"field": "MULTICA_CLI_VERSION", "from": str(existing), "to": str(latest)}
        )
    return {
        "current": {"MULTICA_CLI_VERSION": str(existing)},
        "latest": {"MULTICA_CLI_VERSION": str(latest)},
        "updates": updates,
    }


def _render_env(original: bytes, latest: Mapping[str, str]) -> bytes:
    newline = b"\n" if original.endswith(b"\n") else b""
    rendered: list[str] = []
    replaced = False
    for line in original.decode("utf-8").splitlines():
        match = ASSIGNMENT_PATTERN.fullmatch(line)
        if match and match.group(1) == "MULTICA_CLI_VERSION":
            rendered.append(f"MULTICA_CLI_VERSION={latest['MULTICA_CLI_VERSION']}")
            replaced = True
        else:
            rendered.append(line)
    if not replaced:
        raise ResolverError("requested_field_set_changed")
    return "\n".join(rendered).encode("utf-8") + newline


def _write_temp(path: Path, content: bytes) -> Path:
    descriptor, raw_path = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(raw_path)
    try:
        os.fchmod(descriptor, path.stat().st_mode & 0o777)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return temp_path


def update_files(
    env_path: Path,
    version_path: Path,
    fetch: Callable[[str, int], dict[str, Any]] = live_fetcher,
) -> dict[str, Any]:
    original_env = env_path.read_bytes()
    original_version = version_path.read_bytes()
    result = resolve_versions(parse_env_bytes(original_env), fetch)
    result["release_version"] = None
    if not result["updates"]:
        return result

    current_version = read_version(version_path)
    next_version = current_version.bump_patch()
    next_env = _render_env(original_env, result["latest"])
    next_version_bytes = f"{next_version}\n".encode("ascii")
    parse_env_bytes(next_env)
    SemVer.parse(str(next_version), field="VERSION")

    env_temp = _write_temp(env_path, next_env)
    version_temp = _write_temp(version_path, next_version_bytes)
    try:
        os.replace(env_temp, env_path)
        os.replace(version_temp, version_path)
    except OSError as exc:
        env_path.write_bytes(original_env)
        version_path.write_bytes(original_version)
        raise ResolverError(f"atomic_write rollback_complete cause={type(exc).__name__}") from exc
    finally:
        env_temp.unlink(missing_ok=True)
        version_temp.unlink(missing_ok=True)

    result["release_version"] = {"from": str(current_version), "to": str(next_version)}
    return result


def build_args(
    env_path: Path,
    version_path: Path,
    revision: str | None = None,
    *,
    version: str | None = None,
) -> dict[str, str]:
    assignments = parse_env_bytes(env_path.read_bytes())
    if version is None:
        release_version = str(read_version(version_path))
    elif version in {"ci", "develop"}:
        release_version = version
    else:
        release_version = str(SemVer.parse(version, field="build_version"))
    if revision is None:
        revision = _run("git", "rev-parse", "HEAD").stdout.strip()
    if REVISION_PATTERN.fullmatch(revision) is None:
        raise ResolverError("COMMIT must be a full 40-hex revision")
    return assignments | {"VERSION": release_version, "COMMIT": revision}


def validate_repository(base_ref: str | None, *, automation_diff: bool = False) -> dict[str, Any]:
    current_env = parse_env_bytes(ENV_PATH.read_bytes())
    current_version = read_version(VERSION_PATH)
    result: dict[str, Any] = {
        "runtime_fields": len(current_env),
        "base_ref": base_ref,
        "version": str(current_version),
    }
    if base_ref is None:
        return result

    base_env_result = _run("git", "show", f"{base_ref}:build/runtime-versions.env", check=False)
    base_version_result = _run("git", "show", f"{base_ref}:VERSION", check=False)
    if base_env_result.returncode != 0 or base_version_result.returncode != 0:
        raise ResolverError("automation_base_files_missing")
    base_env = parse_env_bytes(base_env_result.stdout.encode("utf-8"))
    base_version = SemVer.parse(base_version_result.stdout.strip(), field="base_VERSION")
    all_changed_fields = [
        field
        for field in sorted(set(base_env) | set(current_env))
        if base_env.get(field) != current_env.get(field)
    ]
    unexpected = set(all_changed_fields) - set(REQUESTED_FIELDS)
    if unexpected:
        raise ResolverError(
            f"unexpected_runtime_fields fields={','.join(sorted(unexpected))}"
        )
    changed_fields = all_changed_fields
    for field in changed_fields:
        if SemVer.parse(current_env[field], field=field) <= SemVer.parse(base_env[field], field=field):
            raise ResolverError(f"runtime_version_not_increased field={field}")

    expected_version = base_version.bump_patch() if changed_fields else base_version
    if current_version != expected_version:
        raise ResolverError(
            f"release_version_mismatch expected={expected_version} actual={current_version}"
        )
    if automation_diff:
        names = {
            line
            for line in _run("git", "diff", "--name-only", base_ref, "--").stdout.splitlines()
            if line
        }
        expected_files = {"VERSION", "build/runtime-versions.env"}
        if names != expected_files:
            raise ResolverError(
                f"automation_diff_mismatch expected={sorted(expected_files)} actual={sorted(names)}"
            )
        if changed_fields != ["MULTICA_CLI_VERSION"]:
            raise ResolverError("automation_diff_requires_cli_update")
    result["changed_fields"] = changed_fields
    result["release_version"] = {"from": str(base_version), "to": str(current_version)}
    return result


def _job_blocks(text: str) -> dict[str, str]:
    jobs_match = re.search(r"(?m)^jobs:\s*$", text)
    if jobs_match is None:
        raise ResolverError("workflow_jobs_missing")
    job_text = text[jobs_match.end() :]
    matches = list(re.finditer(r"(?m)^  ([A-Za-z0-9_-]+):\s*$", job_text))
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(job_text)
        blocks[match.group(1)] = job_text[match.start() : end]
    if not blocks:
        raise ResolverError("workflow_job_definitions_missing")
    return blocks


def validate_actions(directory: Path) -> dict[str, Any]:
    workflows = sorted((*directory.glob("*.yml"), *directory.glob("*.yaml")))
    expected = {
        "runtime-version-update.yml",
        "create-develop-to-main-pr.yml",
        "release.yml",
    }
    actual = {path.name for path in workflows}
    if actual != expected:
        raise ResolverError(
            f"workflow_inventory_mismatch expected={','.join(sorted(expected))} "
            f"actual={','.join(sorted(actual))}"
        )

    texts: dict[str, str] = {}
    action_count = 0
    checkout_count = 0
    for path in workflows:
        text = path.read_text(encoding="utf-8")
        texts[path.name] = text
        if re.search(r"(?m)^permissions:\s*\{\}\s*$", text) is None:
            raise ResolverError(f"workflow_top_permissions_not_empty file={path.name}")
        secret_exception = (
            path.name == "release.yml"
            and text.count(HELM_REPOSITORY_SECRET) == 1
            and text.count("secrets.") == 1
        )
        if "pull_request_target:" in text or (
            "secrets." in text and not secret_exception
        ):
            raise ResolverError(f"workflow_privilege_contract_failed file={path.name}")
        for job_name, block in _job_blocks(text).items():
            if re.search(r"(?m)^    permissions:(?:\s*\{\})?\s*$", block) is None:
                raise ResolverError(
                    f"job_permissions_missing file={path.name} job={job_name}"
                )
        lines = text.splitlines()
        for index, line in enumerate(lines):
            match = ACTION_USES_PATTERN.match(line)
            if match is None:
                continue
            reference, comment = match.groups()
            if reference.startswith("./"):
                continue
            action_count += 1
            action, separator, revision = reference.rpartition("@")
            if not separator or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
                raise ResolverError(f"action_not_full_sha file={path.name} action={action}")
            if not comment or re.search(r"v[0-9]", comment) is None:
                raise ResolverError(
                    f"action_version_comment_missing file={path.name} action={action}"
                )
            if action == "actions/checkout":
                checkout_count += 1
                indentation = len(line) - len(line.lstrip())
                step_lines: list[str] = []
                for candidate in lines[index + 1 :]:
                    candidate_indent = len(candidate) - len(candidate.lstrip())
                    if candidate.strip().startswith("- ") and candidate_indent == indentation:
                        break
                    step_lines.append(candidate)
                if re.search(
                    r"(?m)^\s+persist-credentials:\s*false\s*$", "\n".join(step_lines)
                ) is None:
                    raise ResolverError(
                        f"checkout_persists_credentials file={path.name} line={index + 1}"
                    )

    required_fragments = {
        "runtime-version-update.yml": (
            "schedule:",
            "workflow_dispatch:",
            "build/runtime-versions.env VERSION",
            "gh pr create",
            "pulls/$PR_NUMBER/merge",
            'event_type:"create-develop-to-main-pr"',
        ),
        "create-develop-to-main-pr.yml": (
            "pull_request:",
            "types: [automation-ci, create-develop-to-main-pr]",
            '-f name="verify"',
            '-f name="runtime-image"',
            "--base main --head develop",
            "gh pr create --base main --head develop",
        ),
        "release.yml": (
            "branches: [main]",
            "docker/build-push-action@",
            "platform: linux/amd64",
            "platform: linux/arm64",
            "docker buildx imagetools create",
            "gh release create",
            "VERSION=$(cat VERSION)",
        ),
    }
    for file_name, fragments in required_fragments.items():
        for fragment in fragments:
            if fragment not in texts[file_name]:
                raise ResolverError(
                    f"workflow_contract_missing file={file_name} fragment={fragment}"
                )
    return {"workflows": len(workflows), "external_actions": action_count, "checkouts": checkout_count}


def _load_fixture(path: str | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ResolverError("fixture must be a JSON object")
    return value


def _fetcher_from_argument(path: str | None) -> Callable[[str, int], dict[str, Any]]:
    fixture = _load_fixture(path)
    return live_fetcher if fixture is None else fixture_fetcher(fixture)


def _emit_github_output(values: Mapping[str, str]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        raise ResolverError("GITHUB_OUTPUT is required")
    with Path(output_path).open("a", encoding="utf-8") as stream:
        for name, value in values.items():
            stream.write(f"{name}={value}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)

    check = commands.add_parser("check")
    check.add_argument("--offline-fixture")

    update = commands.add_parser("update")
    update.add_argument("--offline-fixture")

    validate = commands.add_parser("validate")
    validate.add_argument("--base-ref")
    validate.add_argument("--automation-diff", action="store_true")

    arguments = commands.add_parser("build-args")
    arguments.add_argument("--revision")
    arguments.add_argument("--version")
    arguments.add_argument("--format", choices=("json", "github-output"), default="json")

    actions = commands.add_parser("validate-actions")
    actions.add_argument("directory", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        activate_target_root(arguments.root)
        if arguments.command == "check":
            current = parse_env_bytes(ENV_PATH.read_bytes())
            result = resolve_versions(current, _fetcher_from_argument(arguments.offline_fixture))
        elif arguments.command == "update":
            result = update_files(
                ENV_PATH,
                VERSION_PATH,
                _fetcher_from_argument(arguments.offline_fixture),
            )
        elif arguments.command == "validate":
            result = validate_repository(
                arguments.base_ref, automation_diff=arguments.automation_diff
            )
        elif arguments.command == "build-args":
            result = build_args(
                ENV_PATH,
                VERSION_PATH,
                arguments.revision,
                version=arguments.version,
            )
            if arguments.format == "github-output":
                _emit_github_output(result)
                return 0
        elif arguments.command == "validate-actions":
            result = validate_actions(arguments.directory)
        else:
            raise AssertionError(f"unhandled command: {arguments.command}")
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (ResolverError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

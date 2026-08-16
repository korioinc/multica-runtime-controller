#!/usr/bin/env python3
"""Resolve runtime versions and enforce release/workflow policy.

The module intentionally uses only the Python standard library so the same
owner logic runs locally and on a stock GitHub-hosted runner.
"""

from __future__ import annotations

import argparse
import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest


REPOSITORY = "korioinc/multica-runtime-controller"
IMAGE_REPOSITORY = REPOSITORY
IMAGE_REFERENCE = f"ghcr.io/{IMAGE_REPOSITORY}"
IMAGE_SOURCE = f"https://github.com/{REPOSITORY}"
ENV_PATH = Path("build/runtime-versions.env")
VERSION_PATH = Path("VERSION")
REQUESTED_FIELDS = (
    "MULTICA_CLI_VERSION",
    "CODEX_VERSION",
    "PI_VERSION",
)
FIELD_SOURCES = {
    "MULTICA_CLI_VERSION": "multica",
    "CODEX_VERSION": "codex",
    "PI_VERSION": "pi",
}
SOURCE_URLS = {
    "multica": "https://api.github.com/repos/multica-ai/multica/releases/latest",
    "codex": "https://registry.npmjs.org/-/package/@openai%2Fcodex/dist-tags",
    "pi": "https://registry.npmjs.org/-/package/@earendil-works%2Fpi-coding-agent/dist-tags",
}
SEMVER_PATTERN = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
ASSIGNMENT_PATTERN = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")
REGISTRY_STABLE_TAG_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
ACTION_USES_PATTERN = re.compile(r"^\s*uses:\s*([^\s#]+)(?:\s+#\s*(\S.*))?$")


class ResolverError(RuntimeError):
    """A version source or repository policy failed closed."""


class ReleaseConflict(RuntimeError):
    """An immutable release identity conflicts with requested state."""


@dataclass(frozen=True, order=True)
class SemVer:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: str, *, field: str = "version") -> "SemVer":
        match = SEMVER_PATTERN.fullmatch(value)
        if not match:
            raise ResolverError(f"invalid_stable_semver field={field} value={value!r}")
        return cls(*(int(part) for part in match.groups()))

    def next_patch(self) -> "SemVer":
        return SemVer(self.major, self.minor, self.patch + 1)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


def _run(*command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def activate_target_root(path: Path) -> Path:
    """Make every relative file and git operation use one explicit repository root."""
    if not path.is_absolute():
        raise ResolverError("target_root_not_absolute")
    root = path.resolve(strict=True)
    if not root.is_dir():
        raise ResolverError(f"target_root_not_directory path={root}")
    os.chdir(root)
    return root


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
        if not match:
            raise ResolverError(f"invalid_assignment line={line_number}")
        name, value = match.groups()
        if name in assignments:
            raise ResolverError(f"duplicate_assignment field={name}")
        if not value or value != value.strip():
            raise ResolverError(f"invalid_assignment_value field={name}")
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
            return json.loads(json.dumps(data[source]))
        except KeyError as exc:
            raise ResolverError(f"fixture_missing_source source={source}") from exc

    return fetch


def _http_json(url: str, *, source: str, timeout: float = 15.0) -> Any:
    headers = {
        "Accept": "application/vnd.github+json" if "api.github.com" in url else "application/json",
        "User-Agent": "multica-runtime-controller-version-resolver/1",
    }
    token = os.environ.get("GH_TOKEN")
    if token and "api.github.com" in url:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    request = urlrequest.Request(url, headers=headers)
    with urlrequest.urlopen(request, timeout=timeout) as response:
        payload = response.read()
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ResolverError(f"malformed_json source={source}") from exc
    return value


def live_fetcher(source: str, _attempt: int) -> dict[str, Any]:
    payload = _http_json(SOURCE_URLS[source], source=source)
    if not isinstance(payload, dict):
        raise ResolverError(f"malformed_schema source={source}")
    if source in {"codex", "pi"}:
        return {"dist-tags": payload}
    return payload


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
            if 400 <= exc.code < 500:
                status = exc.code
                exc.close()
                raise ResolverError(f"http_4xx source={source} status={status}") from exc
            if not 500 <= exc.code < 600:
                status = exc.code
                exc.close()
                raise ResolverError(f"http_error source={source} status={status}") from exc
            status = exc.code
            exc.close()
            terminal = ResolverError(f"http_5xx source={source} status={status} attempts={attempt}")
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
    valid_assets = {
        asset.get("name")
        for asset in assets
        if isinstance(asset, dict)
        and isinstance(asset.get("size"), int)
        and asset["size"] > 0
        and asset.get("state") == "uploaded"
    }
    missing = expected - valid_assets
    if missing:
        raise ResolverError(f"multica_missing_assets names={','.join(sorted(missing))}")
    return version


def _parse_codex(payload: Mapping[str, Any]) -> SemVer:
    tags = payload.get("dist-tags")
    if not isinstance(tags, dict):
        raise ResolverError("codex_dist_tags_schema")
    latest_raw = tags.get("latest")
    if not isinstance(latest_raw, str):
        raise ResolverError("codex_latest_schema")
    latest = SemVer.parse(latest_raw, field="CODEX_VERSION")
    expected = {
        "linux-x64": f"{latest}-linux-x64",
        "linux-arm64": f"{latest}-linux-arm64",
    }
    for name, value in expected.items():
        if tags.get(name) != value:
            raise ResolverError(f"codex_dist_tag_mismatch tag={name}")
    return latest


def _parse_pi(payload: Mapping[str, Any]) -> SemVer:
    tags = payload.get("dist-tags")
    if not isinstance(tags, dict) or not isinstance(tags.get("latest"), str):
        raise ResolverError("pi_dist_tags_schema")
    return SemVer.parse(tags["latest"], field="PI_VERSION")


def resolve_versions(
    current: Mapping[str, str],
    fetch: Callable[[str, int], dict[str, Any]] = live_fetcher,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    parsers = {"multica": _parse_multica, "codex": _parse_codex, "pi": _parse_pi}
    latest: dict[str, str] = {}
    updates: list[dict[str, str]] = []
    for field in REQUESTED_FIELDS:
        source = FIELD_SOURCES[field]
        payload = _fetch_with_retry(source, fetch, sleep)
        newest = parsers[source](payload)
        existing = SemVer.parse(current[field], field=field)
        if newest < existing:
            raise ResolverError(f"downgrade field={field} current={existing} latest={newest}")
        latest[field] = str(newest)
        if newest > existing:
            updates.append({"field": field, "from": str(existing), "to": str(newest)})
    return {
        "current": {field: current[field] for field in REQUESTED_FIELDS},
        "latest": latest,
        "updates": updates,
    }


def _render_env(original: bytes, latest: Mapping[str, str]) -> bytes:
    newline = b"\n" if original.endswith(b"\n") else b""
    lines = original.decode("utf-8").splitlines()
    rendered: list[str] = []
    replaced: set[str] = set()
    for line in lines:
        match = ASSIGNMENT_PATTERN.fullmatch(line)
        if match and match.group(1) in latest:
            field = match.group(1)
            rendered.append(f"{field}={latest[field]}")
            replaced.add(field)
        else:
            rendered.append(line)
    if replaced != set(REQUESTED_FIELDS):
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
    current = parse_env_bytes(original_env)
    version = read_version(version_path)
    result = resolve_versions(current, fetch)
    if not result["updates"]:
        result["release"] = {"from": str(version), "to": str(version)}
        return result
    next_version = version.next_patch()
    next_env = _render_env(original_env, result["latest"])
    next_version_bytes = f"{next_version}\n".encode("ascii")
    parse_env_bytes(next_env)
    SemVer.parse(next_version_bytes.decode("ascii").strip(), field="VERSION")
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
    result["release"] = {"from": str(version), "to": str(next_version)}
    return result


def build_args(env_path: Path, version_path: Path, revision: str | None = None) -> dict[str, str]:
    assignments = parse_env_bytes(env_path.read_bytes())
    version = read_version(version_path)
    if revision is None:
        revision = _run("git", "rev-parse", "HEAD").stdout.strip()
    if not REVISION_PATTERN.fullmatch(revision):
        raise ResolverError("COMMIT must be a full 40-hex revision")
    return assignments | {"VERSION": str(version), "COMMIT": revision}


def validate_repository(base_ref: str | None, *, automation_diff: bool = False) -> dict[str, Any]:
    current_env = parse_env_bytes(ENV_PATH.read_bytes())
    current_version = read_version(VERSION_PATH)
    result: dict[str, Any] = {
        "version": str(current_version),
        "runtime_fields": len(current_env),
        "base_ref": base_ref,
    }
    if base_ref is None:
        return result
    base_env_result = _run("git", "show", f"{base_ref}:{ENV_PATH}", check=False)
    base_version_result = _run("git", "show", f"{base_ref}:{VERSION_PATH}", check=False)
    changed_fields: list[str] = []
    if base_env_result.returncode == 0:
        base_env = parse_env_bytes(base_env_result.stdout.encode("utf-8"))
        changed_fields = [
            field
            for field in sorted(set(base_env) | set(current_env))
            if base_env.get(field) != current_env.get(field)
        ]
        unexpected = set(changed_fields) - set(REQUESTED_FIELDS)
        if unexpected:
            raise ResolverError(f"unexpected_runtime_fields fields={','.join(sorted(unexpected))}")
        for field in changed_fields:
            if SemVer.parse(current_env[field], field=field) <= SemVer.parse(base_env[field], field=field):
                raise ResolverError(f"runtime_version_not_increased field={field}")
    if changed_fields:
        if base_version_result.returncode != 0:
            raise ResolverError("base_VERSION_missing_for_runtime_update")
        base_version = SemVer.parse(base_version_result.stdout.strip(), field="base_VERSION")
        if current_version != base_version.next_patch():
            raise ResolverError(
                f"VERSION_patch_mismatch base={base_version} current={current_version}"
            )
    if automation_diff:
        names = {
            line
            for line in _run("git", "diff", "--name-only", base_ref, "--").stdout.splitlines()
            if line
        }
        expected = {str(VERSION_PATH), str(ENV_PATH)}
        if names != expected:
            raise ResolverError(
                f"automation_diff_mismatch expected={sorted(expected)} actual={sorted(names)}"
            )
        if not changed_fields:
            raise ResolverError("automation_diff_has_no_runtime_update")
    result["changed_fields"] = changed_fields
    return result


def _stable_versions(surfaces: Mapping[str, Any]) -> list[SemVer]:
    values: list[SemVer] = []
    for raw in surfaces.get("git_tags", []):
        if isinstance(raw, str):
            candidate = raw[1:] if raw.startswith("v") else raw
            if SEMVER_PATTERN.fullmatch(candidate):
                values.append(SemVer.parse(candidate))
    for release in surfaces.get("github_releases", []):
        raw = release.get("tag_name") if isinstance(release, dict) else release
        if isinstance(raw, str):
            candidate = raw[1:] if raw.startswith("v") else raw
            if SEMVER_PATTERN.fullmatch(candidate):
                values.append(SemVer.parse(candidate))
    for raw in surfaces.get("registry_tags", []):
        name = raw.get("name") if isinstance(raw, dict) else raw
        if isinstance(name, str) and REGISTRY_STABLE_TAG_PATTERN.fullmatch(name):
            values.append(SemVer.parse(name))
    return values


def _check_revision(value: Any, expected: str, kind: str) -> None:
    if not isinstance(value, dict) or value.get("revision") != expected:
        raise ReleaseConflict(f"{kind}_identity_mismatch")


def _check_digest(value: Mapping[str, Any], expected: str, kind: str) -> None:
    if value.get("digest") != expected:
        raise ReleaseConflict(f"{kind}_digest_mismatch")


def plan_release_state(
    surfaces: Mapping[str, Any], version: str, revision: str
) -> dict[str, str]:
    parsed_version = SemVer.parse(version, field="release_version")
    if not REVISION_PATTERN.fullmatch(revision):
        raise ReleaseConflict("revision_not_full_sha")
    image = surfaces.get("version_image")
    tag = surfaces.get("git_tag")
    release = surfaces.get("github_release")
    latest = surfaces.get("latest")
    if image is None:
        if tag is not None or release is not None:
            raise ReleaseConflict("release_without_candidate")
        if latest is not None and latest.get("version") == str(parsed_version):
            raise ReleaseConflict("latest_without_candidate")
        return {"state": "absent", "next_action": "build_candidate"}
    if not isinstance(image, dict):
        raise ReleaseConflict("candidate_schema")
    if image.get("revision") != revision or image.get("version") != str(parsed_version):
        raise ReleaseConflict("candidate_identity_mismatch")
    if image.get("source") != IMAGE_SOURCE:
        raise ReleaseConflict("candidate_source_mismatch")
    digest = image.get("digest")
    if not isinstance(digest, str) or not DIGEST_PATTERN.fullmatch(digest):
        raise ReleaseConflict("candidate_digest_invalid")
    platforms = set(image.get("platforms", []))
    if platforms != {"linux/amd64", "linux/arm64"}:
        raise ReleaseConflict("candidate_platform_mismatch")
    package = surfaces.get("package")
    if isinstance(package, dict) and (
        package.get("name") != "multica-runtime-controller"
        or package.get("package_type") != "container"
    ):
        raise ReleaseConflict("package_identity_mismatch")
    if not isinstance(package, dict) or package.get("visibility") != "public":
        return {
            "state": "candidate_private",
            "next_action": "publish_package_visibility",
            "digest": digest,
        }
    if tag is not None:
        _check_revision(tag, revision, "tag")
    if release is None:
        if latest is not None and latest.get("digest") == digest:
            raise ReleaseConflict("latest_before_release")
        return {
            "state": "candidate_verified",
            "next_action": "finalize_release",
            "digest": digest,
        }
    if tag is None:
        raise ReleaseConflict("release_without_tag")
    _check_revision(release, revision, "release")
    _check_digest(release, digest, "release")
    if latest is not None and latest.get("digest") == digest:
        return {
            "state": "latest_digest_matched",
            "next_action": "already_published",
            "digest": digest,
        }
    return {
        "state": "release_verified",
        "next_action": "promote_latest",
        "digest": digest,
    }


def preflight_release(
    surfaces: Mapping[str, Any],
    version: str,
    revision: str,
    *,
    require_absent: bool,
    allow_same_identity: bool,
) -> dict[str, Any]:
    if require_absent == allow_same_identity:
        raise ReleaseConflict("preflight_mode_requires_exactly_one_choice")
    requested = SemVer.parse(version, field="release_version")
    versions = _stable_versions(surfaces)
    maximum = max(versions) if versions else None
    state = plan_release_state(surfaces, version, revision)
    if maximum is not None and maximum > requested:
        raise ReleaseConflict(f"external_maximum_ahead next_patch={maximum.next_patch()}")
    if require_absent:
        if state["state"] != "absent" or (maximum is not None and maximum >= requested):
            raise ReleaseConflict("release_version_already_exists")
    else:
        if state["state"] == "absent":
            raise ReleaseConflict("recovery_identity_absent")
        if maximum is not None and maximum != requested:
            raise ReleaseConflict("recovery_version_not_live_maximum")
    return {
        "version": str(requested),
        "revision": revision,
        "maximum": str(maximum) if maximum is not None else None,
        "next_patch": str(maximum.next_patch()) if maximum is not None else "0.0.1",
        **state,
    }


def validate_revision(version: str, revision: str, *, require_main_ancestor: bool) -> None:
    SemVer.parse(version, field="release_version")
    if not REVISION_PATTERN.fullmatch(revision):
        raise ReleaseConflict("revision_not_full_sha")
    if _run("git", "cat-file", "-e", f"{revision}^{{commit}}", check=False).returncode != 0:
        raise ReleaseConflict("revision_not_found")
    version_result = _run("git", "show", f"{revision}:VERSION", check=False)
    if version_result.returncode != 0 or version_result.stdout != f"{version}\n":
        raise ReleaseConflict("VERSION_at_revision_mismatch")
    if require_main_ancestor:
        main_ref = "origin/main" if _run("git", "show-ref", "--verify", "--quiet", "refs/remotes/origin/main", check=False).returncode == 0 else "main"
        if _run("git", "merge-base", "--is-ancestor", revision, main_ref, check=False).returncode != 0:
            raise ReleaseConflict("revision_not_main_ancestor")


def _github_json(path: str, *, missing_ok: bool = False) -> Any:
    url = f"https://api.github.com/repos/{REPOSITORY}/{path.lstrip('/')}"
    try:
        return _http_json(url, source="github_release")
    except urlerror.HTTPError as exc:
        if missing_ok and exc.code == 404:
            return None
        raise


def _github_paginated_releases() -> list[dict[str, Any]]:
    releases: list[dict[str, Any]] = []
    page = 1
    while True:
        payload = _github_json(f"releases?per_page=100&page={page}")
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise ReleaseConflict("github_releases_schema")
        releases.extend(payload)
        if len(payload) < 100:
            return releases
        page += 1


def _github_package() -> dict[str, Any] | None:
    url = (
        "https://api.github.com/orgs/korioinc/packages/container/"
        "multica-runtime-controller"
    )
    try:
        payload = _http_json(url, source="github_package")
    except urlerror.HTTPError as exc:
        if exc.code == 404:
            exc.close()
            return None
        raise
    if not isinstance(payload, dict):
        raise ReleaseConflict("github_package_schema")
    return payload


def _ghcr_tags() -> list[str]:
    path = "tags/list?n=100"
    tags: list[str] = []
    cursors: set[str] = set()
    while path:
        try:
            raw, _ = _registry_request(path, accept="application/json")
        except urlerror.HTTPError as exc:
            if exc.code == 404:
                exc.close()
                return []
            raise
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ReleaseConflict("ghcr_tag_schema")
        page = payload.get("tags")
        if page is None:
            page = []
        if not isinstance(page, list) or not all(isinstance(tag, str) for tag in page):
            raise ReleaseConflict("ghcr_tag_schema")
        tags.extend(page)
        if len(page) < 100:
            break
        cursor = page[-1]
        if cursor in cursors:
            raise ReleaseConflict("ghcr_tag_pagination_stalled")
        cursors.add(cursor)
        path = f"tags/list?n=100&last={urlparse.quote(cursor, safe='')}"
    return tags


def _registry_request(path: str, *, accept: str | None = None) -> tuple[bytes, Mapping[str, str]]:
    token_url = (
        "https://ghcr.io/token?service=ghcr.io&scope="
        + urlparse.quote(f"repository:{IMAGE_REPOSITORY}:pull", safe=":")
    )
    token_headers = {"Accept": "application/json", "User-Agent": "multica-release-verifier/1"}
    actor = os.environ.get("GITHUB_ACTOR")
    github_token = os.environ.get("GH_TOKEN")
    if actor and github_token:
        credentials = base64.b64encode(f"{actor}:{github_token}".encode()).decode("ascii")
        token_headers["Authorization"] = f"Basic {credentials}"
    token_request = urlrequest.Request(token_url, headers=token_headers)
    with urlrequest.urlopen(token_request, timeout=30) as response:
        token_payload = json.loads(response.read())
    if not isinstance(token_payload, dict):
        raise ReleaseConflict("ghcr_registry_token_schema")
    token = token_payload.get("token")
    if not isinstance(token, str):
        raise ReleaseConflict("ghcr_registry_token_schema")
    headers = {"Authorization": f"Bearer {token}", "User-Agent": "multica-release-verifier/1"}
    if accept:
        headers["Accept"] = accept
    request = urlrequest.Request(
        f"https://ghcr.io/v2/{IMAGE_REPOSITORY}/{path}", headers=headers
    )
    with urlrequest.urlopen(request, timeout=30) as response:
        return response.read(), dict(response.headers.items())


def _registry_image(tag: str) -> dict[str, Any] | None:
    accepts = ", ".join(
        (
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
    )
    try:
        raw, headers = _registry_request(f"manifests/{tag}", accept=accepts)
    except urlerror.HTTPError as exc:
        if exc.code == 404:
            exc.close()
            return None
        raise
    payload = json.loads(raw)
    digest = headers.get("Docker-Content-Digest") or headers.get("docker-content-digest")
    if not isinstance(digest, str):
        digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    descriptors = payload.get("manifests")
    if not isinstance(descriptors, list):
        descriptors = [{"digest": digest, "platform": {}}]
    platforms: set[str] = set()
    revisions: set[str] = set()
    versions: set[str] = set()
    sources: set[str] = set()
    for descriptor in descriptors:
        platform = descriptor.get("platform", {})
        architecture = platform.get("architecture")
        operating_system = platform.get("os")
        if operating_system and architecture and architecture != "unknown":
            platforms.add(f"{operating_system}/{architecture}")
        descriptor_digest = descriptor.get("digest")
        if not isinstance(descriptor_digest, str):
            raise ReleaseConflict("registry_descriptor_digest_missing")
        manifest_raw, _ = _registry_request(f"manifests/{descriptor_digest}", accept=accepts)
        manifest = json.loads(manifest_raw)
        config_digest = manifest.get("config", {}).get("digest")
        if not isinstance(config_digest, str):
            raise ReleaseConflict("registry_config_digest_missing")
        config_raw, _ = _registry_request(f"blobs/{config_digest}")
        config = json.loads(config_raw)
        labels = config.get("config", {}).get("Labels") or {}
        revision = labels.get("org.opencontainers.image.revision")
        version = labels.get("org.opencontainers.image.version")
        source = labels.get("org.opencontainers.image.source")
        if isinstance(revision, str):
            revisions.add(revision)
        if isinstance(version, str):
            versions.add(version)
        if isinstance(source, str):
            sources.add(source)
    return {
        "digest": digest,
        "revision": next(iter(revisions)) if len(revisions) == 1 else None,
        "version": next(iter(versions)) if len(versions) == 1 else None,
        "platforms": sorted(platforms),
        "source": next(iter(sources)) if len(sources) == 1 else None,
    }


def _release_digest(body: str | None) -> str | None:
    if not body:
        return None
    match = re.search(r"(?im)^Manifest digest:\s*`?(sha256:[0-9a-f]{64})`?\s*$", body)
    return match.group(1) if match else None


def live_release_surfaces(version: str) -> dict[str, Any]:
    git_tags = _run("git", "tag", "--list").stdout.splitlines()
    releases_payload = _github_paginated_releases()
    release_tags = [release.get("tag_name") for release in releases_payload]
    package_payload = _github_package()
    package = None
    if isinstance(package_payload, dict):
        package = {
            "name": package_payload.get("name"),
            "package_type": package_payload.get("package_type"),
            "visibility": package_payload.get("visibility"),
        }
    registry_tags = _ghcr_tags() if package is not None else []
    version_image = _registry_image(version) if version in registry_tags else None
    latest_image = _registry_image("latest") if "latest" in registry_tags else None
    tag_name = f"v{version}"
    tag_revision_result = _run("git", "rev-list", "-n", "1", tag_name, check=False)
    tag_object = None
    if tag_revision_result.returncode == 0 and tag_revision_result.stdout.strip():
        tag_object = {"revision": tag_revision_result.stdout.strip(), "tag_name": tag_name}
    release_payload = _github_json(f"releases/tags/{tag_name}", missing_ok=True)
    release_object = None
    if release_payload is not None:
        release_object = {
            "tag_name": tag_name,
            "revision": tag_object.get("revision") if tag_object else None,
            "digest": _release_digest(release_payload.get("body")),
        }
    return {
        "git_tags": git_tags,
        "github_releases": [{"tag_name": tag} for tag in release_tags if isinstance(tag, str)],
        "registry_tags": registry_tags,
        "package": package,
        "version_image": version_image,
        "git_tag": tag_object,
        "github_release": release_object,
        "latest": latest_image,
    }


def validate_actions(directory: Path) -> dict[str, Any]:
    workflows = sorted((*directory.glob("*.yml"), *directory.glob("*.yaml")))
    if not workflows:
        raise ResolverError("no_workflows_found")
    action_count = 0
    checkout_count = 0
    workflow_texts: dict[str, str] = {}
    workflow_jobs: dict[str, dict[str, str]] = {}

    def permissions_for(block: str) -> dict[str, str]:
        marker = re.search(r"(?m)^    permissions:(?:\s*\{\})?\s*$", block)
        if marker is None:
            return {}
        permissions: dict[str, str] = {}
        for line in block[marker.end() :].splitlines():
            if line and len(line) - len(line.lstrip()) <= 4:
                break
            match = re.fullmatch(r"\s{6}([a-z-]+):\s*(read|write|none)\s*", line)
            if match:
                permissions[match.group(1)] = match.group(2)
        return permissions

    def require_fragments(file_name: str, text: str, fragments: tuple[str, ...]) -> None:
        for fragment in fragments:
            if fragment not in text:
                raise ResolverError(
                    f"workflow_contract_missing file={file_name} fragment={fragment}"
                )

    for path in workflows:
        text = path.read_text(encoding="utf-8")
        workflow_texts[path.name] = text
        if re.search(r"DOCKERHUB_|docker\.io/jskorlol/multica-runtime-controller", text):
            raise ResolverError(f"legacy_registry_reference file={path}")
        if not re.search(r"(?m)^permissions:\s*\{\}\s*$", text):
            raise ResolverError(f"workflow_top_permissions_not_empty file={path}")
        if re.search(r"(?m)^\s*pull_request_target\s*:", text):
            raise ResolverError(f"forbidden_event file={path} event=pull_request_target")
        if re.search(r"(?m)^\s*(?:set\s+-x|printenv)(?:\s|$)", text):
            raise ResolverError(f"forbidden_debug_command file={path}")
        lines = text.splitlines()
        for index, line in enumerate(lines):
            match = ACTION_USES_PATTERN.match(line)
            if not match:
                continue
            reference, comment = match.groups()
            if reference.startswith("./"):
                continue
            action_count += 1
            if "@" not in reference:
                raise ResolverError(f"action_missing_ref file={path} line={index + 1}")
            action, revision = reference.rsplit("@", 1)
            if not re.fullmatch(r"[0-9a-f]{40}", revision):
                raise ResolverError(f"action_not_full_sha file={path} action={action}")
            if not comment or not re.search(r"v[0-9]", comment):
                raise ResolverError(f"action_version_comment_missing file={path} action={action}")
            if action == "actions/checkout":
                checkout_count += 1
                indentation = len(line) - len(line.lstrip())
                step_lines: list[str] = []
                for candidate in lines[index + 1 :]:
                    candidate_indent = len(candidate) - len(candidate.lstrip())
                    if candidate.strip().startswith("- ") and candidate_indent == indentation:
                        break
                    step_lines.append(candidate)
                step = "\n".join(step_lines)
                if not re.search(r"(?m)^\s+persist-credentials:\s*false\s*$", step):
                    raise ResolverError(f"checkout_persists_credentials file={path} line={index + 1}")
        jobs_match = re.search(r"(?m)^jobs:\s*$", text)
        if not jobs_match:
            raise ResolverError(f"workflow_jobs_missing file={path}")
        job_text = text[jobs_match.end() :]
        job_matches = list(re.finditer(r"(?m)^  ([A-Za-z0-9_-]+):\s*$", job_text))
        if not job_matches:
            raise ResolverError(f"workflow_job_definitions_missing file={path}")
        blocks: dict[str, str] = {}
        for job_index, job_match in enumerate(job_matches):
            end = job_matches[job_index + 1].start() if job_index + 1 < len(job_matches) else len(job_text)
            block = job_text[job_match.start() : end]
            job_name = job_match.group(1)
            blocks[job_name] = block
            if not re.search(r"(?m)^    permissions:(?:\s*\{\})?\s*$", block):
                raise ResolverError(f"job_permissions_missing file={path} job={job_name}")
        workflow_jobs[path.name] = blocks
        if re.search(r"(?m)^  workflow_dispatch:\s*$", text):
            raise ResolverError(
                f"privileged_ref_selectable_dispatch_forbidden file={path}"
            )
        if re.search(r"(?m)^  pull_request:\s*$", text):
            if "secrets." in text or re.search(r"(?m)^\s+environment:\s*", text):
                raise ResolverError(f"pull_request_workflow_is_privileged file={path}")

    required_workflows = {
        "ci.yml",
        "create-develop-to-main-pr.yml",
        "runtime-version-update.yml",
        "runtime-version-auto-merge.yml",
    }
    missing = sorted(required_workflows - set(workflow_texts))
    if missing:
        raise ResolverError(f"required_workflow_missing files={','.join(missing)}")

    ci = workflow_texts["ci.yml"]
    if "docker/setup-qemu-action" in ci or "platforms: linux/amd64,linux/arm64" in ci:
        raise ResolverError("ci_native_platform_build_required")
    ci_on = ci[ci.index("on:") : ci.index("permissions:")]
    if re.search(r"(?m)^\s+paths(?:-ignore)?:", ci_on):
        raise ResolverError("ci_pull_request_paths_filter_forbidden")
    require_fragments(
        "ci.yml",
        ci,
        (
            "  pull_request:",
            "  repository_dispatch:",
            "types: [automation-ci]",
            "runtime-update",
            "release-patch",
            "sync-main",
            "promotion",
            ".commits >= 2",
            ".ahead_by >= 2",
            '.base.ref == "develop"',
            '.head.ref == "develop"',
            "EVENT_SHA: ${{ github.sha }}",
            "EVENT_REF: ${{ github.ref }}",
            "INPUT_HEAD_SHA: ${{ github.event.client_payload.head_sha }}",
            '[ "$EVENT_REF" = refs/heads/main ]',
            '[ "$EVENT_SHA" = "$live_main" ]',
            "CI automation {0} PR-{1} SHA-{2}",
            "automation-verify",
            "automation-runtime-image",
            "runtime-image-build:",
            "architecture: amd64",
            "runner: ubuntu-24.04",
            "platform: linux/amd64",
            "architecture: arm64",
            "runner: ubuntu-24.04-arm",
            "platform: linux/arm64",
            "runs-on: ${{ matrix.runner }}",
            "platforms: ${{ matrix.platform }}",
            "scope=runtime-main-${{ matrix.architecture }}",
            "scope=${{ steps.scope.outputs.cache_scope }}-${{ matrix.architecture }}",
            '"repos/$GITHUB_REPOSITORY/check-runs"',
            "head_sha: $head",
            "ref: ${{ needs.context.outputs.checkout_sha }}",
            "runtime-pr-",
            'git fetch --no-tags origin "$BASE_SHA"',
            "scripts/install-runtime-tools.sh scripts/runtime-entrypoint.sh",
            "scripts/verify-runtime-tools.sh src",
            "dispatch-merge:",
            "needs: [context, verify, runtime-image, publish-checks]",
            "needs.context.outputs.automation_kind != 'promotion'",
            "SOURCE_RUN_ID: ${{ github.run_id }}",
            "SOURCE_RUN_ATTEMPT: ${{ github.run_attempt }}",
            '{event_type:"automation-merge",client_payload:',
            '"repos/$GITHUB_REPOSITORY/dispatches"',
        ),
    )
    expected_ci_permissions = {
        "context": {"contents": "read", "pull-requests": "read"},
        "verify": {"contents": "read"},
        "runtime-image-build": {"contents": "read"},
        "runtime-image": {},
        "publish-checks": {
            "checks": "write",
            "contents": "read",
            "pull-requests": "read",
        },
        "dispatch-merge": {"contents": "write"},
    }
    for job_name, expected in expected_ci_permissions.items():
        block = workflow_jobs["ci.yml"].get(job_name)
        if block is None:
            raise ResolverError(f"ci_job_missing job={job_name}")
        actual = permissions_for(block)
        if actual != expected:
            raise ResolverError(
                f"ci_job_permissions_mismatch job={job_name} expected={expected} actual={actual}"
            )

    updater = workflow_texts["runtime-version-update.yml"]
    for forbidden in (
        "AUTOMATION_APP_",
        "actions/create-github-app-token",
        "secrets.",
        "environment:",
        "gh pr merge",
        "python3 scripts/runtime_versions.py",
    ):
        if forbidden in updater:
            raise ResolverError(
                f"updater_forbidden_fragment fragment={forbidden}"
            )
    require_fragments(
        "runtime-version-update.yml",
        updater,
        (
            "cron: '0 */4 * * *'",
            "types: [runtime-version-update]",
            "vars.RELEASE_AUTOMATION_ENABLED == 'true'",
            "github.token",
            "automation/runtime-versions",
            "--base develop",
            'kind:"runtime-update"',
            '{event_type:"automation-ci",client_payload:{kind:"runtime-update"',
            '"repos/$GITHUB_REPOSITORY/dispatches"',
            "pull_request_number",
            "head_sha",
            "TRUSTED_RESOLVER: ${{ runner.temp }}/trusted-runtime-versions.py",
            "EVENT_WORKFLOW_SHA: ${{ github.sha }}",
            'git show "$live_main:scripts/runtime_versions.py"',
            'git hash-object "$TRUSTED_RESOLVER"',
            "TARGET_ROOT: ${{ github.workspace }}",
            'python3 -I "$TRUSTED_RESOLVER" --root "$TARGET_ROOT"',
        ),
    )
    if updater.count('python3 -I "$TRUSTED_RESOLVER" --root "$TARGET_ROOT"') != 7:
        raise ResolverError("updater_trusted_resolver_call_count_mismatch")
    propose = workflow_jobs["runtime-version-update.yml"].get("propose")
    if propose is None:
        raise ResolverError("updater_job_missing job=propose")
    expected_propose = {
        "contents": "write",
        "pull-requests": "write",
    }
    actual_propose = permissions_for(propose)
    if actual_propose != expected_propose:
        raise ResolverError(
            f"updater_job_permissions_mismatch expected={expected_propose} actual={actual_propose}"
        )

    merge_workflow = workflow_texts["runtime-version-auto-merge.yml"]
    for forbidden in (
        "uses:",
        "secrets.",
        "environment:",
        "actions/checkout",
        "download-artifact",
        "actions/cache",
        "git checkout",
        "make ",
        "./scripts/",
        "actions/workflows/release.yml/dispatches",
        "workflow_run:",
    ):
        if forbidden in merge_workflow:
            raise ResolverError(
                f"merge_workflow_forbidden_fragment fragment={forbidden}"
            )
    require_fragments(
        "runtime-version-auto-merge.yml",
        merge_workflow,
        (
            "  repository_dispatch:",
            "types: [automation-merge]",
            "vars.RELEASE_AUTOMATION_ENABLED == 'true'",
            "github.event.client_payload.kind == 'runtime-update'",
            "github.event.client_payload.kind == 'release-patch'",
            "github.event.client_payload.kind == 'sync-main'",
            "EVENT_WORKFLOW_SHA",
            "INPUT_WORKFLOW_SHA",
            "INPUT_PULL_REQUEST_NUMBER",
            "INPUT_HEAD_SHA",
            "display_title",
            '.name == $title',
            "CI automation",
            "automation/runtime-versions",
            "automation/release-patch",
            "automation/sync-main",
            '$kind == "sync-main" and .commits >= 2',
            '$kind != "sync-main" and .commits == 1',
            "github.event.client_payload.run_id",
            "github.event.client_payload.run_attempt",
            '[ "$EVENT_NAME" = repository_dispatch ]',
            '[ "$EVENT_REF" = refs/heads/main ]',
            '[ "$EVENT_WORKFLOW_SHA" = "$INPUT_WORKFLOW_SHA" ]',
            'expected_title="CI automation $INPUT_KIND PR-$INPUT_PULL_REQUEST_NUMBER SHA-$INPUT_HEAD_SHA"',
            ".github/workflows/ci.yml",
            "triggering_actor",
            "attempts/$RUN_ATTEMPT/jobs",
            '["automation-runtime-image","automation-runtime-image-amd64","automation-runtime-image-arm64","automation-verify","context","dispatch-merge","publish-checks"]',
            "source CI run %s did not complete before the merge deadline",
            "--match-head-commit",
            "compare/develop...$automation_head",
            "--merge --delete-branch",
            "--squash --delete-branch",
            'event == "repository_dispatch"',
            '{event_type:"reconcile-develop",client_payload:{revision:$revision}}',
            '"repos/$GITHUB_REPOSITORY/dispatches"',
        ),
    )
    expected_merge_permissions = {
        "actions": "read",
        "contents": "write",
        "pull-requests": "write",
    }
    expected_dispatch_permissions = {"contents": "write"}
    for job_name, expected in (
        ("merge", expected_merge_permissions),
        ("dispatch-develop", expected_dispatch_permissions),
    ):
        block = workflow_jobs["runtime-version-auto-merge.yml"].get(job_name)
        if block is None:
            raise ResolverError(f"merge_workflow_job_missing job={job_name}")
        actual = permissions_for(block)
        if actual != expected:
            raise ResolverError(
                f"merge_workflow_permissions_mismatch job={job_name} "
                f"expected={expected} actual={actual}"
            )

    promotion = workflow_texts["create-develop-to-main-pr.yml"]
    for forbidden in (
        "HEAD:refs/heads/develop",
        "git merge origin/main",
        "--head \"$GITHUB_REPOSITORY_OWNER:develop\"",
        "cancel-in-progress: true",
    ):
        if forbidden in promotion:
            raise ResolverError(
                f"promotion_workflow_forbidden_fragment fragment={forbidden}"
            )
    require_fragments(
        "create-develop-to-main-pr.yml",
        promotion,
        (
            "branches: [develop]",
            "branches: [main]",
            "vars.RELEASE_AUTOMATION_ENABLED == 'true'",
            "cron: '0 * * * *'",
            "  repository_dispatch:",
            "types: [reconcile-develop]",
            "FORCE_RELEASE: ${{ github.event.client_payload.force_release }}",
            "forced release requires identical main and develop trees",
            "cancel-in-progress: false",
            "mode='bootstrap'",
            "mode='sync'",
            "automation/release-patch",
            "automation/sync-main",
            ".commits >= 2",
            "git merge --no-ff --no-edit \"$MAIN_SHA\"",
            "gh pr create --base develop --head \"$branch\"",
            'kind:"release-patch"',
            'kind:"sync-main"',
            "gh pr create --base main --head develop",
            'kind:"promotion"',
            '{event_type:"automation-ci",client_payload:',
            '{event_type:"development-image",client_payload:',
            '"repos/$GITHUB_REPOSITORY/dispatches"',
        ),
    )
    expected_promotion_permissions = {
        "resolve": {"contents": "read"},
        "write-sync": {"contents": "write"},
        "propose-sync": {
            "contents": "write",
            "pull-requests": "write",
        },
        "write-patch": {"contents": "write"},
        "propose-patch": {
            "contents": "write",
            "pull-requests": "write",
        },
        "promote": {
            "contents": "write",
            "pull-requests": "write",
        },
    }
    for job_name, expected in expected_promotion_permissions.items():
        block = workflow_jobs["create-develop-to-main-pr.yml"].get(job_name)
        if block is None:
            raise ResolverError(f"promotion_job_missing job={job_name}")
        actual = permissions_for(block)
        if actual != expected:
            raise ResolverError(
                f"promotion_job_permissions_mismatch job={job_name} "
                f"expected={expected} actual={actual}"
            )
        if job_name in {"propose-sync", "propose-patch", "promote"} and \
                "GH_REPO: ${{ github.repository }}" not in block:
            raise ResolverError(
                f"promotion_job_repository_context_missing job={job_name}"
            )

    development = workflow_texts.get("develop-image.yml")
    if development is None:
        raise ResolverError("required_workflow_missing files=develop-image.yml")
    development_verify = workflow_jobs["develop-image.yml"].get("verify")
    if development_verify is None or "if: vars.RELEASE_AUTOMATION_ENABLED == 'true'" not in development_verify:
        raise ResolverError("development_image_activation_gate_missing job=verify")
    if "docker/setup-qemu-action" in development or "platforms: linux/amd64,linux/arm64" in development:
        raise ResolverError("development_native_platform_build_required")
    require_fragments(
        "develop-image.yml",
        development,
        (
            "  repository_dispatch:",
            "types: [development-image]",
            "run-name: Development image ${{ github.event.client_payload.revision }}",
            '[ "$EVENT_REF" = refs/heads/main ]',
            '[ "$EVENT_SHA" = "$live_main" ]',
            '"repos/$GITHUB_REPOSITORY/git/ref/heads/develop"',
            "revision: ${{ steps.context.outputs.revision }}",
            '--tag "$IMAGE:develop-$REVISION"',
            "build_args: ${{ steps.build-args.outputs.value }}",
            "build-args: ${{ needs.verify.outputs.build_args }}",
            "publish-platform:",
            "architecture: amd64",
            "runner: ubuntu-24.04",
            "platform: linux/amd64",
            "architecture: arm64",
            "runner: ubuntu-24.04-arm",
            "platform: linux/arm64",
            "runs-on: ${{ matrix.runner }}",
            "platforms: ${{ matrix.platform }}",
            "BUILDKIT_MULTI_PLATFORM: 1",
            "oci-artifact=true",
            "push-by-digest=true",
            "name-canonical=true",
            "development-digest-${{ matrix.architecture }}",
            "actions/upload-artifact@",
            "actions/download-artifact@",
            "docker buildx imagetools create",
            'while [ "$attempt" -le 60 ]',
            "development manifest did not converge",
        ),
    )
    expected_development_permissions = {
        "verify": {"contents": "read"},
        "publish-platform": {"contents": "read", "packages": "write"},
        "publish": {"packages": "write"},
    }
    for job_name, expected in expected_development_permissions.items():
        block = workflow_jobs["develop-image.yml"].get(job_name)
        if block is None:
            raise ResolverError(f"development_job_missing job={job_name}")
        actual = permissions_for(block)
        if actual != expected:
            raise ResolverError(
                f"development_job_permissions_mismatch job={job_name} "
                f"expected={expected} actual={actual}"
            )
    release = workflow_texts.get("release.yml")
    if release is None:
        raise ResolverError("required_workflow_missing files=release.yml")
    release_plan = workflow_jobs["release.yml"].get("plan")
    if release_plan is None or "if: vars.RELEASE_AUTOMATION_ENABLED == 'true'" not in release_plan:
        raise ResolverError("release_activation_gate_missing job=plan")
    if "docker/setup-qemu-action" in release or "platforms: linux/amd64,linux/arm64" in release:
        raise ResolverError("release_native_platform_build_required")
    if ".bypass_actors == []" in release:
        raise ResolverError("release_requires_private_ruleset_field")
    require_fragments(
        "release.yml",
        release,
        (
            "  repository_dispatch:",
            "types: [stable-release-recovery]",
            "INPUT_VERSION: ${{ github.event.client_payload.version }}",
            "INPUT_REVISION: ${{ github.event.client_payload.revision }}",
            "pull-requests: read",
            'promotion=$(gh api "repos/$GITHUB_REPOSITORY/pulls/$number")',
            '.merged_by.login == "jskorlol"',
            '.user.login == "jskorlol"',
            '.state == "APPROVED"',
            ".commit_id == $head",
            "candidate-platform:",
            "architecture: amd64",
            "runner: ubuntu-24.04",
            "platform: linux/amd64",
            "architecture: arm64",
            "runner: ubuntu-24.04-arm",
            "platform: linux/arm64",
            "runs-on: ${{ matrix.runner }}",
            "platforms: ${{ matrix.platform }}",
            "BUILDKIT_MULTI_PLATFORM: 1",
            "oci-artifact=true",
            "push-by-digest=true",
            "name-canonical=true",
            "release-digest-${{ matrix.architecture }}",
            "actions/upload-artifact@",
            "actions/download-artifact@",
            "docker buildx imagetools create",
            'while [ "$attempt" -le 60 ]',
            "release manifest did not converge",
        ),
    )
    expected_release_permissions = {
        "plan": {"contents": "read", "packages": "read", "pull-requests": "read"},
        "candidate-platform": {"contents": "read", "packages": "write"},
        "candidate": {"contents": "read", "packages": "write"},
    }
    for job_name, expected in expected_release_permissions.items():
        block = workflow_jobs["release.yml"].get(job_name)
        if block is None:
            raise ResolverError(f"release_job_missing job={job_name}")
        actual = permissions_for(block)
        if actual != expected:
            raise ResolverError(
                f"release_job_permissions_mismatch job={job_name} "
                f"expected={expected} actual={actual}"
            )
    if checkout_count == 0:
        raise ResolverError("no_checkout_steps_found")
    return {"workflows": len(workflows), "external_actions": action_count, "checkouts": checkout_count}


def _load_fixture(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ResolverError("fixture_root_must_be_object")
    return value


def _fetcher_from_argument(path: str | None) -> Callable[[str, int], dict[str, Any]]:
    return fixture_fetcher(_load_fixture(path)) if path else live_fetcher


def _emit_github_output(values: Mapping[str, Any]) -> None:
    for key, value in values.items():
        if value is None:
            value = ""
        if isinstance(value, (dict, list)):
            value = json.dumps(value, separators=(",", ":"))
        print(f"{key}={value}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)

    check = commands.add_parser("check")
    check.add_argument("--format", choices=("json",), default="json")
    check.add_argument("--offline-fixture")

    update = commands.add_parser("update")
    update.add_argument("--write", action="store_true", required=True)
    update.add_argument("--offline-fixture")

    validate = commands.add_parser("validate")
    validate.add_argument("--base-ref")
    validate.add_argument("--automation-diff", action="store_true")

    args_parser = commands.add_parser("build-args")
    args_parser.add_argument("--format", choices=("json", "github-output"), default="json")
    args_parser.add_argument("--revision")

    preflight = commands.add_parser("release-preflight")
    preflight.add_argument("--version", required=True)
    preflight.add_argument("--revision", required=True)
    source = preflight.add_mutually_exclusive_group(required=True)
    source.add_argument("--live", action="store_true")
    source.add_argument("--offline-fixture")
    mode = preflight.add_mutually_exclusive_group(required=True)
    mode.add_argument("--require-absent", action="store_true")
    mode.add_argument("--allow-same-identity", action="store_true")

    state = commands.add_parser("release-state")
    state.add_argument("--version", required=True)
    state.add_argument("--revision", required=True)
    source = state.add_mutually_exclusive_group()
    source.add_argument("--live", action="store_true")
    source.add_argument("--offline-fixture")
    state.add_argument("--format", choices=("json", "github-output"), default="json")

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
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if arguments.command == "update":
            result = update_files(
                ENV_PATH, VERSION_PATH, _fetcher_from_argument(arguments.offline_fixture)
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if arguments.command == "validate":
            print(
                json.dumps(
                    validate_repository(arguments.base_ref, automation_diff=arguments.automation_diff),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if arguments.command == "build-args":
            result = build_args(ENV_PATH, VERSION_PATH, arguments.revision)
            if arguments.format == "github-output":
                _emit_github_output(result)
            else:
                print(json.dumps(result, indent=2))
            return 0
        if arguments.command in {"release-preflight", "release-state"}:
            live = getattr(arguments, "live", False) or not arguments.offline_fixture
            validate_revision(
                arguments.version, arguments.revision, require_main_ancestor=bool(live)
            )
            surfaces = (
                live_release_surfaces(arguments.version)
                if live
                else _load_fixture(arguments.offline_fixture)
            )
            if arguments.command == "release-preflight":
                result = preflight_release(
                    surfaces,
                    arguments.version,
                    arguments.revision,
                    require_absent=arguments.require_absent,
                    allow_same_identity=arguments.allow_same_identity,
                )
            else:
                result = plan_release_state(surfaces, arguments.version, arguments.revision)
            if getattr(arguments, "format", "json") == "github-output":
                _emit_github_output(result)
            else:
                print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if arguments.command == "validate-actions":
            print(json.dumps(validate_actions(arguments.directory), indent=2, sort_keys=True))
            return 0
    except (ResolverError, ReleaseConflict, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    raise AssertionError(f"unhandled command: {arguments.command}")


if __name__ == "__main__":
    raise SystemExit(main())

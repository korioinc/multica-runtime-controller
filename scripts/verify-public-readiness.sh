#!/usr/bin/env bash

set -euo pipefail

readonly EXPECTED_GITLEAKS_VERSION="8.30.1"
readonly DEFAULT_REPOSITORY="korioinc/multica-runtime-controller"

fail() {
  printf 'PUBLIC_READINESS failed: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

fetch_array() {
  local output=$1
  local endpoint=$2
  gh api --paginate --slurp "$endpoint" | jq 'add // []' >"$output"
}

fetch_connection() {
  local output=$1
  local endpoint=$2
  local field=$3
  gh api --paginate --slurp "$endpoint" |
    jq --arg field "$field" '[.[] | .[$field][]?]' >"$output"
}

repository=${PUBLIC_READINESS_REPOSITORY:-$DEFAULT_REPOSITORY}
cutover_actor=${PUBLIC_READINESS_CUTOVER_ACTOR:-}
mode=${1:-}
shift || true

output_path=
snapshot_path=
while (($#)); do
  case "$1" in
    --output)
      (($# >= 2)) || fail "--output requires a path"
      output_path=$2
      shift 2
      ;;
    --snapshot)
      (($# >= 2)) || fail "--snapshot requires a path"
      snapshot_path=$2
      shift 2
      ;;
    *) fail "unknown argument: $1" ;;
  esac
done

case "$mode" in
  scan) ;;
  snapshot)
    [[ -n $output_path ]] || fail "snapshot requires --output outside the repository"
    ;;
  verify)
    [[ -n $snapshot_path ]] || fail "verify requires --snapshot"
    ;;
  *) fail "usage: $0 {scan|snapshot --output PATH|verify --snapshot PATH}" ;;
esac

for command_name in git gh gitleaks jq shasum python3 unzip; do
  require_command "$command_name"
done

actual_gitleaks_version=$(gitleaks version)
[[ $actual_gitleaks_version == "$EXPECTED_GITLEAKS_VERSION" ]] ||
  fail "gitleaks version must be $EXPECTED_GITLEAKS_VERSION, got $actual_gitleaks_version"

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

if [[ -n $output_path ]]; then
  mkdir -p "$(dirname "$output_path")"
  output_parent=$(cd "$(dirname "$output_path")" && pwd -P)
  case "$output_parent/$(basename "$output_path")" in
    "$repo_root"/*) fail "snapshot output must remain outside the repository" ;;
  esac
fi

if [[ -n $snapshot_path ]]; then
  snapshot_path=$(cd "$(dirname "$snapshot_path")" && pwd -P)/$(basename "$snapshot_path")
  [[ -f $snapshot_path ]] || fail "snapshot does not exist: $snapshot_path"
fi

workdir=$(mktemp -d "${TMPDIR:-/tmp}/multica-public-readiness.XXXXXX")
trap 'rm -rf "$workdir"' EXIT
mkdir -p "$workdir/surfaces" "$workdir/logs" "$workdir/artifacts"

git fetch --all --tags --prune
git for-each-ref --format='%(refname) %(objectname)' refs/heads refs/remotes refs/tags |
  LC_ALL=C sort >"$workdir/ref-map.txt"
git ls-remote --refs origin | LC_ALL=C sort >"$workdir/remote-ref-map.txt"
cat "$workdir/ref-map.txt" "$workdir/remote-ref-map.txt" >"$workdir/all-ref-map.txt"

gitleaks git --no-banner --redact=100 --log-level error --log-opts='--all' \
  --report-format json --report-path "$workdir/git-findings.json" .
gitleaks dir --no-banner --redact=100 --log-level error \
  --report-format json --report-path "$workdir/tree-findings.json" .

gh api "repos/$repository" >"$workdir/repository.json"
fetch_array "$workdir/surfaces/issues.json" \
  "repos/$repository/issues?state=all&per_page=100"
jq '[.[] | select(has("pull_request") | not)]' "$workdir/surfaces/issues.json" \
  >"$workdir/surfaces/issues.only.json"
mv "$workdir/surfaces/issues.only.json" "$workdir/surfaces/issues.json"
fetch_array "$workdir/surfaces/issue-comments.json" \
  "repos/$repository/issues/comments?per_page=100"
fetch_array "$workdir/surfaces/pulls.json" \
  "repos/$repository/pulls?state=all&per_page=100"
fetch_array "$workdir/surfaces/pull-review-comments.json" \
  "repos/$repository/pulls/comments?per_page=100"
printf '[]\n' >"$workdir/surfaces/pull-reviews.json"
while IFS= read -r pull_number; do
  reviews_file="$workdir/pull-reviews-$pull_number.json"
  fetch_array "$reviews_file" "repos/$repository/pulls/$pull_number/reviews?per_page=100"
  jq -s 'add' "$workdir/surfaces/pull-reviews.json" "$reviews_file" \
    >"$workdir/surfaces/pull-reviews.next.json"
  mv "$workdir/surfaces/pull-reviews.next.json" "$workdir/surfaces/pull-reviews.json"
done < <(jq -r '.[].number' "$workdir/surfaces/pulls.json")

fetch_array "$workdir/surfaces/releases.json" \
  "repos/$repository/releases?per_page=100"
mkdir -p "$workdir/surfaces/release-assets"
while IFS= read -r asset_id; do
  gh api --header 'Accept: application/octet-stream' \
    "repos/$repository/releases/assets/$asset_id" \
    >"$workdir/surfaces/release-assets/$asset_id"
done < <(jq -r '.[].assets[]?.id' "$workdir/surfaces/releases.json")
fetch_connection "$workdir/surfaces/actions-runs.json" \
  "repos/$repository/actions/runs?per_page=100" workflow_runs
fetch_connection "$workdir/surfaces/actions-artifacts.json" \
  "repos/$repository/actions/artifacts?per_page=100" artifacts
fetch_connection "$workdir/surfaces/actions-caches.json" \
  "repos/$repository/actions/caches?per_page=100" actions_caches

if jq -e '.has_discussions == true' "$workdir/repository.json" >/dev/null; then
  # GraphQL's $endCursor is expanded by gh, not by this shell.
  # shellcheck disable=SC2016
  gh api graphql --paginate -f query='query($endCursor:String){repository(owner:"korioinc",name:"multica-runtime-controller"){discussions(first:100,after:$endCursor){nodes{id number title body updatedAt comments(first:100){totalCount nodes{body updatedAt replies(first:100){totalCount nodes{body updatedAt}}}}} pageInfo{hasNextPage endCursor}}}}' |
    jq -s '[.[].data.repository.discussions.nodes[]]' \
      >"$workdir/surfaces/discussions.json"
else
  printf '[]\n' >"$workdir/surfaces/discussions.json"
fi

gh api graphql -f query='query{repository(owner:"korioinc",name:"multica-runtime-controller"){projectsV2(first:100){totalCount nodes{id number title shortDescription readme updatedAt}}}}' \
  >"$workdir/projects-response.json"
jq '.data.repository.projectsV2.nodes // []' "$workdir/projects-response.json" \
  >"$workdir/surfaces/projects.json"
project_total=$(jq -r '.data.repository.projectsV2.totalCount' "$workdir/projects-response.json")
project_loaded=$(jq 'length' "$workdir/surfaces/projects.json")
[[ $project_total == "$project_loaded" ]] || fail "more than 100 repository projects require explicit review"

owner=${repository%%/*}
gh api graphql -f query="query{organization(login:\"$owner\"){packages(first:100){totalCount nodes{name packageType repository{nameWithOwner} versions(first:100){totalCount nodes{id version}}}}}}" \
  >"$workdir/packages-response.json"
jq --arg repository "$repository" \
  '[.data.organization.packages.nodes[]? | select(.repository.nameWithOwner == $repository)]' \
  "$workdir/packages-response.json" >"$workdir/surfaces/packages.json"
package_total=$(jq -r '.data.organization.packages.totalCount // 0' "$workdir/packages-response.json")
((package_total <= 100)) || fail "more than 100 organization packages require explicit review"

jq '{has_wiki,has_pages,has_discussions,has_projects}' "$workdir/repository.json" \
  >"$workdir/surfaces/repository-features.json"
if jq -e '.has_wiki == true' "$workdir/repository.json" >/dev/null; then
  git ls-remote "https://github.com/$repository.wiki.git" >"$workdir/surfaces/wiki-refs.txt"
else
  : >"$workdir/surfaces/wiki-refs.txt"
fi
printf '[]\n' >"$workdir/wiki-findings.json"
if [[ -s $workdir/surfaces/wiki-refs.txt ]]; then
  git clone --quiet --mirror "https://github.com/$repository.wiki.git" "$workdir/wiki.git"
  gitleaks git --no-banner --redact=100 --log-level error --log-opts='--all' \
    --report-format json --report-path "$workdir/wiki-findings.json" "$workdir/wiki.git"
fi
if jq -e '.has_pages == true' "$workdir/repository.json" >/dev/null; then
  gh api "repos/$repository/pages" >"$workdir/surfaces/pages.json"
else
  printf '{}\n' >"$workdir/surfaces/pages.json"
fi

while IFS= read -r run_id; do
  gh api "repos/$repository/actions/runs/$run_id/logs" >"$workdir/logs/$run_id.zip"
  unzip -qq "$workdir/logs/$run_id.zip" -d "$workdir/logs/$run_id"
done < <(jq -r '.[] | select(.status == "completed") | .id' "$workdir/surfaces/actions-runs.json")

while IFS= read -r artifact_id; do
  gh api "repos/$repository/actions/artifacts/$artifact_id/zip" \
    >"$workdir/artifacts/$artifact_id.zip"
  unzip -qq "$workdir/artifacts/$artifact_id.zip" -d "$workdir/artifacts/$artifact_id"
done < <(jq -r '.[] | select(.expired == false) | .id' "$workdir/surfaces/actions-artifacts.json")

gitleaks dir --no-banner --redact=100 --log-level error \
  --report-format json --report-path "$workdir/surface-findings.json" "$workdir/surfaces"
gitleaks dir --no-banner --redact=100 --log-level error \
  --report-format json --report-path "$workdir/actions-findings.json" "$workdir/logs"
gitleaks dir --no-banner --redact=100 --log-level error \
  --report-format json --report-path "$workdir/artifact-findings.json" "$workdir/artifacts"

fetch_array "$workdir/collaborators.json" \
  "repos/$repository/collaborators?affiliation=all&per_page=100"
fetch_array "$workdir/teams.json" "repos/$repository/teams?per_page=100"
fetch_array "$workdir/deploy-keys.json" "repos/$repository/keys?per_page=100"
fetch_array "$workdir/org-admins.json" "orgs/$owner/members?role=admin&per_page=100"
gh api "orgs/$owner/installations" >"$workdir/installations.json"
gh api "repos/$repository/actions/permissions" >"$workdir/actions-permissions.json"

if [[ -z $cutover_actor ]]; then
  cutover_actor=$(gh api user --jq .login)
fi

jq --arg actor "$cutover_actor" '
  [
    (.[] | select(.login != $actor) |
      select(.permissions.push == true or .permissions.maintain == true or .permissions.admin == true) |
      {kind:"collaborator",name:.login,detail:.role_name})
  ]' "$workdir/collaborators.json" >"$workdir/active-collaborator-writers.json"
jq --arg actor "$cutover_actor" \
  '[.[] | select(.login != $actor) | {kind:"organization_admin",name:.login,detail:"owner"}]' \
  "$workdir/org-admins.json" >"$workdir/active-org-writers.json"
jq '[.[] | select(.permission == "push" or .permission == "maintain" or .permission == "admin") |
  {kind:"team",name:.slug,detail:.permission}]' "$workdir/teams.json" \
  >"$workdir/active-team-writers.json"
jq '[.[] | select(.read_only == false) | {kind:"deploy_key",name:.title,detail:(.id|tostring)}]' \
  "$workdir/deploy-keys.json" >"$workdir/active-key-writers.json"
jq '[.installations[] |
  select([.permissions | to_entries[] |
    select(.value == "write" and (.key == "actions" or .key == "administration" or
      .key == "checks" or .key == "contents" or .key == "deployments" or
      .key == "environments" or .key == "issues" or .key == "merge_queues" or
      .key == "pages" or .key == "pull_requests" or .key == "secrets" or
      .key == "statuses" or .key == "workflows"))] | length > 0) |
  {kind:"github_app",name:.app_slug,detail:(.repository_selection + ":installation=" + (.id|tostring))}]' \
  "$workdir/installations.json" >"$workdir/active-app-writers.json"
jq -s 'add' \
  "$workdir/active-collaborator-writers.json" \
  "$workdir/active-org-writers.json" \
  "$workdir/active-team-writers.json" \
  "$workdir/active-key-writers.json" \
  "$workdir/active-app-writers.json" >"$workdir/active-writers.json"

python3 - "$workdir" "$repository" "$cutover_actor" >"$workdir/readiness.json" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
repository = sys.argv[2]
actor = sys.argv[3]

def digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def tree_digest(path: pathlib.Path) -> str:
    value = hashlib.sha256()
    for entry in sorted(item for item in path.rglob("*") if item.is_file()):
        value.update(entry.relative_to(path).as_posix().encode())
        value.update(b"\0")
        value.update(entry.read_bytes())
        value.update(b"\0")
    return value.hexdigest()

def timestamps(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"updated_at", "updatedAt", "published_at", "created_at"} and isinstance(item, str):
                yield item
            yield from timestamps(item)
    elif isinstance(value, list):
        for item in value:
            yield from timestamps(item)

surface_specs = {
    "issues": "issues.json",
    "issue_comments": "issue-comments.json",
    "pull_requests": "pulls.json",
    "pull_reviews": "pull-reviews.json",
    "pull_review_comments": "pull-review-comments.json",
    "discussions": "discussions.json",
    "projects": "projects.json",
    "packages": "packages.json",
    "releases_and_assets": "releases.json",
    "actions_runs_and_logs": "actions-runs.json",
    "actions_artifacts": "actions-artifacts.json",
    "actions_caches": "actions-caches.json",
}
surfaces = {}
for name, filename in surface_specs.items():
    path = root / "surfaces" / filename
    data = json.loads(path.read_text())
    count = len(data)
    if name == "releases_and_assets":
        count += sum(len(item.get("assets", [])) for item in data if isinstance(item, dict))
    status = "empty" if count == 0 else "redacted_reviewed"
    if name == "actions_caches" and count:
        status = "requires_deletion"
    observed_timestamps = sorted(timestamps(data))
    surfaces[name] = {
        "count": count,
        "status": status,
        "sha256": digest(path),
        "updated_at_max": observed_timestamps[-1] if observed_timestamps else None,
    }

surfaces["actions_runs_and_logs"]["content_sha256"] = tree_digest(root / "logs")
surfaces["actions_artifacts"]["content_sha256"] = tree_digest(root / "artifacts")
surfaces["releases_and_assets"]["content_sha256"] = tree_digest(root / "surfaces" / "release-assets")

for name, filename in (("wiki", "wiki-refs.txt"), ("pages", "pages.json")):
    path = root / "surfaces" / filename
    if name == "wiki":
        count = sum(1 for line in path.read_text().splitlines() if line.strip())
    else:
        data = json.loads(path.read_text())
        count = 1 if data else 0
    status = "empty" if count == 0 else "redacted_reviewed"
    if name == "pages" and count:
        status = "requires_manual_review"
    surfaces[name] = {
        "count": count,
        "status": status,
        "sha256": digest(path),
        "updated_at_max": None,
    }

features_path = root / "surfaces" / "repository-features.json"
features = json.loads(features_path.read_text())
feature_count = sum(1 for enabled in features.values() if enabled)
surfaces["repository_features"] = {
    "count": feature_count,
    "status": "redacted_reviewed" if feature_count else "empty",
    "sha256": digest(features_path),
    "updated_at_max": None,
}
if surfaces["packages"]["count"]:
    surfaces["packages"]["status"] = "requires_manual_review"

surface_manifest = json.dumps(surfaces, sort_keys=True, separators=(",", ":")).encode()
active_writers = json.loads((root / "active-writers.json").read_text())
actions_permissions = json.loads((root / "actions-permissions.json").read_text())
runs = json.loads((root / "surfaces" / "actions-runs.json").read_text())
manifest = {
    "schema": 1,
    "repository": repository,
    "cutover_actor": actor,
    "ref_map_sha256": digest(root / "all-ref-map.txt"),
    "surface_manifest_sha256": hashlib.sha256(surface_manifest).hexdigest(),
    "writer_manifest_sha256": digest(root / "active-writers.json"),
    "surfaces": surfaces,
    "active_writers": active_writers,
    "actions_enabled": bool(actions_permissions.get("enabled")),
    "nonterminal_action_runs": sum(1 for run in runs if run.get("status") != "completed"),
    "findings": {
        "git": len(json.loads((root / "git-findings.json").read_text() or "[]")),
        "tree": len(json.loads((root / "tree-findings.json").read_text() or "[]")),
        "surfaces": len(json.loads((root / "surface-findings.json").read_text() or "[]")),
        "actions": len(json.loads((root / "actions-findings.json").read_text() or "[]")),
        "artifacts": len(json.loads((root / "artifact-findings.json").read_text() or "[]")),
        "wiki": len(json.loads((root / "wiki-findings.json").read_text() or "[]")),
    },
}
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

finding_count=$(jq '[.findings[]] | add' "$workdir/readiness.json")
((finding_count == 0)) || fail "redacted scans reported $finding_count finding(s)"
jq -e '[.surfaces[].status] | all(. == "empty" or . == "redacted_reviewed" or . == "requires_deletion")' \
  "$workdir/readiness.json" >/dev/null || fail "an unreviewed public surface remains"

if [[ $mode == scan ]]; then
  jq '{repository,ref_map_sha256,surface_manifest_sha256,findings,surfaces,actions_enabled,nonterminal_action_runs,active_writers}' \
    "$workdir/readiness.json"
  printf 'PUBLIC_READINESS passed mode=scan freeze_required=%s\n' \
    "$(jq -r '(.actions_enabled or .nonterminal_action_runs > 0 or (.active_writers|length) > 0 or .surfaces.actions_caches.count > 0)' "$workdir/readiness.json")"
  exit 0
fi

jq -e '.actions_enabled == false' "$workdir/readiness.json" >/dev/null ||
  fail "GitHub Actions must be disabled during the frozen cutover"
jq -e '.nonterminal_action_runs == 0' "$workdir/readiness.json" >/dev/null ||
  fail "all GitHub Actions runs must be terminal"
jq -e '.surfaces.actions_caches.count == 0' "$workdir/readiness.json" >/dev/null ||
  fail "all private-era Actions caches must be deleted"
jq -e '.active_writers | length == 0' "$workdir/readiness.json" >/dev/null ||
  fail "active writer other than the cutover actor remains"

if [[ $mode == snapshot ]]; then
  cp "$workdir/readiness.json" "$output_path"
  chmod 0600 "$output_path"
  printf 'PUBLIC_READINESS passed mode=snapshot snapshot=%s\n' "$output_path"
  exit 0
fi

for field in ref_map_sha256 surface_manifest_sha256 writer_manifest_sha256; do
  expected=$(jq -r ".$field" "$snapshot_path")
  actual=$(jq -r ".$field" "$workdir/readiness.json")
  [[ $actual == "$expected" ]] || fail "$field drifted after the frozen snapshot"
done
printf 'PUBLIC_READINESS passed mode=verify snapshot=%s\n' "$snapshot_path"

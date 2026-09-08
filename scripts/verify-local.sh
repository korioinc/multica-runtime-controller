#!/usr/bin/env bash
# All orchestration is shell; compiled commands are the repository's native
# controller/provider protocol checks. No ambient Kubernetes context is used.
set -euo pipefail
export LC_ALL=C
lv_repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
# shellcheck source=scripts/lib/local-state.sh
source "$lv_repository/scripts/lib/local-state.sh"
# shellcheck source=scripts/lib/local-image.sh
source "$lv_repository/scripts/lib/local-image.sh"
# shellcheck source=scripts/lib/local-cluster.sh
source "$lv_repository/scripts/lib/local-cluster.sh"
# shellcheck source=scripts/lib/local-scenarios.sh
source "$lv_repository/scripts/lib/local-scenarios.sh"

lv_usage() {
  cat <<'USAGE'
Usage:
  scripts/verify-local.sh --core-only [--keep-on-failure]
  scripts/verify-local.sh --chart DIRECTORY --runtime-image IMAGE [--keep-on-failure] [--resume DIRECTORY]

--core-only builds and natively executes the CLI-free controller base and Go SDK.
--chart requires a locally available, verified complete runtime image built from
this controller checkout. Integration creates an owned Docker registry/backend
and disposable K3s; it never reads the current Kubernetes context. Native ORAS
copies immutable OCI bytes through the host's local registry endpoint.
--resume reuses only the same stopped orchestration's still-running owned handles
and unchanged sources, chart and runtime-image selection.
USAGE
}
lv_chart='' lv_input_image='' lv_core=false lv_keep=false lv_resume=''
while [[ $# -gt 0 ]]; do
  case $1 in
    --core-only) lv_core=true; shift ;;
    --chart|--runtime-image|--resume)
      [[ $# -ge 2 ]] || { lv_usage >&2; exit 2; }
      case $1 in --chart) lv_chart=$2 ;; --runtime-image) lv_input_image=$2 ;; --resume) lv_resume=$2 ;; esac
      shift 2 ;;
    --keep-on-failure) lv_keep=true; shift ;;
    --help|-h) lv_usage; exit 0 ;;
    *) lv_usage >&2; exit 2 ;;
  esac
done
if [[ $lv_core == true ]]; then
  [[ -z $lv_chart && -z $lv_input_image && -z $lv_resume ]] || { lv_usage >&2; exit 2; }
  lv_mode_name=core
else
  [[ -n $lv_chart && -n $lv_input_image && $lv_input_image != -* && $lv_input_image != *[[:space:]]* ]] || { lv_usage >&2; exit 2; }
  [[ -f $lv_chart/Chart.yaml ]] || { lv_fail 'explicit chart is missing Chart.yaml'; exit 2; }
  lv_chart=$(cd -- "$lv_chart" && pwd -P)
  lv_mode_name=integration
fi
for lv_tool in docker jq git make rg; do command -v "$lv_tool" >/dev/null || { lv_fail "$lv_tool is required"; exit 2; }; done
if [[ $lv_core != true ]]; then
  for lv_tool in helm go uuidgen oras curl tar; do command -v "$lv_tool" >/dev/null || { lv_fail "$lv_tool is required"; exit 2; }; done
fi
if [[ -n ${DOCKER_CONTEXT:-} ]]; then
  lv_endpoint=$(docker context inspect --format '{{.Endpoints.docker.Host}}' "$DOCKER_CONTEXT")
elif [[ -n ${DOCKER_HOST:-} ]]; then
  lv_endpoint=$DOCKER_HOST
else
  lv_endpoint=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
fi
case $lv_endpoint in unix://*|tcp://127.0.0.1:*|tcp://localhost:*) : ;; *) lv_fail 'only a local Docker engine endpoint is supported'; exit 2 ;; esac
lv_engine=$(docker info --format '{{.OSType}}/{{.Architecture}}')
case $lv_engine in linux/x86_64|linux/amd64) lv_arch=amd64 ;; linux/aarch64|linux/arm64) lv_arch=arm64 ;; *) lv_fail 'a native Linux amd64 or arm64 Docker engine is required'; exit 2 ;; esac
if [[ -n $lv_resume ]]; then
  [[ -d $lv_resume && -f $lv_resume/state.json ]] || { lv_fail 'resume directory has no saved owned state'; exit 2; }
  lv_work=$(cd -- "$lv_resume" && pwd -P)
else
  lv_work=$(mktemp -d "${TMPDIR:-/tmp}/multica-runtime-verify.XXXXXX")
  lv_work=$(cd -- "$lv_work" && pwd -P)
fi
[[ ! -L $lv_work/.orchestration.lock ]] || { lv_fail 'orchestration lock must not be a symlink'; exit 2; }
exec 9>>"$lv_work/.orchestration.lock"
if command -v flock >/dev/null; then flock -n 9 || { lv_fail 'the owned orchestration is still active'; exit 2; }
elif command -v lockf >/dev/null; then lockf -s -t 0 9 || { lv_fail 'the owned orchestration is still active'; exit 2; }
else lv_fail 'native flock or BSD lockf is required'; exit 2; fi
lv_name=$(basename "$lv_work" | tr '[:upper:]' '[:lower:]')
[[ $lv_name =~ ^multica-runtime-verify\.[a-z0-9]+$ ]] || { lv_fail 'unrecognized disposable work-directory identity'; exit 2; }
lv_namespace=runtime-verify
lv_registry=$lv_name-registry
lv_evidence=$lv_work/evidence
lv_kubeconfig=$lv_work/kubeconfig
lv_active_log='' lv_phase=created lv_registry_port=''
if [[ -z $lv_resume ]]; then
  mkdir -m 0777 "$lv_evidence"
  chmod 0777 "$lv_evidence"
  mkdir "$lv_work/context"
  jq -cn --arg root "$lv_work" --arg chart "$lv_chart" --arg image "$lv_input_image" --arg arch "$lv_arch" --arg owner "$lv_name" --arg mode "$lv_mode_name" \
    '{schemaVersion:2,root:$root,chart:$chart,runtimeImage:$image,arch:$arch,owner:$owner,mode:$mode,containers:{},images:{},completedSteps:[],passed:[],phase:"created",complete:false}' >"$lv_work/state.json"
  chmod 0600 "$lv_work/state.json"
  lv_source_snapshot >"$lv_evidence/source-build.json"
else
  lv_restore
fi
trap 'lv_finish "$?"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'Local verification work directory: %s\n' "$lv_work"
if [[ $lv_core == true ]]; then
  lv_verify_base
else
  if [[ -z $lv_resume ]]; then
    lv_step build-fixtures lv_build_fixtures
    lv_step start-cluster lv_start_cluster
  fi
  lv_runtime_scenarios
fi
lv_check_source
lv_phase=complete

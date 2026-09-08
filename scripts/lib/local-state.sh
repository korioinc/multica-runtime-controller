#!/usr/bin/env bash
# State and exact-handle fencing for an exclusively owned disposable Docker run.
# Globals are initialized by verify-local.sh; jq programs passed to lv_state are literal.
# shellcheck disable=SC2034,SC2154,SC2016

lv_fail() { printf 'local verification: %s\n' "$*" >&2; return 1; }
lv_sha() { if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi | cut -d ' ' -f 1; }
lv_mode() { stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1"; }
lv_uuid() { uuidgen | tr '[:upper:]' '[:lower:]'; }
lv_state() {
  local pending="$lv_work/state.json.pending"
  jq "$@" "$lv_work/state.json" >"$pending" || return
  chmod 0600 "$pending"
  mv "$pending" "$lv_work/state.json"
}
lv_stage() {
  lv_phase=$1
  lv_state --arg phase "$1" '.phase=$phase'
  printf '%s\n' "$1"
}
lv_pass() {
  lv_state --arg proof "$1" '.passed += [$proof]'
  printf 'PASS: %s\n' "$1"
}
lv_step() {
  local name=$1
  shift
  if jq -e --arg name "$name" '.completedSteps | index($name) != null' "$lv_work/state.json" >/dev/null; then return; fi
  "$@"
  lv_state --arg name "$name" '.completedSteps += [$name]'
}
lv_run() {
  lv_active_log=$1
  shift
  "$@" >"$lv_evidence/$lv_active_log" 2>&1 || return
  lv_active_log=''
}
lv_source_snapshot() {
  local paths=$lv_work/source-paths name sha mode
  {
    rg --files --hidden "$lv_repository/src/cmd" "$lv_repository/src/internal" "$lv_repository/scripts" "$lv_repository/.github" "$lv_repository/build"
    printf '%s\n' "$lv_repository/src/go.mod" "$lv_repository/src/go.sum" "$lv_repository/Dockerfile" "$lv_repository/.dockerignore" "$lv_repository/Makefile" "$lv_repository/VERSION"
    if [[ -n $lv_chart ]]; then rg --files --hidden "$lv_chart"; fi
  } | LC_ALL=C sort -u >"$paths"
  while IFS= read -r name; do
    [[ -f $name && ! -L $name ]] || continue
    # Capture executable/build/render inputs. Documentation edits do not alter
    # the binaries or manifests whose provenance this execution proof records.
    case "$name" in
      "$lv_repository/src/cmd/"*.go|"$lv_repository/src/internal/"*.go|"$lv_repository/scripts/"*.sh|"$lv_repository/.github/"*.sh|"$lv_repository/.github/"*.yaml|"$lv_repository/.github/"*.yml) : ;;
      "$lv_repository/src/go.mod"|"$lv_repository/src/go.sum"|"$lv_repository/Dockerfile"|"$lv_repository/.dockerignore"|"$lv_repository/Makefile"|"$lv_repository/VERSION"|"$lv_repository/build/runtime-versions.env"|"$lv_repository/build/"*Dockerfile*) : ;;
      "$lv_chart/"*) [[ -n $lv_chart ]] || continue; case "$name" in *.yaml|*.yml|*.json|*.tpl|*.sh|*.env|*Dockerfile*) : ;; *) continue ;; esac ;;
      *) continue ;;
    esac
    sha=$(lv_sha <"$name")
    mode=$(lv_mode "$name")
    jq -cn --arg path "$name" --arg sha "$sha" --arg mode "$mode" '{key:$path,value:{sha256:$sha,mode:$mode}}'
  done <"$paths" | jq -csS 'from_entries'
}
lv_check_source() {
  lv_source_snapshot >"$lv_evidence/source-final.json"
  if ! cmp -s "$lv_evidence/source-build.json" "$lv_evidence/source-final.json"; then
    jq -n --slurpfile before "$lv_evidence/source-build.json" --slurpfile after "$lv_evidence/source-final.json" \
      '($before[0] + $after[0] | keys) | map(select($before[0][.] != $after[0][.]))' >"$lv_evidence/changed-source.json"
    lv_fail 'source changed after image/helper build; retained output cannot prove the current checkout'
  fi
}
lv_owned_container() {
  local role=$1 identifier info name
  identifier=$(jq -er --arg role "$role" '.containers[$role].id' "$lv_work/state.json") || return
  name=$(jq -er --arg role "$role" '.containers[$role].name' "$lv_work/state.json") || return
  info=$(docker inspect "$identifier") || return
  jq -e --arg id "$identifier" --arg name "/$name" --arg owner "$lv_name" \
    'length == 1 and .[0].Id == $id and .[0].Name == $name and .[0].Config.Labels["io.multica.localverify"] == $owner' <<<"$info" >/dev/null || { lv_fail "owned $role container identity changed"; return 1; }
  printf '%s\n' "$identifier"
}
lv_create_container() {
  local role=$1 name=$2 identifier
  shift 2
  identifier=$(docker create --name "$name" --label "io.multica.localverify=$lv_name" "$@") || return
  lv_state --arg role "$role" --arg name "$name" --arg id "$identifier" '.containers[$role]={name:$name,id:$id}' || return
  printf '%s\n' "$identifier"
}
lv_image_record() {
  local tag=$1 identifier
  identifier=$(docker image inspect "$tag" | jq -er 'if length == 1 then .[0].Id else error("ambiguous local image") end')
  lv_state --arg tag "$tag" --arg id "$identifier" '.images[$tag]=$id'
}
lv_kube() {
  local node
  node=$(lv_owned_container node) || return
  docker exec -i "$node" kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml "$@"
}
lv_backend() {
  local backend
  backend=$(lv_owned_container backend) || return
  docker exec "$backend" curl --max-time 20 --fail --silent --show-error "$@"
}
lv_backend_state() { lv_backend http://127.0.0.1:18080/fixture/state; }
lv_wait() {
  local description=$1 limit=$2 beginning=$SECONDS notice=$SECONDS
  shift 2
  while (( SECONDS - beginning < limit )); do
    if "$@"; then return; elif [[ $? == 125 ]]; then return 1; fi
    if (( SECONDS - notice >= 20 )); then printf 'Waiting for %s\n' "$description"; notice=$SECONDS; fi
    sleep 2
  done
  lv_fail "$description did not complete; no acceptance result was recorded"
}
lv_restore() {
  jq -e --arg work "$lv_work" --arg chart "$lv_chart" --arg image "$lv_input_image" --arg arch "$lv_arch" --arg name "$lv_name" \
    '.schemaVersion == 2 and .mode == "integration" and .root == $work and .chart == $chart and .runtimeImage == $image and .arch == $arch and .owner == $name and (.completedSteps | type == "array") and .complete != true' \
    "$lv_work/state.json" >/dev/null || lv_fail 'resume requires the same unfinished chart, image, platform and owned directory'
  lv_check_source
  local role identifier info tag expected actual
  expected=$(jq -er '.inputImageID' "$lv_work/state.json")
  actual=$(docker image inspect "$lv_input_image" | jq -er 'if length == 1 then .[0].Id else error("ambiguous input image") end')
  [[ $actual == "$expected" ]] || lv_fail 'selected runtime image tag changed since the retained run'
  while IFS=$'\t' read -r tag expected; do
    actual=$(docker image inspect "$tag" | jq -er 'if length == 1 then .[0].Id else error("ambiguous owned image") end')
    [[ $actual == "$expected" ]] || lv_fail 'an owned fixture image tag was replaced after the retained run'
  done < <(jq -r '.images | to_entries[] | [.key,.value] | @tsv' "$lv_work/state.json")
  for role in node backend registry; do
    identifier=$(lv_owned_container "$role")
    info=$(docker inspect "$identifier")
    jq -e '.[0].State.Running == true' <<<"$info" >/dev/null || lv_fail "resume $role handle is stopped; this cluster cannot be resumed"
    if [[ $role == node ]]; then
      jq -e --arg source "$lv_evidence" '.[0].Mounts | any(.Source == $source and .Destination == "/verification-evidence")' <<<"$info" >/dev/null || lv_fail 'resume node evidence mount changed'
    fi
  done
  lv_backend_origin=$(jq -er '.backendOrigin' "$lv_work/state.json")
  lv_registry_port=$(jq -er '.registryPort' "$lv_work/state.json")
  lv_image_a=$(jq -er '.imageA' "$lv_work/state.json")
  lv_image_b=$(jq -er '.imageB' "$lv_work/state.json")
  lv_fixture_a=$(jq -er '.fixtureA' "$lv_work/state.json")
  lv_fixture_b=$(jq -er '.fixtureB' "$lv_work/state.json")
}
lv_collect() {
  local role identifier pod container
  while IFS= read -r role; do
    identifier=$(lv_owned_container "$role" 2>/dev/null) || continue
    docker logs "$identifier" >"$lv_evidence/$role-container.log" 2>&1
  done < <(jq -r '.containers | keys[]' "$lv_work/state.json")
  if lv_owned_container node >/dev/null 2>&1; then
    lv_kube -n "$lv_namespace" get pods -o json >"$lv_evidence/pods.json" 2>"$lv_evidence/pods-error.log"
    lv_kube -n "$lv_namespace" get events -o json >"$lv_evidence/events.json" 2>"$lv_evidence/events-error.log"
    if jq -e '.items' "$lv_evidence/pods.json" >/dev/null 2>&1; then
      while IFS=$'\t' read -r pod container; do
        lv_kube -n "$lv_namespace" logs "$pod" -c "$container" >"$lv_evidence/$pod-$container.log" 2>&1
      done < <(jq -r '.items[] | .metadata.name as $pod | (.spec.initContainers[]?,.spec.containers[]) | [$pod,.name] | @tsv' "$lv_evidence/pods.json")
    fi
  fi
}
lv_cleanup() {
  local role identifier network tag expected actual
  while IFS= read -r role; do
    identifier=$(lv_owned_container "$role" 2>/dev/null) || continue
    docker rm --force --volumes "$identifier" >/dev/null 2>&1
  done < <(jq -r '.containers | keys | reverse[]' "$lv_work/state.json")
  network=$(jq -r '.networkID // empty' "$lv_work/state.json")
  if [[ -n $network ]] && docker network inspect "$network" | jq -e --arg owner "$lv_name" --arg id "$network" '.[0].Id == $id and .[0].Labels["io.multica.localverify"] == $owner' >/dev/null 2>&1; then
    docker network rm "$network" >/dev/null 2>&1
  fi
  while IFS=$'\t' read -r tag expected; do
    case $tag in "multica-runtime-verify:$lv_name-"*|"localhost:${lv_registry_port:-unset}/runtime:"*|"127.0.0.1:${lv_registry_port:-unset}/runtime:"*) : ;; *) continue ;; esac
    actual=$(docker image inspect "$tag" 2>/dev/null | jq -r '.[0].Id') || continue
    [[ $actual == "$expected" ]] || continue
    docker image rm "$tag" >/dev/null 2>&1
  done < <(jq -r '.images | to_entries[] | [.key,.value] | @tsv' "$lv_work/state.json")
}
lv_finish() {
  local status=$1
  trap - EXIT INT TERM
  set +e
  if [[ $status != 0 && -n ${lv_active_log:-} ]]; then tail -n 60 "$lv_evidence/$lv_active_log" >&2; fi
  lv_collect
  lv_state --argjson status "$status" --arg phase "${lv_phase:-unknown}" '.complete=($status == 0) | .exitCode=$status | .phase=$phase'
  jq '{schemaVersion:2,complete,exitCode,mode,chart,runtimeImage,arch,phase,passed}' "$lv_work/state.json" >"$lv_evidence/summary.json"
  if [[ $status == 0 || $lv_keep != true ]]; then lv_cleanup; fi
  printf 'Local verification evidence: %s\n' "$lv_evidence"
  if [[ $status != 0 && $lv_keep == true ]]; then printf 'Retained owned handles: %s/state.json\n' "$lv_work"; fi
  exit "$status"
}

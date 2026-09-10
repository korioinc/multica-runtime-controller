#!/usr/bin/env bash
# Actual Kubernetes task, filesystem, admission and recovery scenarios.
# Globals are shared with the entrypoint; lv_state receives literal jq expressions.
# shellcheck disable=SC2034,SC2154,SC2016

lv_snapshot_workers() {
  local label=$1 destination node path relative sha mode target
  destination=$(mktemp -d "$lv_work/$label.XXXXXX")
  node=$(lv_owned_container node)
  docker cp "$node:/verification-workspace/.multica-runtime/workers/." "$destination"
  while IFS= read -r -d '' path; do
    relative=${path#"$destination/"}
    if [[ -L $path ]]; then
      target=$(readlink "$path")
      jq -cn --arg path "$relative" --arg link "$target" '{key:$path,value:{link:$link}}'
    else
      sha=$(lv_sha <"$path")
      mode=$(lv_mode "$path")
      jq -cn --arg path "$relative" --arg sha "$sha" --arg mode "$mode" '{key:$path,value:{sha256:$sha,mode:$mode}}'
    fi
  done < <(find "$destination" \( -type f -o -type l \) -print0) | jq -csS 'from_entries' >"$lv_evidence/$label.json"
}
lv_journal_failure() {
  lv_stage 'Rejecting task resources when the durable journal cannot be written'
  local controller task kind
  lv_snapshot_workers journal-before-files
  jq -e 'length > 0' "$lv_evidence/journal-before-files.json" >/dev/null
  controller=$(lv_controller)
  lv_kube -n "$lv_namespace" exec "$controller" -c controller -- chmod 0500 /workspace/.multica-runtime/attempts
  if ! lv_drive journal-failure; then
    lv_kube -n "$lv_namespace" exec "$controller" -c controller -- chmod 0700 /workspace/.multica-runtime/attempts
    return 1
  fi
  lv_kube -n "$lv_namespace" exec "$controller" -c controller -- chmod 0700 /workspace/.multica-runtime/attempts
  task=$(lv_backend_state | jq -er .journalTask)
  for kind in pods secrets; do
    lv_kube -n "$lv_namespace" get "$kind" -l "multica.ai/task-id=$task" -o json | jq -e '.items | length == 0' >/dev/null
  done
  lv_snapshot_workers journal-after-files
  jq -e --slurpfile before "$lv_evidence/journal-before-files.json" 'to_entries as $after | $before[0] | to_entries | all(. as $item | any($after[]; .key == $item.key and .value == $item.value))' "$lv_evidence/journal-after-files.json" >/dev/null
  lv_pass 'durable journal failure prevents task resources and preserves existing worker files'
}
lv_controller_role() {
  local controller account name
  controller=$(lv_controller)
  account=$(lv_kube -n "$lv_namespace" get pod "$controller" -o json | jq -er .spec.serviceAccountName)
  name=$(lv_kube -n "$lv_namespace" get rolebindings -o json | jq -er --arg account "$account" --arg namespace "$lv_namespace" \
    '[.items[] | select(.roleRef.kind == "Role" and any(.subjects[]?; .kind == "ServiceAccount" and .name == $account and .namespace == $namespace)) | .roleRef.name] | unique | if length == 1 then .[0] else error("one owned controller Role required") end')
  lv_kube -n "$lv_namespace" get role "$name" -o json
}
lv_restore_cleanup_role() {
  local name uid current patch
  name=$(jq -er .metadata.name "$lv_work/cleanup-role.json")
  uid=$(jq -er .metadata.uid "$lv_work/cleanup-role.json")
  current=$(lv_kube -n "$lv_namespace" get role "$name" -o json)
  [[ $(jq -r .metadata.uid <<<"$current") == "$uid" ]] || { lv_fail 'controller Role was replaced; refusing permission restoration'; return 1; }
  patch=$(jq -c '{rules}' "$lv_work/cleanup-role.json")
  lv_kube -n "$lv_namespace" patch role "$name" --type=merge -p "$patch" >/dev/null
}
lv_cleanup_failure() {
  lv_stage 'Denying actual cleanup while preserving provider success and recovery intent'
  local role task pods secrets node patch
  lv_controller_role >"$lv_work/cleanup-role.json"
  role=$(jq -er .metadata.name "$lv_work/cleanup-role.json")
  patch=$(jq -c '{rules:[.rules[] | if any(.apiGroups[]; . == "") and any(.resources[]; . == "pods" or . == "secrets") then .verbs -= ["delete"] else . end]}' "$lv_work/cleanup-role.json")
  lv_kube -n "$lv_namespace" patch role "$role" --type=merge -p "$patch" >/dev/null
  if ! lv_drive cleanup-failure; then lv_restore_cleanup_role; return 1; fi
  task=$(lv_backend_state | jq -er .cleanupTask)
  pods=$(lv_kube -n "$lv_namespace" get pods -l "multica.ai/task-id=$task" -o json)
  secrets=$(lv_kube -n "$lv_namespace" get secrets -l "multica.ai/task-id=$task" -o json)
  jq -e '.items | length > 0' <<<"$pods" >/dev/null
  jq -e '.items | length > 0' <<<"$secrets" >/dev/null
  jq -cn --arg task "$task" --argjson pods "$pods" --argjson secrets "$secrets" '{task:$task,pods:[$pods.items[].metadata],secrets:[$secrets.items[].metadata]}' >"$lv_evidence/cleanup-denied.json"
  node=$(lv_owned_container node)
  docker exec "$node" tar -czf /verification-evidence/cleanup-attempts.tar.gz -C /verification-workspace/.multica-runtime attempts
  lv_pass 'successful provider work retains exact Pod, request Secret and journal under denied cleanup'
}
lv_restarted() {
  local current
  current=$(lv_kube -n "$lv_namespace" get pod "$lv_restart_name" -o json 2>/dev/null) || return
  [[ $(jq -r .metadata.uid <<<"$current") == "$lv_restart_uid" ]] || { lv_fail 'process restart replaced the controller Pod'; return 125; }
  jq -e --argjson previous "$lv_restart_count" 'any(.status.containerStatuses[]?; .name == "controller" and .restartCount > $previous and .ready == true)' <<<"$current" >/dev/null
}
lv_restart_controller() {
  local action=${1:-} pod status container node inspected pid
  lv_restart_name=$(lv_controller)
  pod=$(lv_kube -n "$lv_namespace" get pod "$lv_restart_name" -o json)
  lv_restart_uid=$(jq -er .metadata.uid <<<"$pod")
  status=$(jq -cer '.status.containerStatuses[] | select(.name == "controller")' <<<"$pod")
  lv_restart_count=$(jq -er .restartCount <<<"$status")
  container=$(jq -er .containerID <<<"$status")
  [[ $container =~ ^containerd://[0-9a-f]{64}$ ]] || lv_fail 'unexpected owned controller container identity'
  node=$(lv_owned_container node)
  inspected=$(docker exec "$node" crictl inspect "${container#containerd://}")
  pid=$(jq -er '.info.pid | select(type == "number" and . > 1)' <<<"$inspected")
  jq -e --arg uid "$lv_restart_uid" '.status.labels["io.kubernetes.pod.uid"] == $uid' <<<"$inspected" >/dev/null
  if [[ -n $action ]]; then
    docker exec "$node" kill -STOP "$pid"
    if ! "$action"; then docker exec "$node" kill -CONT "$pid"; return 1; fi
  fi
  docker exec "$node" kill -KILL "$pid"
  lv_wait 'same-Pod controller process restart' 180 lv_restarted
}
lv_cleanup_recovered() {
  lv_stage 'Recovering denied cleanup in a fresh controller process'
  lv_restart_controller lv_restore_cleanup_role
  lv_drive recovered
  lv_pass 'process recovery preserves files, branch and compatible native session after cleanup denial'
}
lv_interrupted_recovered() {
  lv_stage 'Interrupting the controller after a worker durably publishes edits'
  lv_backend_state | jq -e '.tasks[.interruptedTask].checkpoint | select(.stage == "held")' >"$lv_evidence/interrupted-checkpoint.json"
  lv_restart_controller
  lv_drive interrupted-resume
  lv_pass 'abrupt controller death preserves committed worker edits and same-task retry authority'
}
lv_pod_phase() {
  local pod=$1 expected=$2 current
  current=$(lv_kube -n "$lv_namespace" get pod "$pod" -o json 2>/dev/null) || return
  [[ $(jq -r '.status.phase // ""' <<<"$current") == "$expected" ]]
}
lv_mixed_image() {
  lv_stage 'Rejecting different init/main images before application workspace admission'
  lv_rendered_pod | jq --arg ns "$lv_namespace" --arg init "$lv_image_a" --arg main "$lv_image_b" \
    '{apiVersion:"v1",kind:"Pod",metadata:{name:"mixed-image",namespace:$ns},spec:.spec} |
      .spec.restartPolicy="Never" | .spec.initContainers[].image=$init | .spec.containers[].image=$main |
      .spec.volumes |= map(if .name == "workspace" then .persistentVolumeClaim.claimName="rejected" else . end) |
      .spec.containers |= map(del(.startupProbe,.readinessProbe,.livenessProbe))' >"$lv_work/mixed-image.json"
  lv_kube create -f - <"$lv_work/mixed-image.json"
  lv_wait 'mixed-image admission rejection' 180 lv_pod_phase mixed-image Failed
  lv_kube -n "$lv_namespace" get pod mixed-image -o json >"$lv_evidence/mixed-image.json"
  jq -e --arg a "$lv_image_a" 'any(.status.initContainerStatuses[]?; .name == "home-layout" and (.imageID | sub("^docker-pullable://";"")) == $a and .state.terminated.exitCode == 0) and any(.status.containerStatuses[]?; .name == "controller" and .imageID != "" and (.imageID | sub("^docker-pullable://";"")) != $a and .state.terminated.exitCode != 0)' "$lv_evidence/mixed-image.json" >/dev/null
  docker exec "$(lv_owned_container node)" sh -ec 'test ! -e /verification-rejected/.multica-runtime'
  lv_pass 'actual init A/main B is rejected before application workspace initialization'
}
lv_retry_capture_failed() {
  lv_kube -n "$lv_namespace" exec config-retry -c home-layout -- test -f /opt/multica/private/run/configuration.json >/dev/null 2>&1 &&
    lv_kube -n "$lv_namespace" exec config-retry -c home-layout -- test -f /opt/multica/private/run/copy-failed >/dev/null 2>&1
}
lv_retry_projection_b() {
  [[ $(lv_kube -n "$lv_namespace" exec config-retry -c home-layout -- cat /opt/multica/config-input/provider/operator.txt 2>/dev/null) == configuration-B ]]
}
lv_configuration_retry() {
  lv_stage 'Retrying HOME copy from the committed bundle after the source projection changes'
  cat >"$lv_work/retry-setup.sh" <<'SETUP'
set -eu
chmod 0777 /opt/multica/private
mkdir -p /opt/multica/private/agents /opt/multica/private/tmp /opt/multica/private/run
chown 65532:65532 /opt/multica/private/agents /opt/multica/private/tmp /opt/multica/private/run
printf blocker > /opt/multica/private/agents/.fixture
chown 65532:65532 /opt/multica/private/agents/.fixture
printf preserved > /opt/multica/private/run/keep
chown 65532:65532 /opt/multica/private/run/keep
stat -c '%i' /opt/multica/private/run/keep > /opt/multica/private/run/keep-inode
chown 65532:65532 /opt/multica/private/run/keep-inode
SETUP
  cat >"$lv_work/retry-layout.sh" <<'LAYOUT'
set -eu
if /opt/multica/controller/runtime home layout --private-root=/opt/multica/private --config-copy='{"sourceGroup":"provider","source":"/opt/multica/config-input/provider","target":"/home/multica/agents/.fixture"}'; then
  echo 'the copy obstruction did not fail' >&2
  exit 1
fi
test -f /opt/multica/private/run/configuration.json
touch /opt/multica/private/run/copy-failed
while [ ! -f /opt/multica/private/run/retry ]; do sleep 1; done
exec /opt/multica/controller/runtime home layout --private-root=/opt/multica/private --config-copy='{"sourceGroup":"provider","source":"/opt/multica/config-input/provider","target":"/home/multica/agents/.fixture"}'
LAYOUT
  cat >"$lv_work/retry-check.sh" <<'CHECK'
set -eu
test "$(cat /home/multica/agents/.fixture/operator.txt)" = configuration-A
test "$(cat /run/multica/keep)" = preserved
test "$(stat -c '%i' /run/multica/keep)" = "$(cat /run/multica/keep-inode)"
for path in /home/multica/agents /tmp /run/multica; do
  test "$(stat -c '%u' "$path")" = 65532
  mode=$(stat -c '%a' "$path")
  test "$((0$mode & 0777))" = 448
  parent=$(dirname "$path")
  while :; do
    test "$(stat -c '%u' "$parent")" = 0
    mode=$(stat -c '%a' "$parent")
    test "$((0$mode & 0022))" = 0
    test "$parent" != / || break
    parent=$(dirname "$parent")
  done
done
export CBM_CACHE_DIR="$HOME/.cache/codebase-memory-mcp"
export CBM_RUNTIME_DIR="$CBM_CACHE_DIR"
mkdir -p "$CBM_CACHE_DIR"
chmod 0700 "$HOME/.cache" "$CBM_CACHE_DIR"
find "$CBM_CACHE_DIR" -type f -exec chmod 0600 {} +
coproc CBM { exec /opt/multica/tools/bin/codebase-memory-mcp 2>/tmp/cbm.stderr; }
cbm_pid=$CBM_PID
exec {cbm_input}>&"${CBM[1]}"
exec {cbm_output}<&"${CBM[0]}"
trap 'kill "$cbm_pid" 2>/dev/null || :; wait "$cbm_pid" 2>/dev/null || :' EXIT
receive_cbm() {
  local expected=$1 line deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    if ! IFS= read -r -t 10 -u "$cbm_output" line; then continue; fi
    if jq -e --argjson id "$expected" '.id == $id' <<<"$line" >/dev/null 2>&1; then
      jq -e '.error == null and .result != null and .result.isError != true' <<<"$line" >/dev/null
      return
    fi
  done
  cat /tmp/cbm.stderr >&2
  return 1
}
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"k3s-private-fixture","version":"1.0.0"}}}' >&"$cbm_input"
receive_cbm 1
printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}' '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' >&"$cbm_input"
receive_cbm 2
printf '%s\n' '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_projects","arguments":{}}}' >&"$cbm_input"
receive_cbm 3
printf 'committed A, seed precedence, preserved inode, private ancestors and actual CBM MCP/IPC verified\n'
CHECK
  jq -cn --arg ns "$lv_namespace" '{apiVersion:"v1",kind:"ConfigMap",metadata:{name:"retry-provider",namespace:$ns},data:{"operator.txt":"configuration-A"}}' | lv_kube create -f -
  jq -cn --arg ns "$lv_namespace" --arg image "$lv_image_a" --arg arch "$lv_arch" --rawfile setup "$lv_work/retry-setup.sh" --rawfile layout "$lv_work/retry-layout.sh" --rawfile check "$lv_work/retry-check.sh" \
    'def secure:{runAsNonRoot:true,runAsUser:65532,runAsGroup:65532,allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}};
    {apiVersion:"v1",kind:"Pod",metadata:{name:"config-retry",namespace:$ns},spec:{restartPolicy:"Never",automountServiceAccountToken:false,nodeSelector:{"kubernetes.io/os":"linux","kubernetes.io/arch":$arch},securityContext:{runAsUser:65532,runAsGroup:65532,fsGroup:65532,seccompProfile:{type:"RuntimeDefault"}},volumes:[{name:"private",emptyDir:{}},{name:"provider",configMap:{name:"retry-provider",defaultMode:256}}],initContainers:[
      {name:"world-writable-fixture",image:$image,command:["/bin/sh","-ec",$setup],securityContext:(secure + {runAsNonRoot:false,runAsUser:0,runAsGroup:0,capabilities:{drop:["ALL"],add:["CHOWN","FOWNER","DAC_OVERRIDE"]}}),volumeMounts:[{name:"private",mountPath:"/opt/multica/private"}]},
      {name:"home-layout",image:$image,command:["/bin/sh","-ec",$layout],securityContext:secure,volumeMounts:[{name:"private",mountPath:"/opt/multica/private"},{name:"provider",mountPath:"/opt/multica/config-input/provider",readOnly:true}]}],containers:[
      {name:"check",image:$image,command:["/bin/bash","-ec",$check],securityContext:secure,volumeMounts:[{name:"private",mountPath:"/home/multica/agents",subPath:"agents"},{name:"private",mountPath:"/tmp",subPath:"tmp"},{name:"private",mountPath:"/run/multica",subPath:"run"}]}]}}' >"$lv_work/config-retry.json"
  lv_kube create -f - <"$lv_work/config-retry.json"
  lv_wait 'committed bundle followed by the intentional HOME copy failure' 120 lv_retry_capture_failed
  lv_kube -n "$lv_namespace" patch configmap retry-provider --type=merge -p '{"data":{"operator.txt":"configuration-B"}}'
  lv_wait 'updated B source projection' 180 lv_retry_projection_b
  lv_kube -n "$lv_namespace" exec config-retry -c home-layout -- sh -ec 'rm /opt/multica/private/agents/.fixture; touch /opt/multica/private/run/retry'
  lv_wait 'committed configuration copy retry and private runtime paths' 120 lv_pod_phase config-retry Succeeded
  lv_kube -n "$lv_namespace" logs config-retry -c check >"$lv_evidence/configuration-retry.log"
  lv_pass 'source A survives copy failure/B projection/retry; actual CBM starts with private native K3s HOME/tmp/run and visible ancestors'
}
lv_task_pod_pending() {
  local task=$1 raw
  raw=$(lv_kube -n "$lv_namespace" get pods -l "multica.ai/task-id=$task" -o json 2>/dev/null) || return
  jq -e '.items | length == 1 and (.[0].spec.nodeName // "") == "" and .[0].status.phase == "Pending"' <<<"$raw" >/dev/null
}
lv_task_started() {
  local task=$1 state
  state=$(lv_backend_state) || return
  if jq -e --arg task "$task" '.tasks[$task].failure != null or .tasks[$task].provider.error != null and .tasks[$task].provider.error != ""' <<<"$state" >/dev/null; then lv_fail 'the held worker failed before its provider checkpoint'; return 125; fi
  jq -e --arg task "$task" '.tasks[$task].provider != null' <<<"$state" >/dev/null
}
lv_task_complete() {
  local task=$1 state
  state=$(lv_backend_state) || return
  if jq -e --arg task "$task" '.tasks[$task].failure != null' <<<"$state" >/dev/null; then lv_fail 'the fixture task failed'; return 125; fi
  jq -e --arg task "$task" '.tasks[$task].completion != null and .tasks[$task].provider.stage == "completed"' <<<"$state" >/dev/null
}
lv_task_pod() {
  lv_kube -n "$lv_namespace" get pods -l "multica.ai/task-id=$1" -o json |
    jq -er 'if (.items | length) == 1 then .items[0].metadata.name else error("one task Pod required") end'
}
lv_release_task() {
  lv_backend -X POST -H 'Content-Type: application/json' --data '{}' "http://127.0.0.1:18080/fixture/release/$1" >/dev/null
  lv_wait 'held task completion' 180 lv_task_complete "$1"
}
lv_worker_content_rejection() {
  local worker=$1 expected
  expected=$(jq -er '.homeDigest | select(test("^[0-9a-f]{64}$"))' "$lv_evidence/pending-request.json")
  cat >"$lv_work/tamper-home.sh" <<'TAMPER'
set -eu
archive_source=/etc/multica/home/task-home.tar
scratch=/opt/multica/private/fixture-artifact
mkdir -m 0700 "$scratch" "$scratch/tree"
cp "$archive_source" "$scratch/original.tar"
test "$(sha256sum "$scratch/original.tar" | cut -d ' ' -f 1)" = "$1"
tar -xf "$scratch/original.tar" -C "$scratch/tree"
test -f "$scratch/tree/home/.fixture/operator.txt"
printf 'untrusted-worker-configuration\n' >"$scratch/tree/home/.fixture/operator.txt"
tar -cf "$scratch/task-home.tar" -C "$scratch/tree" identity.json home
after=$(sha256sum "$scratch/task-home.tar" | cut -d ' ' -f 1)
test "$after" != "$1"
test "$(tar -xOf "$scratch/task-home.tar" home/.fixture/operator.txt)" = untrusted-worker-configuration
printf 'HOME content substitution prepared: source=%s tampered=%s\n' "$1" "$after"
TAMPER
  lv_kube -n "$lv_namespace" get pod "$worker" -o json |
    jq --arg ns "$lv_namespace" --arg expected "$expected" --rawfile tamper "$lv_work/tamper-home.sh" '
      . as $pod | ($pod.spec.initContainers[] | select(.name == "home-layout")) as $home |
      ($home.volumeMounts[] | select(.mountPath == "/etc/multica/home/task-home.tar")) as $input |
      {apiVersion:"v1",kind:"Pod",metadata:{name:"worker-content-reject",namespace:$ns},spec:$pod.spec} |
      del(.spec.nodeName) |
      .spec.initContainers |= map(if .name == "home-layout" then
        .volumeMounts |= map(if .mountPath == "/etc/multica/home/task-home.tar" then
          {name:"runtime-private",mountPath:.mountPath,subPath:"fixture-artifact/task-home.tar",readOnly:true}
          else . end) else . end) |
      .spec.initContainers = ([{name:"tamper-home",image:$home.image,imagePullPolicy:$home.imagePullPolicy,
        command:["/bin/sh","-ec",$tamper,"tamper-home",$expected],securityContext:$home.securityContext,
        volumeMounts:[{name:"runtime-private",mountPath:"/opt/multica/private"},$input]}] + .spec.initContainers)
    ' >"$lv_work/worker-content-reject.json"
  lv_kube create -f - <"$lv_work/worker-content-reject.json"
  lv_wait 'worker HOME archive content rejection' 120 lv_pod_phase worker-content-reject Failed
  lv_kube -n "$lv_namespace" get pod worker-content-reject -o json >"$lv_evidence/worker-content-reject.json"
  lv_kube -n "$lv_namespace" logs worker-content-reject -c tamper-home >"$lv_evidence/worker-home-tamper.log"
  lv_kube -n "$lv_namespace" logs worker-content-reject -c home-layout >"$lv_evidence/worker-home-rejection.log"
  jq -e 'any(.status.initContainerStatuses[]?; .name == "tamper-home" and .state.terminated.exitCode == 0) and any(.status.initContainerStatuses[]?; .name == "home-layout" and .state.terminated.exitCode != null and .state.terminated.exitCode != 0) and all(.status.containerStatuses[]?; .state.running == null and .state.terminated == null)' "$lv_evidence/worker-content-reject.json" >/dev/null
  rg -q 'reason=task_home_digest_mismatch' "$lv_evidence/worker-home-rejection.log"
  lv_pass 'worker init rejects changed HOME archive contents before any provider process starts'
}
lv_tag_and_configuration_drift() {
  lv_stage 'Moving latest and operator sources while A workers keep their admitted inputs'
  local controller first first_worker second second_worker result current
  controller=$(lv_controller)
  lv_export_selection
  jq -e --arg image "$lv_image_a" '.runtimeRef.image == $image' "$lv_evidence/selection.json" >/dev/null
  first=$(lv_backend_state | jq -er .held)
  first_worker=$(lv_task_pod "$first")
  current=$(lv_push_fixture "$lv_fixture_b" latest)
  [[ $current == "$lv_image_b" ]] || lv_fail 'latest tag did not move to the independently built B manifest'
  lv_kube -n "$lv_namespace" patch configmap fixture-provider --type=merge -p '{"data":{"operator.txt":"configuration-B"}}'
  [[ $(lv_kube -n "$lv_namespace" exec "$controller" -c controller -- cat /home/multica/agents/.fixture/operator.txt) == configuration-A ]]
  [[ $(lv_kube -n "$lv_namespace" exec "$first_worker" -c worker -- cat /home/multica/agents/.fixture/operator.txt) == configuration-A ]]
  lv_kube -n "$lv_namespace" exec "$controller" -c controller -- sh -ec 'printf controller-private > /home/multica/agents/.fixture/operator.txt'
  lv_kube -n "$lv_namespace" exec "$first_worker" -c worker -- sh -ec 'printf worker-private > /home/multica/agents/.fixture/operator.txt'
  [[ $(lv_kube -n "$lv_namespace" exec "$controller" -c controller -- cat /home/multica/agents/.fixture/operator.txt) == controller-private ]]
  lv_kube cordon verification-node >/dev/null
  result=$(lv_backend -X POST -H 'Content-Type: application/json' --data '{"case":"pending-home-archive","scope":"b","transport":"http","hold":true}' http://127.0.0.1:18080/fixture/run)
  second=$(jq -er .input.taskID <<<"$result")
  lv_state --arg first "$first" --arg second "$second" '.heldOriginal=$first | .heldPending=$second'
  lv_wait 'worker pending while its source is removed' 90 lv_task_pod_pending "$second"
  second_worker=$(lv_task_pod "$second")
  lv_kube -n "$lv_namespace" get pod "$second_worker" -o json >"$lv_evidence/pending-worker.json"
  jq -e --arg image "$lv_image_a" 'all(.spec.containers[]; .image == $image) and all(.spec.initContainers[]; .image == $image)' "$lv_evidence/pending-worker.json" >/dev/null
  lv_kube -n "$lv_namespace" delete configmap fixture-provider --wait=true
  lv_kube uncordon verification-node >/dev/null
  lv_wait 'pending A worker uses its preserved HOME archive' 180 lv_task_started "$second"
  [[ $(lv_kube -n "$lv_namespace" exec "$second_worker" -c worker -- cat /home/multica/agents/.fixture/operator.txt) == configuration-A ]]
  [[ $(lv_kube -n "$lv_namespace" exec "$first_worker" -c worker -- cat /home/multica/agents/.fixture/operator.txt) == worker-private ]]
  lv_kube -n "$lv_namespace" get pod "$second_worker" -o json >"$lv_evidence/admitted-worker.json"
  jq -e --arg image "$lv_image_a" 'all(.status.containerStatuses[]; (.imageID | sub("^docker-pullable://";"")) == $image) and all(.status.initContainerStatuses[]; (.imageID | sub("^docker-pullable://";"")) == $image)' "$lv_evidence/admitted-worker.json" >/dev/null
  lv_kube -n "$lv_namespace" exec "$second_worker" -c worker -- cat /etc/multica/task/request.json >"$lv_evidence/pending-request.json"
  jq -e --slurpfile selected "$lv_evidence/selection.json" '.runtimeRef == $selected[0].runtimeRef and .ownerID == $selected[0].ownerID and (.homeDigest | test("^[0-9a-f]{64}$"))' "$lv_evidence/pending-request.json" >/dev/null
  lv_worker_content_rejection "$second_worker"
  lv_release_task "$first"
  lv_drive release
  lv_pass 'latest A→B, mutable source update/deletion and pending scheduling keep A image/platform and HOME contents; controller and worker HOME changes are isolated'
}
lv_access_absent() {
  local task kind
  while IFS= read -r task; do
    for kind in pods secrets; do
      lv_kube -n "$lv_namespace" get "$kind" -l "multica.ai/task-id=$task" -o json | jq -e '.items | length == 0' >/dev/null || return 1
    done
  done < <(jq -r '.positiveControl,.denied[].taskID' "$lv_evidence/access.json")
}
lv_access_checks() {
  lv_stage 'Checking observed task authority through the actual controller shims'
  local controller
  controller=$(lv_controller)
  lv_kube -n "$lv_namespace" exec -i "$controller" -c controller -- sh -ec 'cat > /tmp/fixture-access-request.json' <"$lv_evidence/request.json"
  lv_kube -n "$lv_namespace" exec "$controller" -c controller -- env LOCALVERIFY_DISPOSABLE_CLUSTER=true /usr/local/bin/verifyruntime access --request-file=/tmp/fixture-access-request.json >"$lv_evidence/access.json"
  jq -e '.passed == true' "$lv_evidence/access.json" >/dev/null
  lv_wait 'absence of denied access probe resources' 30 lv_access_absent
  lv_pass 'actual shims deny unobserved tasks, credential/scope mismatch and local-directory requests without retaining unauthorized resources'
}
lv_kubernetes_checks() {
  lv_stage 'Checking actual Kubernetes response loss, UID fencing, cleanup and scheduling'
  local index backend digest
  lv_export_selection
  cp "$lv_evidence/selection.json" "$lv_evidence/equivalent-selection-before.json"
  lv_backend_state | jq -e '[.tasks[] | select(.input.case == "environment-change" and .completion != null and .provider != null)] | if length == 1 then .[0] else error("one completed B continuation required") end' >"$lv_evidence/equivalent-before.json"
  index=$(jq -er .publishedImageB "$lv_work/state.json")
  digest=${index##*@}
  backend=$(lv_owned_container backend)
  docker exec "$backend" curl --fail --silent --show-error -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json' "http://$lv_registry:5000/v2/runtime/manifests/$digest" >"$lv_evidence/index-manifest.json"
  [[ "sha256:$(lv_sha <"$lv_evidence/index-manifest.json")" == "$digest" ]] || lv_fail 'registry index bytes differ from the selected index digest'
  jq -e --arg arch "$lv_arch" '(.mediaType == "application/vnd.oci.image.index.v1+json" or .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json") and ([.manifests[] | select(.platform.os == "linux" and .platform.architecture == $arch)] | length == 1)' "$lv_evidence/index-manifest.json" >/dev/null
  lv_run kubernetes.log docker exec --env LOCALVERIFY_DISPOSABLE_CLUSTER=true "$(lv_owned_container node)" /verifykube --kubeconfig /etc/rancher/k3s/k3s.yaml --namespace "$lv_namespace" --selection /verification-evidence/selection.json --request /verification-evidence/request.json --index-image "$index" --index-manifest /verification-evidence/index-manifest.json --evidence /verification-evidence/kubernetes.json
  lv_pass 'actual K3s create-response loss, payload/UID substitution, live controller authority, referenced Secret protection, cleanup recovery and fixed-node scheduling'
}
lv_equivalent_configuration() {
  lv_stage 'Continuing a native session after the actual controller Pod is recreated'
  local prior result task
  lv_export_selection
  cp "$lv_evidence/selection.json" "$lv_evidence/equivalent-selection-after.json"
  jq -e --slurpfile before "$lv_evidence/equivalent-selection-before.json" --slurpfile checks "$lv_evidence/kubernetes.json" '
    . as $after | $before[0] as $before |
    $after.controller.uid != $before.controller.uid and $after.runtimeRef == $before.runtimeRef and
    any($checks[0].checks[]; .name == "actual-controller-recreated" and .passed == true and
      .details.beforeUID == $before.controller.uid and .details.afterUID == $after.controller.uid)
  ' "$lv_evidence/equivalent-selection-after.json" >/dev/null
  prior=$(jq -er .input.taskID "$lv_evidence/equivalent-before.json")
  result=$(jq -cn --arg prior "$prior" '{case:"controller-object-continuation",scope:"a",transport:"http",priorTaskID:$prior}')
  task=$(lv_backend -X POST -H 'Content-Type: application/json' --data "$result" http://127.0.0.1:18080/fixture/run | jq -er .input.taskID)
  lv_wait 'same-runtime continuation under the new controller UID' 240 lv_task_complete "$task"
  lv_backend_state | jq -e --arg task "$task" '.tasks[$task]' >"$lv_evidence/equivalent-after.json"
  jq -e --slurpfile before "$lv_evidence/equivalent-before.json" '
    .provider as $after | $before[0].provider as $before |
    $after.priorWork == true and $after.priorSession == true and
    $after.storage == $before.storage and $after.branch == $before.branch and $after.session == $before.session and
    $after.runtimeRef == $before.runtimeRef
  ' "$lv_evidence/equivalent-after.json" >/dev/null
  lv_pass 'same logical image/configuration preserves real files, branch and Pi session across a verified controller UID change'
}
lv_transport_checks() {
  lv_stage 'Severing an active Kubernetes execution stream'
  lv_export_selection
  lv_run transport.log docker exec --env LOCALVERIFY_DISPOSABLE_CLUSTER=true "$(lv_owned_container node)" /verifystream --kubeconfig /etc/rancher/k3s/k3s.yaml --namespace "$lv_namespace" --selection /verification-evidence/selection.json --request /verification-evidence/request.json --evidence /verification-evidence/transport.json
  lv_pass 'actual exec transport severance preserves worker files and leaves UID-fenced recovery authority'
}
lv_runtime_scenarios() {
  lv_step admission-native lv_verify_binding
  lv_step install-a lv_install A
  lv_step baseline lv_drive baseline
  lv_step mixed-image lv_mixed_image
  lv_step configuration-retry lv_configuration_retry
  lv_step journal-failure lv_journal_failure
  lv_step cleanup-failure lv_cleanup_failure
  lv_step cleanup-recovered lv_cleanup_recovered
  lv_step interrupted-start lv_drive interrupted-start
  lv_step interrupted-recovered lv_interrupted_recovered
  lv_step hold lv_drive hold
  lv_step image-and-config-drift lv_tag_and_configuration_drift
  lv_step install-b lv_install B
  lv_step changed lv_drive changed
  lv_step access lv_access_checks
  lv_step kubernetes lv_kubernetes_checks
  lv_step equivalent-controller lv_equivalent_configuration
  lv_step transport lv_transport_checks
}

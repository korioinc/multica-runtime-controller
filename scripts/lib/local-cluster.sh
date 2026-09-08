#!/usr/bin/env bash
# Local image/registry/K3s construction. Never uses the ambient kubeconfig.
# Globals are shared with verify-local.sh; lv_state consumes literal jq expressions.
# shellcheck disable=SC2034,SC2154,SC2016

lv_verify_base() {
  lv_stage 'Building and executing the CLI-free controller base'
  local tag="multica-runtime-verify:$lv_name-base" revision
  revision=$(git -C "$lv_repository" rev-parse HEAD)
  lv_run base-build.log make -C "$lv_repository" image "IMAGE=$tag" "PLATFORM=linux/$lv_arch" VERSION=dev "COMMIT=$revision"
  lv_image_record "$tag"
  lv_run base-native.log "$lv_repository/.github/scripts/verify-base.sh" "$tag" "linux/$lv_arch" dev "$revision"
  lv_pass "native linux/$lv_arch controller base: installed build/shims and Go SDK execute without an official CLI"
}
lv_build_fixtures() {
  lv_stage 'Checking the explicit runtime image and building isolated provider fixtures'
  local input_id
  input_id=$(docker image inspect "$lv_input_image" | jq -er 'if length == 1 then .[0].Id else error("ambiguous selected image") end')
  lv_state --arg id "$input_id" '.inputImageID=$id'
  lv_fixture_a="multica-runtime-verify:$lv_name-a"
  lv_fixture_b="multica-runtime-verify:$lv_name-b"
  mkdir -p "$lv_work/context/a" "$lv_work/context/b"
  lv_run runtime-image.log docker run --rm --network none --read-only "$input_id" image verify
  lv_run fixture-a-build.log "$lv_repository/scripts/local-fixture-image.sh" --runtime-image "$input_id" --tag "$lv_fixture_a" --platform "linux/$lv_arch" --build-id "$(lv_uuid)" --context "$lv_work/context/a"
  lv_image_record "$lv_fixture_a"
  lv_run fixture-b-build.log "$lv_repository/scripts/local-fixture-image.sh" --runtime-image "$input_id" --tag "$lv_fixture_b" --platform "linux/$lv_arch" --build-id "$(lv_uuid)" --context "$lv_work/context/b"
  lv_image_record "$lv_fixture_b"
  lv_state --arg a "$lv_fixture_a" --arg b "$lv_fixture_b" '.fixtureA=$a | .fixtureB=$b'
  lv_check_source
}
lv_backend_ready() { lv_backend http://127.0.0.1:18080/fixture/health >/dev/null 2>&1; }
lv_api_ready() { [[ $(lv_kube get --raw /readyz 2>/dev/null) == ok ]]; }
lv_registry_ready() {
  local registry
  registry=$(lv_owned_container registry) || return
  docker exec "$registry" wget -q -O /dev/null http://127.0.0.1:5000/v2/ >/dev/null 2>&1
}
lv_push_fixture() {
  local source=$1 tag=$2 host_ref actual
  host_ref="127.0.0.1:$lv_registry_port/runtime:$tag"
  lv_owned_container registry >/dev/null || return
  lv_export_oci "$source" || return
  printf '{"auths":{}}\n' >"$lv_work/registry-auth.json" || return
  lv_run "registry-push-$tag.log" oras cp --from-oci-layout "$lv_copy_source" --to-plain-http --to-registry-config "$lv_work/registry-auth.json" --no-tty "$host_ref" || return
  oras manifest fetch --plain-http --registry-config "$lv_work/registry-auth.json" "$host_ref" --output "$lv_work/context/registry-$tag.manifest" || return
  actual="sha256:$(lv_sha <"$lv_work/context/registry-$tag.manifest")"
  [[ $actual == "$lv_copy_digest" ]] || { lv_fail 'registry changed the copied OCI manifest bytes'; return 1; }
  printf '%s:5000/runtime@%s\n' "$lv_registry" "$actual"
}
lv_start_cluster() {
  lv_stage 'Starting an owned local registry, backend and disposable K3s node'
  local network identifier backend_ip node port configuration helper
  network=$(docker network create --label "io.multica.localverify=$lv_name" "$lv_name-network")
  lv_state --arg id "$network" '.networkID=$id'
  identifier=$(lv_create_container registry "$lv_registry" --network "$network" -p 127.0.0.1::5000 registry:2.8.3)
  docker start "$identifier" >/dev/null
  lv_wait 'owned local registry' 60 lv_registry_ready
  lv_registry_port=$(docker port "$identifier" 5000/tcp | awk -F: '/^127\.0\.0\.1:/ {print $NF}')
  [[ $lv_registry_port =~ ^[0-9]+$ ]] || lv_fail 'registry must have exactly one loopback port'
  lv_state --arg port "$lv_registry_port" '.registryPort=$port'
  lv_image_a=$(lv_push_fixture "$lv_fixture_a" latest)
  lv_image_b=$(lv_push_fixture "$lv_fixture_b" candidate-b)
  lv_state --arg a "$lv_image_a" --arg b "$lv_image_b" '.imageA=$a | .imageB=$b | .publishedImageA=$a | .publishedImageB=$b'
  identifier=$(lv_create_container backend "$lv_name-backend" --network "$network" \
    --env LOCALVERIFY_DISPOSABLE_CLUSTER=true --mount "type=bind,src=$lv_evidence,dst=/evidence" \
    --tmpfs /git:rw,mode=0700,uid=65532,gid=65532 --tmpfs /tmp:rw,mode=0700,uid=65532,gid=65532 \
    --entrypoint /bin/sleep "$lv_fixture_a" infinity)
  docker start "$identifier" >/dev/null
  backend_ip=$(docker inspect "$identifier" | jq -er --arg network "$lv_name-network" '.[0].NetworkSettings.Networks[$network].IPAddress')
  lv_backend_origin="http://$backend_ip:18080"
  lv_state --arg origin "$lv_backend_origin" '.backendOrigin=$origin'
  docker exec --detach "$identifier" /usr/local/bin/verifyruntime backend --origin "$lv_backend_origin" --evidence /evidence
  lv_wait 'owned backend process' 30 lv_backend_ready
  jq -cn --arg registry "$lv_registry:5000" '{mirrors:{($registry):{endpoint:[("http://"+$registry)]}}}' >"$lv_work/registries.json"
  node=$(lv_create_container node "$lv_name-node" --privileged --network "$network" --tmpfs /run --tmpfs /var/run \
    -p 127.0.0.1::6443 --mount "type=bind,src=$lv_evidence,dst=/verification-evidence" \
    --mount "type=bind,src=$lv_work/registries.json,dst=/etc/rancher/k3s/registries.yaml,readonly" \
    rancher/k3s:v1.34.3-k3s1 server --node-name verification-node --disable traefik --disable servicelb --disable metrics-server \
    --kube-apiserver-arg enable-admission-plugins=OwnerReferencesPermissionEnforcement)
  docker start "$node" >/dev/null
  printf 'Live K3s container: %s %s\n' "$lv_name-node" "$node"
  lv_wait 'owned K3s API' 300 lv_api_ready
  lv_kube wait --for=condition=Ready node/verification-node --timeout=180s
  port=$(docker port "$node" 6443/tcp | awk -F: '/^127\.0\.0\.1:/ {print $NF}')
  [[ $port =~ ^[0-9]+$ ]] || lv_fail 'K3s must have exactly one loopback API port'
  configuration=$(docker exec "$node" cat /etc/rancher/k3s/k3s.yaml)
  [[ $configuration == *https://127.0.0.1:6443* ]] || lv_fail 'K3s did not supply its node-local kubeconfig'
  printf '%s\n' "$configuration" | sed "s#https://127.0.0.1:6443#https://127.0.0.1:$port#g" >"$lv_kubeconfig"
  chmod 0600 "$lv_kubeconfig"
  for helper in verifykube verifystream; do
    [[ -x $lv_work/context/a/$helper ]] || lv_fail "fixture build did not produce $helper"
    docker cp "$lv_work/context/a/$helper" "$node:/$helper"
  done
  docker exec "$node" sh -ec 'mkdir -p /verification-workspace /verification-rejected; chown 65532:65532 /verification-workspace /verification-rejected'
  lv_initialize_storage
  lv_make_values
}

lv_verify_binding() {
  lv_stage 'Running production B against an A receipt and stale A API image status'
  local identifier prior
  prior=$(jq -er .publishedImageA "$lv_work/state.json")
  [[ -x $lv_work/context/b/verifybinding ]] || lv_fail 'fixture build did not produce the native image-binding verifier'
  chmod 0555 "$lv_work/context/b/verifybinding"
  docker run --rm --network none --read-only --entrypoint /bin/cat "$lv_fixture_a" /opt/multica/runtime/image.json >"$lv_evidence/prior-image.json"
  chmod 0644 "$lv_evidence/prior-image.json"
  identifier=$(lv_create_container binding "$lv_name-binding" --network none --read-only --user 65532:65532 \
    --env LOCALVERIFY_DISPOSABLE_CONTAINER=true \
    --mount "type=bind,src=$lv_work/context/b/verifybinding,dst=/verification-tools/verifybinding,readonly" \
    --mount "type=bind,src=$lv_evidence,dst=/evidence" \
    --tmpfs /home/multica/agents:rw,exec,mode=0700,uid=65532,gid=65532 \
    --tmpfs /tmp:rw,exec,mode=0700,uid=65532,gid=65532 \
    --tmpfs /run/multica:rw,exec,mode=0700,uid=65532,gid=65532 \
    --tmpfs /workspace:rw,exec,mode=0700,uid=65532,gid=65532 \
    --tmpfs /var/run/secrets/kubernetes.io/serviceaccount:rw,mode=0700,uid=65532,gid=65532 \
    --entrypoint /verification-tools/verifybinding "$lv_fixture_b" --prior-descriptor /evidence/prior-image.json --prior-image "$prior" --evidence /evidence/binding.json)
  lv_run binding.log docker start --attach "$identifier"
  docker inspect "$identifier" | jq -e '.[0].State.Running == false and .[0].State.ExitCode == 0' >/dev/null
  lv_pass 'actual B rootfs rejects A receipt and stale A API status before backend/workspace admission; native positive image-binding controls passed'
}
lv_initialize_storage() {
  jq -cn --arg namespace "$lv_namespace" '{apiVersion:"v1",kind:"List",items:[
    {apiVersion:"v1",kind:"Namespace",metadata:{name:$namespace}},
    (["workspace","rejected"][] | . as $name |
      {apiVersion:"v1",kind:"PersistentVolume",metadata:{name:("verification-"+$name)},spec:{capacity:{storage:"8Gi"},accessModes:["ReadWriteOnce"],persistentVolumeReclaimPolicy:"Retain",storageClassName:"",hostPath:{path:("/verification-"+$name),type:"Directory"}}},
      {apiVersion:"v1",kind:"PersistentVolumeClaim",metadata:{name:$name,namespace:$namespace},spec:{accessModes:["ReadWriteOnce"],resources:{requests:{storage:"8Gi"}},storageClassName:"",volumeName:("verification-"+$name)}}),
    {apiVersion:"v1",kind:"Secret",metadata:{name:"fixture-token",namespace:$namespace},stringData:{token:"mul_disposable_chart_fixture"}},
    {apiVersion:"v1",kind:"ConfigMap",metadata:{name:"fixture-provider",namespace:$namespace},data:{"operator.txt":"configuration-A"}}
  ]}' | lv_kube apply -f -
}
lv_make_values() {
  jq -cn --arg image "$lv_registry:5000/runtime:latest" --arg platform "linux/$lv_arch" --arg backend "$lv_backend_origin" \
    '{image:$image,imagePullPolicy:"Always",platform:$platform,
      runtime:{pollInterval:"1s",heartbeatInterval:"1s",startupTimeout:"120s"},
      workspace:{storage:{existingClaim:"workspace",size:"",accessMode:"ReadWriteOnce"}},
      scheduling:{singleNodeName:"verification-node"},
      multica:{baseURL:$backend,controllerTokenSecret:{name:"fixture-token",key:"token"}},
      operator:{configVolumes:[{name:"provider",configMap:{name:"fixture-provider",items:[{key:"operator.txt",path:"operator.txt"}]}}],configMounts:[{name:"provider",mountPath:"/home/multica/agents/.fixture",readOnly:true}]},
      controller:{resources:{requests:{cpu:"50m",memory:"128Mi"},limits:{cpu:"2",memory:"1Gi"}}},
      worker:{taskDeadline:600,resources:{requests:{cpu:"50m",memory:"128Mi"},limits:{cpu:"1",memory:"512Mi"}}}}
    ' >"$lv_work/values-A.json"
}
lv_install() {
  local generation=$1 expected_build actual_image fixture
  lv_stage "Installing the actual chart with runtime $generation"
  if [[ $generation == B ]]; then
    jq '.controller.podAnnotations["fixture.multica.ai/generation"]="B"' "$lv_work/values-A.json" >"$lv_work/values-B.json"
    jq -cn --arg namespace "$lv_namespace" '{apiVersion:"v1",kind:"ConfigMap",metadata:{name:"fixture-provider",namespace:$namespace},data:{"operator.txt":"configuration-B"}}' | lv_kube apply -f -
  fi
  lv_run "helm-$generation.log" helm upgrade --install verify "$lv_chart" --kubeconfig "$lv_kubeconfig" --namespace "$lv_namespace" --values "$lv_work/values-$generation.json" --wait --timeout 5m
  lv_export_selection
  if [[ $generation == A ]]; then fixture=$lv_fixture_a; else fixture=$lv_fixture_b; fi
  expected_build=$(docker run --rm --network none --read-only --entrypoint /bin/cat "$fixture" /opt/multica/runtime/image.json | jq -er .imageBuildID)
  jq -e --arg build "$expected_build" --arg platform "linux/$lv_arch" '.runtimeRef.imageBuildID == $build and .runtimeRef.platform == $platform' "$lv_evidence/selection.json" >/dev/null
  actual_image=$(jq -er .runtimeRef.image "$lv_evidence/selection.json")
  [[ $actual_image == "$lv_registry:5000/runtime@sha256:"* ]] || lv_fail 'controller did not bind a pullable manifest from the owned registry'
  # A CRI may report the pulled index or the selected manifest. Preserve its
  # actual admitted repository digest instead of guessing the child identity.
  if [[ $generation == A ]]; then lv_image_a=$actual_image; lv_state --arg image "$actual_image" '.imageA=$image'
  else lv_image_b=$actual_image; lv_state --arg image "$actual_image" '.imageB=$image'; fi
}
lv_controller_ready() {
  local raw count
  raw=$(lv_kube -n "$lv_namespace" get pods -l app.kubernetes.io/instance=verify,app.kubernetes.io/component=controller -o json 2>/dev/null) || return
  count=$(jq '[.items[] | select(.metadata.deletionTimestamp == null and any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' <<<"$raw") || return
  [[ $count == 1 ]] || return 1
  jq -r '.items[] | select(.metadata.deletionTimestamp == null and any(.status.conditions[]?; .type == "Ready" and .status == "True")) | .metadata.name' <<<"$raw" >"$lv_work/controller-name"
}
lv_controller() {
  lv_wait 'single current controller authority' 180 lv_controller_ready >&2 || return
  cat "$lv_work/controller-name"
}
lv_export_selection() {
  local controller
  controller=$(lv_controller)
  lv_kube -n "$lv_namespace" exec "$controller" -c controller -- cat /run/multica/selection.json >"$lv_evidence/selection.json"
}
lv_drive() {
  local backend
  lv_stage "Exercising actual runtime phase $1"
  backend=$(lv_owned_container backend)
  lv_run "runtime-$1.log" docker exec "$backend" /usr/local/bin/verifyruntime drive --backend http://127.0.0.1:18080 --phase "$1"
}
lv_rendered_pod() {
  # kubectl owns YAML decoding; jq collects its stream of native JSON objects.
  helm template verify "$lv_chart" --namespace "$lv_namespace" --values "$lv_work/values-A.json" |
    lv_kube create --dry-run=client -f - -o json |
    jq -s '[.[] | if .kind == "List" then .items[] else . end] | map(select(.kind == "Deployment")) | if length == 1 then .[0].spec.template else error("one controller template required") end'
}

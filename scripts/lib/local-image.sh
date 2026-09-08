#!/usr/bin/env bash
# Copy immutable image bytes through the host's reachable registry endpoint.
# Docker Desktop's daemon loopback is not the host loopback published by -p.
# Globals are supplied by verify-local.sh.
# shellcheck disable=SC2034,SC2154

lv_blob() {
  local file=$1 media=$2 digest size
  digest=$(lv_sha <"$file") || return
  size=$(wc -c <"$file" | tr -d '[:space:]') || return
  mv "$file" "$lv_layout/blobs/sha256/$digest" || return
  jq -cn --arg media "$media" --arg digest "sha256:$digest" --argjson size "$size" '{mediaType:$media,digest:$digest,size:$size}'
}
lv_classic_oci() {
  local archive=$1 image_id=$2 manifest config member descriptor index_descriptor
  lv_layout=$3
  mkdir -p "$lv_layout/blobs/sha256" || return
  manifest=$(tar -xOf "$archive" manifest.json | jq -ce 'if length == 1 then .[0] else error("one exported image required") end') || return
  config=$(jq -er .Config <<<"$manifest") || return
  tar -xOf "$archive" "$config" >"$lv_layout/config" || return
  [[ "sha256:$(lv_sha <"$lv_layout/config")" == "$image_id" ]] || { lv_fail 'classic export config differs from the selected image ID'; return 1; }
  jq -e --arg arch "$lv_arch" '.os == "linux" and .architecture == $arch' "$lv_layout/config" >/dev/null || return
  cp "$lv_layout/config" "$lv_layout/config-inspection.json" || return
  descriptor=$(lv_blob "$lv_layout/config" application/vnd.oci.image.config.v1+json) || return
  printf '%s\n' "$descriptor" >"$lv_layout/config-descriptor.json" || return
  : >"$lv_layout/layers.jsonl"
  while IFS= read -r member; do
    tar -xOf "$archive" "$member" >"$lv_layout/layer" || return
    lv_blob "$lv_layout/layer" application/vnd.oci.image.layer.v1.tar >>"$lv_layout/layers.jsonl" || return
  done < <(jq -r '.Layers[]' <<<"$manifest")
  jq -s . "$lv_layout/layers.jsonl" >"$lv_layout/layers.json" || return
  # Classic archives export uncompressed layers. Match every diff_id in order
  # before wrapping the unchanged config/layer bytes in a pullable OCI index.
  jq -e --slurpfile layers "$lv_layout/layers.json" '.rootfs.type == "layers" and .rootfs.diff_ids == [$layers[0][].digest]' "$lv_layout/config-inspection.json" >/dev/null || { lv_fail 'classic image layers do not preserve their declared filesystem hashes'; return 1; }
  jq -cn --slurpfile config "$lv_layout/config-descriptor.json" --slurpfile layers "$lv_layout/layers.json" \
    '{schemaVersion:2,mediaType:"application/vnd.oci.image.manifest.v1+json",config:$config[0],layers:$layers[0]}' >"$lv_layout/manifest" || return
  descriptor=$(lv_blob "$lv_layout/manifest" application/vnd.oci.image.manifest.v1+json) || return
  jq -cn --argjson manifest "$descriptor" --arg arch "$lv_arch" '{schemaVersion:2,mediaType:"application/vnd.oci.image.index.v1+json",manifests:[($manifest + {platform:{os:"linux",architecture:$arch}})]}' >"$lv_layout/image-index" || return
  index_descriptor=$(lv_blob "$lv_layout/image-index" application/vnd.oci.image.index.v1+json) || return
  lv_copy_digest=$(jq -er .digest <<<"$index_descriptor") || return
  jq -cn --argjson index "$index_descriptor" '{schemaVersion:2,mediaType:"application/vnd.oci.image.index.v1+json",manifests:[($index + {annotations:{"org.opencontainers.image.ref.name":"fixture"}})]}' >"$lv_layout/index.json" || return
  printf '{"imageLayoutVersion":"1.0.0"}\n' >"$lv_layout/oci-layout" || return
  lv_copy_source="$lv_layout@$lv_copy_digest"
}
lv_export_oci() {
  local source=$1 image_id archive path
  image_id=$(docker image inspect "$source" | jq -er 'if length == 1 then .[0].Id else error("one local image required") end') || return
  path="$lv_work/context/export-${image_id#sha256:}"
  archive=$path.tar
  if [[ ! -f $archive ]]; then
    docker image save --output "$archive.pending" "$image_id" || return
    mv "$archive.pending" "$archive" || return
  fi
  if tar -tf "$archive" | awk '$0 == "oci-layout" { found=1 } END { exit !found }'; then
    oras manifest fetch --oci-layout "$archive@$image_id" --output "$path.manifest" || return
    [[ "sha256:$(lv_sha <"$path.manifest")" == "$image_id" ]] || { lv_fail 'exported OCI root differs from the selected local image'; return 1; }
    lv_copy_source="$archive@$image_id"
    lv_copy_digest=$image_id
  else
    lv_classic_oci "$archive" "$image_id" "$path-layout" || return
  fi
}

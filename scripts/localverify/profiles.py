#!/usr/bin/env python3
"""Prepare real chart environments with one core artifact, then execute K3s workers.

--prepare-only records a reusable Docker checkpoint; it does not claim worker proof.
All Kubernetes access goes through the explicitly guarded disposable node container.
"""
from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

# An explicit input for these installation examples, not a chart default.
EXAMPLE_IMAGE = "docker.io/library/buildpack-deps:bookworm-scm@sha256:4274ea4975976f86239384ac206f98af5f0978fd8f054886eec1371fc7664025"


class Profiles:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.workdir).resolve(strict=True)
        self.evidence = self.root / "evidence"
        self.chart = Path(args.chart).resolve(strict=True)
        self.repo = Path(__file__).resolve().parents[2]
        if os.environ.get("LOCALVERIFY_DISPOSABLE_CLUSTER") != "true":
            raise RuntimeError("LOCALVERIFY_DISPOSABLE_CLUSTER=true is required")
        state = json.loads((self.root / "state.json").read_text())
        if (state.get("root") != str(self.root)
            or state.get("evidence") != str(self.evidence)
            or state.get("node") != args.node
            or state.get("core_volume") != args.core_volume
            or state.get("core_image") != args.core_image
            or state.get("arch") != args.arch
            or args.arch not in ("amd64", "arm64")
            or not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", args.core_image)):
            raise RuntimeError("profile inputs do not match the disposable run state")
        if self.evidence.resolve(strict=True) != self.evidence:
            raise RuntimeError("evidence directory must be canonical")
        node = self.json_command(["docker", "inspect", args.node])[0]
        if not node["State"]["Running"] or not any(
            mount["Destination"] == "/verification-evidence"
            and Path(mount["Source"]).resolve() == self.evidence
            for mount in node["Mounts"]
        ):
            raise RuntimeError("node is not the live disposable evidence-bound container")
        self.prefix = "multica-profiles-" + hashlib.sha256(str(self.root).encode()).hexdigest()[:10]
        self.output = self.evidence / "profiles"
        self.output.mkdir(exist_ok=True)
        previous_resources = self.root / "profile-resources.json"
        previous = json.loads(previous_resources.read_text()) if previous_resources.exists() else {}
        if previous and previous.get("root") != str(self.root):
            raise RuntimeError("profile resource checkpoint belongs to another run")
        self.volumes: set[str] = set(previous.get("volumes", []))
        self.images: set[str] = set(previous.get("images", {}))
        self.image_ids = previous.get("images", {}) if isinstance(previous.get("images", {}), dict) else {}
        self.core_mount = ["--mount", f"type=volume,src={args.core_volume},dst=/opt/multica/core,readonly"]
        self.platform = "linux/" + args.arch
        self.secret = "profile-install-" + uuid.uuid4().hex
        self.owner = self.owner_id()
        self.binary = self.root / "verifyprofile-linux"
        self.run(["go", "build", "-o", str(self.binary), "./cmd/verifyprofile"],
                 "build-profile-helper", cwd=self.repo / "src",
                 env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": args.arch})

    def run(self, argv, phase, *, cwd=None, env=None, data=None):
        log = self.output / (phase + ".log")
        with log.open("wb") as stream:
            result = subprocess.run(argv, cwd=cwd, env=env, input=data, stdout=stream, stderr=subprocess.STDOUT)
        if result.returncode:
            raise RuntimeError(f"{phase} failed ({result.returncode}); inspect {log}")
        return log.read_bytes()

    @staticmethod
    def json_command(argv):
        result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return json.loads(result.stdout)

    def node_command(self, argv, phase, *, data=None):
        return self.run(["docker", "exec", "-i", self.args.node, *argv], phase, data=data)

    def kubectl(self, argv, phase, *, data=None):
        return self.node_command(["kubectl", "--kubeconfig", "/etc/rancher/k3s/k3s.yaml", *argv], phase, data=data)

    def owner_id(self):
        selection = self.evidence / "selection.json"
        if selection.exists():
            return json.loads(selection.read_text())["ownerID"]
        secret = self.json_command([
            "docker", "exec", self.args.node, "kubectl", "--kubeconfig", "/etc/rancher/k3s/k3s.yaml",
            "-n", "runtime-verify", "get", "secret", "verify-multica-runtime-controller-identity", "-o", "json"])
        return str(uuid.UUID(base64.b64decode(secret["data"]["daemon-id"]).decode()))

    def volume(self, suffix, image):
        name = self.prefix + "-" + suffix
        self.run(["docker", "volume", "create", "--label", "multica.io/verification-root=" + str(self.root), name], suffix + "-volume")
        record = self.json_command(["docker", "volume", "inspect", name])[0]
        if record.get("Labels", {}).get("multica.io/verification-root") != str(self.root):
            raise RuntimeError("refusing an unowned Docker volume")
        self.volumes.add(name)
        self.record_resources()
        self.run(["docker", "run", "--rm", "--user", "0", "--mount", f"type=volume,src={name},dst=/owned-volume",
                  image, "/bin/bash", "-ec", "chown 65532:65532 /owned-volume"], suffix + "-permissions")
        return name

    def record_resources(self):
        for name in sorted(self.images):
            current = self.json_command(["docker", "image", "inspect", name])[0]["Id"]
            if name in self.image_ids and self.image_ids[name] != current:
                raise RuntimeError("owned image alias was changed after its checkpoint")
            self.image_ids[name] = current
        (self.root / "profile-resources.json").write_text(json.dumps({
            "root": str(self.root), "node": self.args.node,
            "volumes": sorted(self.volumes), "images": self.image_ids
        }, indent=2) + "\n")

    def container(self, name, image, mounts, env, command, *, log_suffix=None):
        argv = ["docker", "run", "--rm", "--name", self.prefix + "-" + name,
                "--platform", self.platform, "--user", "65532:65532", "--read-only",
                "--tmpfs", "/tmp:rw,exec,uid=65532,gid=65532",
                "--tmpfs", "/home/multica/agents:rw,exec,uid=65532,gid=65532",
                "--tmpfs", "/workspace:rw,exec,uid=65532,gid=65532", *self.core_mount, *mounts]
        for key, value in env.items():
            argv.extend(["--env", key + "=" + str(value)])
        return self.run([*argv, image, *command], name + (log_suffix or ""))

    def example_input(self):
        values = {"runtime": {"image": {"reference": self.args.core_image}}, "environment": {
            "image": {"reference": EXAMPLE_IMAGE}, "platform": self.platform,
            "providers": ["pi", "codex", "copilot", "antigravity"],
            "bootstrap": {"source": "bundled"}}}
        path = self.output / "example-values.json"
        path.write_text(json.dumps(values) + "\n")
        raw = self.run(["helm", "template", "verify", str(self.chart), "--namespace", "runtime-verify",
                        "--values", str(path), "--show-only", "templates/environment-config.yaml"], "render-profile-example")
        # input.json is itself encoded as a JSON-compatible quoted scalar by the
        # chart; parse those JSON bytes instead of implementing a YAML loader.
        entries = [line.strip().removeprefix("input.json: ") for line in raw.decode().splitlines()
                   if line.startswith("  input.json: ")]
        if len(entries) != 1:
            raise RuntimeError("rendered chart did not supply one environment input")
        return json.loads(json.loads(entries[0]))

    def image_reference(self, tag):
        record = self.json_command(["docker", "image", "inspect", tag])[0]
        references = record.get("RepoDigests", [])
        if not references:
            raise RuntimeError("built image has no OCI repository digest")
        repository = tag.rsplit(":", 1)[0]
        wanted = "docker.io/library/" + repository if "/" not in repository else repository
        for reference in references:
            if reference.split("@", 1)[0].split("/")[-1] == repository.split("/")[-1]:
                return wanted + "@" + reference.split("@", 1)[1]
        raise RuntimeError("built image digest does not belong to the profile repository")

    def prepare(self, name, script_name, base_input, image):
        print("profile", name + ": actual core preparation", flush=True)
        folder = self.output / name
        folder.mkdir(exist_ok=True)
        script = self.chart / "files" / "environments" / script_name
        input_value = {**base_input, "environmentImage": image, "revision": "local-profile-" + name,
                       "scriptSHA256": hashlib.sha256(script.read_bytes()).hexdigest(),
                       "inputs": {"profile": name}}
        input_file = folder / "input.json"
        input_file.write_text(json.dumps(input_value, ensure_ascii=False, sort_keys=True) + "\n")
        input_mount = ["--mount", f"type=bind,src={folder},dst=/profile,readonly"]
        input_env = {"MULTICA_ENVIRONMENT_INPUT_FILE": "/profile/input.json"}
        identity = self.container(name + "-identity", image, input_mount, input_env,
                                  ["/opt/multica/core/runtime", "environment", "identity"]).decode().strip()
        if not re.fullmatch(r"[0-9a-f]{64}", identity):
            raise RuntimeError("core did not return an environment identity")
        tools = self.volume(name + "-tools", image)
        control = self.volume(name + "-control", image)
        self.container(name + "-layout", image,
                       [*input_mount, "--mount", f"type=volume,src={tools},dst=/tools-store"],
                       {**input_env, "MULTICA_ENVIRONMENT_ID": identity, "MULTICA_TOOLS_STORE": "/tools-store", "MULTICA_OWNER_ID": self.owner},
                       ["/opt/multica/core/runtime", "environment", "layout"])
        self.container(name + "-capture", image,
                       ["--mount", f"type=volume,src={control},dst=/run/multica"],
                       {"MULTICA_ENVIRONMENT_BASE_ENV_FILE": "/run/multica/image-env.json"},
                       ["/opt/multica/core/runtime", "environment", "capture"])
        generation_mount = f"type=volume,src={tools},dst=/opt/multica/environment,volume-subpath=generations/{identity}"
        prepare_mounts = [*input_mount,
                          "--mount", generation_mount,
                          "--mount", f"type=volume,src={control},dst=/run/multica,readonly",
                          "--mount", f"type=volume,src={tools},dst=/run/multica/prepare.lock,volume-subpath=locks/{identity}.lock",
                          "--mount", f"type=bind,src={script},dst=/bootstrap.sh,readonly"]
        env = {**input_env, "MULTICA_ENVIRONMENT_ID": identity,
               "MULTICA_ENVIRONMENT_SCRIPT_FILE": "/bootstrap.sh",
               "MULTICA_ENVIRONMENT_BASE_ENV_FILE": "/run/multica/image-env.json",
               "MULTICA_ENVIRONMENT_LOCK_FILE": "/run/multica/prepare.lock",
               "MULTICA_ENVIRONMENT_TIMEOUT_SECONDS": "1200", "PROFILE_INSTALL_ONLY": self.secret}
        self.container(name + "-prepare", image, prepare_mounts, env,
                       ["/opt/multica/core/runtime", "environment", "prepare"])
        read_mounts = [*input_mount, "--mount", generation_mount + ",readonly"]
        ready = self.container(name + "-read-ready", image, read_mounts, {}, ["/bin/cat", "/opt/multica/environment/READY"])
        if self.secret.encode() in ready:
            raise RuntimeError("installation credential entered READY metadata")
        (folder / "ready.json").write_bytes(ready)
        manifest = json.loads(self.container(name + "-read-manifest", image, read_mounts, {}, ["/bin/cat", "/opt/multica/environment/environment.json"]))
        # A second real prepare must accept the immutable READY and leave it intact.
        self.container(name + "-reuse", image, prepare_mounts, env,
                       ["/opt/multica/core/runtime", "environment", "prepare"])
        if self.container(name + "-reused-ready", image, read_mounts, {}, ["/bin/cat", "/opt/multica/environment/READY"]) != ready:
            raise RuntimeError("valid READY changed when preparation was reused")
        self.container(name + "-core-check", image, read_mounts, input_env,
                       ["/opt/multica/core/runtime", "environment", "check"])
        consumer_output = folder / "consumer-output"
        consumer_output.mkdir(exist_ok=True)
        consumer_output.chmod(0o777)
        consumer_mounts = ["--mount", f"type=bind,src={folder},dst=/profile,readonly",
                           "--mount", f"type=bind,src={consumer_output},dst=/evidence",
                           "--mount", generation_mount + ",readonly",
                           "--mount", f"type=bind,src={self.binary},dst=/verifyprofile,readonly"]
        self.container(name + "-consume", image, consumer_mounts,
                       {"LOCALVERIFY_DISPOSABLE_CLUSTER": "true", "HOME": "/home/multica/agents", "TMPDIR": "/tmp"},
                       ["/verifyprofile", "--mode", "consume", "--input", "/profile/input.json", "--ref", "/profile/ready.json",
                        "--core-image", self.args.core_image, "--evidence", "/evidence/consumer.json"])
        profile = {"name": name, "input": input_value, "ref": json.loads(ready),
                   "toolsClaim": "verify-profile-" + name + "-" + identity[:8],
                   "expectedVersion": manifest["providers"]["codex"]["version"]}
        (folder / "profile.json").write_text(json.dumps(profile, indent=2) + "\n")
        return {"name": name, "image": image, "identity": identity, "tools": tools, "folder": folder,
                "profile": profile, "prepared": True}

    def pin_image(self, source, target, phase):
        rows = self.node_command(["ctr", "-n", "k8s.io", "images", "list"], phase + "-descriptors").decode().splitlines()
        descriptors = {columns[0]: columns[2] for line in rows
                       if len(columns := line.split()) >= 3 and columns[2].startswith("sha256:")}
        digest = target.split("@", 1)[1]
        if descriptors.get(source) != digest:
            raise RuntimeError("image archive changed the pinned OCI descriptor")
        if target in descriptors:
            if descriptors[target] != digest:
                raise RuntimeError("existing image name resolves to another OCI descriptor")
            return
        self.node_command(["ctr", "-n", "k8s.io", "images", "tag", source, target], phase)

    def transfer_and_execute(self, result):
        name, identity, image = result["name"], result["identity"], result["image"]
        print("profile", name + ": actual K3s worker", flush=True)
        tag = self.prefix + ":" + name
        existing = subprocess.run(["docker", "image", "inspect", tag], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        if existing.returncode == 0:
            current = json.loads(existing.stdout)[0]["Id"]
            if self.image_ids.get(tag) != current:
                raise RuntimeError("refusing to replace an unowned or changed image alias")
        self.run(["docker", "tag", image, tag], name + "-tag-image")
        self.images.add(tag)
        self.record_resources()
        archive = self.root / ("profile-" + name + "-image.tar")
        self.run(["docker", "save", "-o", str(archive), tag], name + "-save-image")
        self.run(["docker", "cp", str(archive), self.args.node + ":/profile-image-" + name + ".tar"], name + "-copy-image")
        self.node_command(["ctr", "-n", "k8s.io", "images", "import", "--platform", self.platform, "/profile-image-" + name + ".tar"], name + "-import-image")
        self.pin_image("docker.io/library/" + tag, image, name + "-pin-image")
        # CRI normalizes a tag+digest Pod reference to repository@digest when
        # looking up cached images. Keep the same OCI descriptor under that name.
        repository, digest = image.split("@", 1)
        last = repository.rsplit("/", 1)[-1]
        normalized = repository.rsplit(":", 1)[0] + "@" + digest if ":" in last else image
        if normalized != image:
            self.pin_image("docker.io/library/" + tag, normalized, name + "-pin-cri-image")
        self.node_command(["crictl", "images", "--output", "json"], name + "-cri-images")
        tar_file = self.root / ("profile-" + name + "-tools.tar")
        with tar_file.open("wb") as stream:
            result_code = subprocess.run(["docker", "run", "--rm", "--user", "65532:65532", "--mount",
                                          f"type=volume,src={result['tools']},dst=/tools-store,readonly", image,
                                          "/bin/tar", "-C", "/tools-store", "-cf", "-", "generations/" + identity], stdout=stream).returncode
        if result_code:
            raise RuntimeError("prepared generation export failed")
        target = "/verification-profile-tools-" + identity
        self.node_command(["mkdir", "-p", target], name + "-node-tools")
        self.run(["docker", "cp", str(tar_file), self.args.node + ":/profile-tools-" + name + ".tar"], name + "-copy-tools")
        self.node_command(["tar", "-C", target, "-xf", "/profile-tools-" + name + ".tar"], name + "-unpack-tools")
        self.node_command(["chown", "65532:65532", target], name + "-node-tools-owner")
        claim = result["profile"]["toolsClaim"]
        selection = json.loads((self.evidence / "selection.json").read_text())
        node_name = selection["worker"]["singleNodeName"]
        pv = {"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": claim},
              "spec": {"capacity": {"storage": "20Gi"}, "accessModes": ["ReadWriteOnce"], "storageClassName": "",
                       "persistentVolumeReclaimPolicy": "Retain", "hostPath": {"path": target, "type": "Directory"},
                       "claimRef": {"namespace": "runtime-verify", "name": claim},
                       "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchFields": [{"key": "metadata.name", "operator": "In", "values": [node_name]}]}]}}}}
        pvc = {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": claim, "namespace": "runtime-verify"},
               "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "", "volumeName": claim, "resources": {"requests": {"storage": "20Gi"}}}}
        self.kubectl(["apply", "-f", "-"], name + "-profile-storage", data=json.dumps({"apiVersion": "v1", "kind": "List", "items": [pv, pvc]}).encode())
        self.run(["docker", "cp", str(self.binary), self.args.node + ":/verifyprofile"], name + "-copy-helper")
        self.node_command(["env", "LOCALVERIFY_DISPOSABLE_CLUSTER=true", "/verifyprofile", "--mode", "worker",
                           "--kubeconfig", "/etc/rancher/k3s/k3s.yaml", "--selection", "/verification-evidence/selection.json",
                           "--profile", "/verification-evidence/profiles/" + name + "/profile.json",
                           "--evidence", "/verification-evidence/profiles/" + name + "/worker.json"], name + "-worker")
        return {"name": name, "environmentID": identity, "coreImage": self.args.core_image,
                "environmentImage": image, "prepared": True, "worker": True}

    def execute(self):
        base = self.example_input()
        image = base["environmentImage"]
        self.run(["docker", "pull", "--platform", self.platform, image], "pull-example-image")
        php_tag = self.prefix + "-php-python:local"
        dockerfile = self.chart / "files/environments/Dockerfile.php-python"
        source_hash = hashlib.sha256(dockerfile.read_bytes()).hexdigest()
        existing = subprocess.run(["docker", "image", "inspect", php_tag], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        if existing.returncode == 0:
            image_record = json.loads(existing.stdout)[0]
            labels = image_record.get("Config", {}).get("Labels", {}) or {}
            if labels.get("multica.io/verification-root") != str(self.root) or labels.get("multica.io/profile-source") != source_hash:
                raise RuntimeError("existing custom profile image does not match this fixture source")
        else:
            self.run(["docker", "build", "--platform", self.platform, "--provenance=false",
                      "--label", "multica.io/verification-root=" + str(self.root),
                      "--label", "multica.io/profile-source=" + source_hash,
                      "-f", str(dockerfile), "-t", php_tag, str(self.chart / "files/environments")], "build-php-python-image")
        self.images.add(php_tag)
        self.record_resources()
        php_image = self.image_reference(php_tag)
        prepared = [self.prepare("default", "node-providers.sh", base, image),
                    self.prepare("go-rust", "go-rust.sh", base, image),
                    self.prepare("php-python", "php-python.sh", base, php_image)]
        if self.args.prepare_only:
            result = {"phase": "prepared", "workerProof": False, "profiles": [p["profile"] for p in prepared]}
        else:
            result = {"phase": "verified", "workerProof": True, "profiles": [self.transfer_and_execute(p) for p in prepared]}
        (self.evidence / "profiles.json").write_text(json.dumps(result, indent=2) + "\n")
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("chart", "workdir", "core-image", "core-volume", "node", "arch"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    try:
        root = Path(args.workdir).resolve(strict=True)
        with (root / "profiles.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            result = Profiles(args).execute()
        print(json.dumps({"phase": result["phase"], "workerProof": result["workerProof"]}), flush=True)
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.CalledProcessError) as error:
        print("profiles:", error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

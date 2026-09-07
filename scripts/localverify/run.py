#!/usr/bin/env python3
"""Exercise current source and the external chart using disposable local services."""
from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
BASE = "docker.io/library/buildpack-deps:bookworm-scm@sha256:4274ea4975976f86239384ac206f98af5f0978fd8f054886eec1371fc7664025"
K3S = "rancher/k3s:v1.34.3-k3s1"
NAMESPACE = "runtime-verify"
OWNER_LABEL = "io.multica.localverify"


class Verification:
    def __init__(self, chart: Path, keep: bool, resume: Path | None = None):
        self.chart = chart.resolve()
        self.keep = keep
        self.root = resume.resolve() if resume else Path(tempfile.mkdtemp(prefix="multica-rewrite-verify-")).resolve()
        self.lease = (self.root / ".orchestration.lock").open("a+")
        fcntl.flock(self.lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.evidence = self.root / "evidence"
        self.evidence.mkdir(mode=0o777, exist_ok=resume is not None)
        self.evidence.chmod(0o777)
        self.context = self.root / "context"
        self.context.mkdir(exist_ok=resume is not None)
        self.name = self.root.name.lower()
        self.node = self.name + "-node"
        self.backend = self.name + "-backend"
        self.network = self.name + "-network"
        self.core_volume = self.name + "-core"
        self.containers: dict[str, str] = {}
        self.volumes: list[str] = []
        self.images: dict[str, str] = {}
        self.network_id = ""
        self.core = self.environment = ""
        self.arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
        if self.arch is None:
            raise RuntimeError("local fixture supports native amd64 or arm64")
        self.kubeconfig = self.root / "kubeconfig"
        self.passed: list[str] = []
        self.phase = "created"
        self.completed_steps: list[str] = []
        self.source = None
        if resume:
            self.restore()
        else:
            self.save_state()

    def save_state(self):
        state = {"root": str(self.root), "evidence": str(self.evidence), "node": self.node,
                 "backend": self.backend, "network": self.network, "network_id": self.network_id,
                 "core_volume": self.core_volume, "core_image": self.core,
                 "environment_image": self.environment, "arch": self.arch,
                 "containers": self.containers, "volumes": self.volumes, "images": self.images,
                 "phase": self.phase, "kubeconfig": str(self.kubeconfig),
                 "completed_steps": self.completed_steps, "passed": self.passed, "pid": os.getpid(),
                 "source": self.source}
        pending = self.root / "state.json.tmp"
        pending.write_text(json.dumps(state, indent=2), encoding="utf-8")
        os.replace(pending, self.root / "state.json")

    def stage(self, value):
        self.phase = value
        self.save_state()
        print(value, flush=True)

    def command(self, args, *, text=None, log=None, env=None, check=True, timeout=1800):
        if log:
            with (self.evidence / log).open("w") as output:
                result = subprocess.run(args, input=text, text=True, stdout=output,
                                        stderr=subprocess.STDOUT, env=env, cwd=ROOT, timeout=timeout)
            if check and result.returncode:
                tail = (self.evidence / log).read_text(errors="replace")[-12000:]
                raise RuntimeError(f"{args[0]} failed ({result.returncode}): {tail}")
            return ""
        result = subprocess.run(args, input=text, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, env=env, cwd=ROOT, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"{args[0]} failed ({result.returncode}): {result.stderr[-12000:]} {result.stdout[-12000:]}")
        return result.stdout.strip() if check else (result.stdout + result.stderr).strip()

    def docker(self, *args, **kwargs):
        return self.command(["docker", *args], **kwargs)

    def kube(self, *args, **kwargs):
        if self.node not in self.containers:
            raise RuntimeError("no owned disposable Kubernetes container")
        return self.docker("exec", "-i", self.containers[self.node], "kubectl", "--kubeconfig",
                           "/etc/rancher/k3s/k3s.yaml", *args, **kwargs)

    def apply(self, objects):
        self.kube("apply", "-f", "-", text=json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}))

    def wait(self, description, predicate, seconds=300):
        end = time.monotonic() + seconds
        next_notice = time.monotonic() + 20
        while time.monotonic() < end:
            try:
                if predicate():
                    return
            except (RuntimeError, KeyError):
                pass
            if time.monotonic() >= next_notice:
                print("Waiting for", description, flush=True)
                next_notice = time.monotonic() + 20
            time.sleep(2)
        raise RuntimeError(f"{description} did not complete; no acceptance result was recorded")

    def pass_check(self, value):
        self.passed.append(value)
        self.save_state()
        print("PASS", value, flush=True)

    @staticmethod
    def canonical(ref):
        repository = ref.split("@", 1)[0]
        return "docker.io/library/" + ref if "/" not in repository else ref

    def inspect_image(self, tag):
        info = json.loads(self.docker("image", "inspect", tag))[0]
        refs = info.get("RepoDigests", [])
        if not refs:
            raise RuntimeError("Docker did not retain a RepoDigest for the locally built image: " + tag)
        self.images[tag] = info["Id"]
        self.save_state()
        return self.canonical(refs[0])

    def create_container(self, name, arguments):
        identifier = self.docker("create", "--name", name, "--label", OWNER_LABEL + "=" + self.name, *arguments)
        self.containers[name] = identifier
        self.save_state()
        return identifier

    def source_snapshot(self):
        files = {}
        paths = []
        for directory in (ROOT / "src/cmd", ROOT / "src/internal"):
            paths.extend(("runtime/" + str(path.relative_to(ROOT)), path) for path in directory.rglob("*.go"))
        for directory in (ROOT / "scripts", ROOT / ".github/workflows"):
            paths.extend(("runtime/" + str(path.relative_to(ROOT)), path) for path in directory.rglob("*")
                         if path.suffix in {".py", ".sh", ".yaml", ".yml"})
        for name in ("src/go.mod", "src/go.sum", "Dockerfile", ".dockerignore", "Makefile",
                     "VERSION", "build/runtime-versions.env", "scripts/localverify/Dockerfile"):
            paths.append(("runtime/" + name, ROOT / name))
        for path in self.chart.rglob("*"):
            if path.suffix in {".yaml", ".yml", ".json", ".tpl", ".sh", ".env"} or "Dockerfile" in path.name:
                paths.append(("chart/" + str(path.relative_to(self.chart)), path))
        helm_root = self.chart.parents[1]
        for name in ("scripts/update_chart.py", "scripts/test_update_chart.py", ".github/workflows/ci.yml"):
            paths.append(("helm/" + name, helm_root / name))
        for label, path in sorted(paths):
            if path.is_file():
                files[label] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                "mode": path.stat().st_mode & 0o777}
        encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        return {"sha256": hashlib.sha256(encoded).hexdigest(), "files": files}

    def verify_source_freshness(self):
        current = self.source_snapshot()
        (self.evidence / "source-final.json").write_text(json.dumps(current, indent=2))
        if self.source is None:
            raise RuntimeError("this retained diagnostic run predates source provenance; a current-source full run is required")
        before, after = self.source["files"], current["files"]
        changed = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
        if changed:
            (self.evidence / "changed-source.json").write_text(json.dumps(changed, indent=2))
            raise RuntimeError("verification source changed after artifact build: " + ", ".join(changed[:12]))

    def build(self):
        self.stage("Building the current core and verification executables")
        for name in ("docker", "go", "helm", "make"):
            if not shutil.which(name):
                raise RuntimeError(f"{name} is required; local verification was not run")
        if not (self.chart / "Chart.yaml").is_file():
            raise RuntimeError("the external Helm chart source is required for complete verification")
        self.docker("info")
        self.source = self.source_snapshot()
        (self.evidence / "source-build.json").write_text(json.dumps(self.source, indent=2))
        self.save_state()
        core_tag = "multica-core-verify:" + self.name
        self.command(["make", "image", f"IMAGE={core_tag}", "PLATFORM=linux/" + self.arch], log="core-build.log")
        self.core = self.inspect_image(core_tag)
        env = dict(os.environ, GOOS="linux", GOARCH=self.arch, CGO_ENABLED="0")
        for name in ("verifyofficial", "verifyruntime", "verifykube", "verifystream"):
            self.command(["go", "-C", str(ROOT / "src"), "build", "-o", str(self.context / name),
                          f"./cmd/{name}"], env=env)
        shutil.copy(ROOT / "scripts/localverify/Dockerfile", self.context / "Dockerfile")
        environment_tag = "multica-environment-verify:" + self.name
        self.docker("build", "--build-arg", "ENVIRONMENT_IMAGE=" + BASE, "-t", environment_tag,
                    str(self.context), log="environment-build.log")
        self.environment = self.inspect_image(environment_tag)
        self.docker("save", "-o", str(self.root / "images.tar"), core_tag, environment_tag)
        self.verify_source_freshness()
        self.save_state()
        (self.evidence / "artifacts.json").write_text(json.dumps({"core": self.core,
            "environment": self.environment, "platform": "linux/" + self.arch}, indent=2))
        return core_tag, environment_tag

    def official(self, core_tag, environment_tag):
        self.stage("Verifying materialized core and the actual official release in Docker")
        self.docker("volume", "create", "--label", OWNER_LABEL + "=" + self.name, self.core_volume)
        self.volumes.append(self.core_volume)
        self.save_state()
        self.docker("run", "--rm", "--user", "0:0", "--entrypoint", "/bin/chown", "-v",
                    self.core_volume + ":/core", environment_tag, "65532:65532", "/core")
        self.docker("run", "--rm", "-v", self.core_volume + ":/opt/multica/core", core_tag, "materialize")
        name = self.name + "-official"
        identifier = self.create_container(name, ["--read-only", "--user", "65532:65532", "-e",
            "LOCALVERIFY_DISPOSABLE_CONTAINER=true", "--tmpfs", "/tmp:exec,mode=1777", "--tmpfs",
            "/workspace:mode=0777", "--tmpfs", "/run/multica:mode=0777", "--tmpfs",
            "/home/multica/agents:mode=0777", "--tmpfs", "/opt/multica/environment:exec,mode=0777",
            "-v", self.core_volume + ":/opt/multica/core:ro", "-v", str(self.evidence) + ":/evidence",
            "--entrypoint", "/usr/local/bin/verifyofficial", environment_tag, "--evidence", "/evidence/official"])
        self.docker("start", "-a", identifier, log="official.log", timeout=600)
        info = json.loads(self.docker("inspect", identifier))[0]
        if info["State"]["ExitCode"] != 0:
            raise RuntimeError("actual official fixture failed; inspect official.log")
        self.pass_check("actual official binary: discovery, registration, model lookup and HTTP/WS claim boundaries")

    def cluster(self, core_tag, environment_tag):
        self.stage("Starting disposable K3s and the local backend")
        self.network_id = self.docker("network", "create", "--label", OWNER_LABEL + "=" + self.name, self.network)
        self.save_state()
        backend = self.create_container(self.backend, ["--network", self.network, "-e",
            "LOCALVERIFY_DISPOSABLE_CLUSTER=true", "-v", str(self.evidence) + ":/evidence",
            "--entrypoint", "/bin/sleep", environment_tag, "infinity"])
        self.docker("start", backend)
        backend_ip = json.loads(self.docker("inspect", backend))[0]["NetworkSettings"]["Networks"][self.network]["IPAddress"]
        self.backend_origin = f"http://{backend_ip}:18080"
        self.docker("exec", "-d", backend, "/usr/local/bin/verifyruntime", "backend", "--origin",
                    self.backend_origin, "--evidence", "/evidence")
        self.wait("local backend", lambda: bool(self.docker("exec", backend, "curl", "-fsS",
                  "http://127.0.0.1:18080/fixture/health")), 30)
        node = self.create_container(self.node, ["--privileged", "--network", self.network,
            "--tmpfs", "/run", "--tmpfs", "/var/run", "-p", "127.0.0.1::6443", "-v",
            str(self.evidence) + ":/verification-evidence", K3S, "server", "--node-name", "verification-node",
            "--disable", "traefik", "--disable", "servicelb", "--disable", "metrics-server"])
        self.docker("start", node)
        print("Live K3s container:", self.node, node, flush=True)
        self.wait("K3s API", lambda: self.kube("get", "--raw", "/readyz") == "ok")
        self.wait("K3s node registration", lambda: bool(self.kube("get", "node", "verification-node", "-o", "name")), 120)
        self.kube("wait", "--for=condition=Ready", "node/verification-node", "--timeout=120s")
        port = self.docker("port", node, "6443/tcp").split(":")[-1]
        config = self.docker("exec", node, "cat", "/etc/rancher/k3s/k3s.yaml")
        self.kubeconfig.write_text(config.replace("127.0.0.1:6443", "127.0.0.1:" + port))
        self.kubeconfig.chmod(0o600)
        self.docker("cp", str(self.root / "images.tar"), node + ":/images.tar")
        self.docker("exec", node, "ctr", "-n", "k8s.io", "images", "import", "--platform",
                    "linux/" + self.arch, "/images.tar", log="image-import.log")
        for tag, ref in ((core_tag, self.core), (environment_tag, self.environment)):
            self.docker("exec", node, "ctr", "-n", "k8s.io", "images", "tag", "docker.io/library/" + tag, ref)
        for helper in ("verifykube", "verifystream"):
            self.docker("cp", str(self.context / helper), node + ":/" + helper)
        self.docker("exec", node, "sh", "-c",
                    "mkdir -p /verification-workspace /verification-tools && chown 65532:65532 /verification-workspace /verification-tools")
        self.initialize_storage()
        self.make_values()
        self.runtime()

    def step(self, name, action, proof=None):
        if name in self.completed_steps:
            return
        action()
        if proof:
            self.pass_check(proof)
        self.completed_steps.append(name)
        self.save_state()

    def runtime(self):
        self.step("identity", self.identity_checks,
                  "Go and Helm agree on bundled, inline Unicode/newline and external script identities")
        self.step("install-a", lambda: self.install("A"))
        self.step("baseline", lambda: self.drive("baseline"),
                  "installed chart: actual task Pods, isolated Git checkout, continuation/retry and scope isolation")
        self.step("journal-failure", self.journal_failure,
                  "journal write failure prevents Pod/Secret creation and preserves existing worker files")
        self.step("cleanup-failure", self.cleanup_failure,
                  "provider success survives an actual denied Kubernetes cleanup")
        self.step("cleanup-recovered", self.cleanup_recovered,
                  "controller process recovery preserves files, branch and native session after denied cleanup")
        self.step("interrupted-start", lambda: self.drive("interrupted-start"))
        self.step("interrupted-recovered", self.interrupted_recovered,
                  "abrupt controller process death preserves committed worker edits for same-task retry")
        self.step("hold", lambda: self.drive("hold"))
        self.step("prepare-b", self.prepare_b)
        self.step("release", lambda: self.drive("release"),
                  "active environment A task remains pinned during independent environment B preparation")
        self.step("install-b", lambda: self.install("B"))
        self.step("changed", lambda: self.drive("changed"),
                  "controller replacement/environment change preserves files and branch with a new Pi session")
        self.step("access", self.access_checks,
                  "actual shim rejects unobserved, credential/scope-mismatched and local-directory execution before resources")
        self.step("kubernetes", self.kubernetes_checks,
                  "actual API create-loss, UID replacement, cleanup recovery and fixed-Node scheduling")
        self.step("profiles", self.profile_checks,
                  "actual bundled, prefix-installed and custom-image profiles with immutable core and worker execution")
        self.step("transport-sever", self.transport_checks,
                  "actual remote exec transport loss is rejected without deleting persisted worker files")

    def identity_checks(self):
        self.stage("Comparing current Go and Helm environment identity implementations")
        self.command(["python3", str(ROOT / "scripts/localverify/identity.py"), "--chart", str(self.chart),
                      "--workdir", str(self.root)], log="identity.log")

    def export_selection(self):
        pod = self.controller_pod()
        selection = self.kube("-n", NAMESPACE, "exec", pod, "-c", "controller", "--", "cat", "/run/multica/selection.json")
        (self.evidence / "selection.json").write_text(selection)

    def transport_checks(self):
        self.stage("Severing an actual active Kubernetes exec transport")
        self.export_selection()
        self.docker("exec", "-e", "LOCALVERIFY_DISPOSABLE_CLUSTER=true", self.containers[self.node], "/verifystream",
            "--kubeconfig", "/etc/rancher/k3s/k3s.yaml", "--namespace", NAMESPACE,
            "--selection", "/verification-evidence/selection.json",
            "--request", "/verification-evidence/request.json", "--evidence", "/verification-evidence/transport.json",
            log="transport.log")

    def access_checks(self):
        self.stage("Verifying actual controller shim authorization and file preservation")
        pod = self.controller_pod()
        payload = (self.evidence / "request.json").read_text()
        self.kube("-n", NAMESPACE, "exec", "-i", pod, "-c", "controller", "--", "/bin/sh", "-c",
                  "cat > /tmp/fixture-access-request.json", text=payload)
        raw = self.kube("-n", NAMESPACE, "exec", pod, "-c", "controller", "--", "env",
            "LOCALVERIFY_DISPOSABLE_CLUSTER=true", "/usr/local/bin/verifyruntime", "access",
            "--request-file=/tmp/fixture-access-request.json")
        report = json.loads(raw)
        if report.get("passed") is not True:
            raise RuntimeError("actual shim access proof did not pass")
        report["resourcesAbsent"] = False
        (self.evidence / "access.json").write_text(json.dumps(report, indent=2))
        identities = {report["positiveControl"], *(entry["taskID"] for entry in report["denied"])}
        def resources_gone():
            for task in identities:
                for kind in ("pods", "secrets"):
                    items = json.loads(self.kube("-n", NAMESPACE, "get", kind, "-l",
                                               "multica.ai/task-id=" + task, "-o", "json"))["items"]
                    if items:
                        return False
            return True
        self.wait("absence of access-probe Pod/Secret resources", resources_gone, 30)
        report["resourcesAbsent"] = True
        (self.evidence / "access.json").write_text(json.dumps(report, indent=2))

    def kubernetes_checks(self):
        self.export_selection()
        self.docker("exec", "-e", "LOCALVERIFY_DISPOSABLE_CLUSTER=true", self.containers[self.node], "/verifykube",
            "--kubeconfig", "/etc/rancher/k3s/k3s.yaml", "--namespace", NAMESPACE,
            "--selection", "/verification-evidence/selection.json", "--request", "/verification-evidence/request.json",
            "--evidence", "/verification-evidence/kubernetes.json", log="kubernetes.log")
        # Kubernetes fault checks may replace the controller. Export the live
        # controller's authority again before the profile worker proofs.
        self.export_selection()

    def profile_checks(self):
        self.stage("Verifying bundled, prefix-installed and custom-image environment profiles")
        self.command(["python3", str(ROOT / "scripts/localverify/profiles.py"), "--chart", str(self.chart),
            "--workdir", str(self.root), "--core-image", self.core, "--core-volume", self.core_volume,
            "--node", self.node, "--arch", self.arch], env=dict(os.environ, LOCALVERIFY_DISPOSABLE_CLUSTER="true"),
            log="profiles.log", timeout=3600)

    def restore(self):
        state = json.loads((self.root / "state.json").read_text())
        if state["root"] != str(self.root) or state["evidence"] != str(self.evidence) or state["node"] != self.node:
            raise RuntimeError("resume state does not identify this owned work directory")
        self.containers = state["containers"]
        self.volumes = state["volumes"]
        self.images = state["images"]
        self.network_id = state["network_id"]
        self.core = state["core_image"]
        self.environment = state["environment_image"]
        self.phase = state["phase"]
        self.completed_steps = state.get("completed_steps", [])
        self.source = state.get("source")
        if "completed_steps" not in state and self.phase != "Installing actual Helm chart environment A":
            raise RuntimeError("old fixture has no trustworthy runtime checkpoint; inspect evidence before resuming")
        self.passed = state.get("passed", [])
        summary = self.evidence / "summary.json"
        if summary.exists():
            self.passed = json.loads(summary.read_text()).get("passed", self.passed)
        for name in (self.node, self.backend):
            info = json.loads(self.docker("inspect", self.containers[name]))[0]
            if info["Name"] != "/" + name or info["Config"].get("Labels", {}).get(OWNER_LABEL) != self.name or not info["State"]["Running"]:
                raise RuntimeError("resume target is not the running owned disposable container")
            if name == self.node and not any(m.get("Source") == str(self.evidence) and m.get("Destination") == "/verification-evidence" for m in info["Mounts"]):
                raise RuntimeError("resume K3s evidence mount is different")
        self.values = json.loads((self.root / "values-A.json").read_text())
        self.backend_origin = self.values["multica"]["baseURL"]
        self.save_state()

    def backend_state(self):
        return json.loads(self.docker("exec", self.containers[self.backend], "curl", "-fsS",
                                     "http://127.0.0.1:18080/fixture/state"))

    def snapshot_worker_files(self, label):
        destination = Path(tempfile.mkdtemp(prefix=label + "-", dir=self.root))
        self.docker("cp", self.containers[self.node] + ":/verification-workspace/.multica-runtime/workers/.", str(destination))
        snapshot = {}
        for path in destination.rglob("*"):
            relative = str(path.relative_to(destination))
            if path.is_symlink():
                snapshot[relative] = {"link": os.readlink(path)}
            elif path.is_file():
                snapshot[relative] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                      "mode": path.stat().st_mode & 0o777}
        (self.evidence / (label + ".json")).write_text(json.dumps(snapshot, indent=2))
        return snapshot

    def journal_failure(self):
        self.stage("Preventing resource creation when the durable journal is unwritable")
        before = self.snapshot_worker_files("journal-before-files")
        if not before:
            raise RuntimeError("journal-failure proof requires existing worker files from the baseline")
        controller = self.controller_pod()
        self.kube("-n", NAMESPACE, "exec", controller, "-c", "controller", "--", "chmod", "0500", "/workspace/.multica-runtime/attempts")
        try:
            self.drive("journal-failure")
        finally:
            self.kube("-n", NAMESPACE, "exec", controller, "-c", "controller", "--", "chmod", "0700", "/workspace/.multica-runtime/attempts")
        task = self.backend_state()["journalTask"]
        for kind in ("pods", "secrets"):
            objects = json.loads(self.kube("-n", NAMESPACE, "get", kind, "-l", "multica.ai/task-id=" + task, "-o", "json"))["items"]
            if objects:
                raise RuntimeError("journal write failure permitted actual " + kind + " creation")
        after = self.snapshot_worker_files("journal-after-files")
        if any(after.get(path) != value for path, value in before.items()):
            raise RuntimeError("journal failure changed pre-existing worker files")
        (self.evidence / "journal-failure.json").write_text(json.dumps({"task": task,
            "resourceCreationPrevented": True, "workerFilesPreserved": True}, indent=2))

    def controller_role(self):
        pod = json.loads(self.kube("-n", NAMESPACE, "get", "pod", self.controller_pod(), "-o", "json"))
        account = pod["spec"]["serviceAccountName"]
        bindings = json.loads(self.kube("-n", NAMESPACE, "get", "rolebindings", "-o", "json"))["items"]
        refs = [b["roleRef"]["name"] for b in bindings if b["roleRef"]["kind"] == "Role" and
                any(s.get("kind") == "ServiceAccount" and s.get("name") == account for s in b.get("subjects", []))]
        if len(refs) != 1:
            raise RuntimeError("fixture must identify exactly one controller Role")
        return json.loads(self.kube("-n", NAMESPACE, "get", "role", refs[0], "-o", "json"))

    def restore_cleanup_role(self):
        role = json.loads((self.root / "cleanup-role.json").read_text())
        current = json.loads(self.kube("-n", NAMESPACE, "get", "role", role["metadata"]["name"], "-o", "json"))
        if current["metadata"]["uid"] != role["metadata"]["uid"]:
            raise RuntimeError("controller Role was replaced; refusing a recovery permission mutation")
        self.kube("-n", NAMESPACE, "patch", "role", role["metadata"]["name"], "--type=merge",
                  "-p", json.dumps({"rules": role["rules"]}))

    def cleanup_failure(self):
        self.stage("Exercising actual denied cleanup without changing provider success")
        role = self.controller_role()
        (self.root / "cleanup-role.json").write_text(json.dumps(role))
        rules = copy.deepcopy(role["rules"])
        for rule in rules:
            if "" in rule.get("apiGroups", []) and set(rule.get("resources", [])) & {"pods", "secrets"}:
                rule["verbs"] = [verb for verb in rule["verbs"] if verb != "delete"]
        self.kube("-n", NAMESPACE, "patch", "role", role["metadata"]["name"], "--type=merge",
                  "-p", json.dumps({"rules": rules}))
        try:
            self.drive("cleanup-failure")
            state = self.backend_state()
            task = state["cleanupTask"]
            pods = json.loads(self.kube("-n", NAMESPACE, "get", "pods", "-l", "multica.ai/task-id=" + task, "-o", "json"))["items"]
            secrets = json.loads(self.kube("-n", NAMESPACE, "get", "secrets", "-l", "multica.ai/task-id=" + task, "-o", "json"))["items"]
            if not pods or not secrets:
                raise RuntimeError("denied cleanup did not retain its actual Pod and request Secret")
            (self.evidence / "cleanup-denied.json").write_text(json.dumps({"task": task,
                "pods": [p["metadata"] for p in pods], "secrets": [v["metadata"] for v in secrets]}, indent=2))
            self.docker("exec", self.containers[self.node], "tar", "-czf", "/verification-evidence/cleanup-attempts.tar.gz",
                        "-C", "/verification-workspace/.multica-runtime", "attempts")
        except BaseException:
            self.restore_cleanup_role()
            raise
        # Keep the denied permission until the old controller is stopped. The
        # next phase restores permission before a fresh process can recover.

    def restart_controller_process(self, before_kill=None):
        name = self.controller_pod()
        pod = json.loads(self.kube("-n", NAMESPACE, "get", "pod", name, "-o", "json"))
        status = next(s for s in pod["status"]["containerStatuses"] if s["name"] == "controller")
        identifier = status["containerID"].removeprefix("containerd://")
        inspected = json.loads(self.docker("exec", self.containers[self.node], "crictl", "inspect", identifier))
        pid = inspected.get("info", {}).get("pid")
        if not isinstance(pid, int) or pid <= 1:
            raise RuntimeError("container runtime did not identify the owned controller process")
        labels = inspected["status"].get("labels", {})
        if labels.get("io.kubernetes.pod.uid") != pod["metadata"]["uid"]:
            raise RuntimeError("container process does not belong to the observed controller Pod")
        if before_kill:
            self.docker("exec", self.containers[self.node], "kill", "-STOP", str(pid))
        try:
            if before_kill:
                before_kill()
            # The node is an ancestor PID namespace; SIGKILL cannot be ignored
            # by the controller's namespace PID 1 as an in-container kill can.
            self.docker("exec", self.containers[self.node], "kill", "-KILL", str(pid))
        except BaseException:
            if before_kill:
                self.docker("exec", self.containers[self.node], "kill", "-CONT", str(pid), check=False)
            raise
        def restarted():
            current = json.loads(self.kube("-n", NAMESPACE, "get", "pod", name, "-o", "json"))
            if current["metadata"]["uid"] != pod["metadata"]["uid"]:
                raise ValueError("process interruption replaced the controller Pod; recovery proof is invalid")
            new = next(s for s in current.get("status", {}).get("containerStatuses", []) if s["name"] == "controller")
            return new.get("restartCount", 0) > status["restartCount"] and new.get("ready", False)
        self.wait("same-Pod controller process restart", restarted, 180)

    def cleanup_recovered(self):
        self.stage("Recovering denied cleanup in a new controller process")
        self.restart_controller_process(self.restore_cleanup_role)
        self.drive("recovered")

    def interrupted_recovered(self):
        self.stage("Abruptly interrupting controller after real worker edits")
        state = self.backend_state()
        checkpoint = state["tasks"][state["interruptedTask"]].get("checkpoint")
        if not checkpoint or checkpoint.get("stage") != "held":
            raise RuntimeError("worker did not commit its held filesystem checkpoint before interruption")
        (self.evidence / "interrupted-checkpoint.json").write_text(json.dumps(checkpoint, indent=2))
        self.restart_controller_process()
        self.drive("interrupted-resume")

    def initialize_storage(self):
        items = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}}]
        for name in ("workspace", "tools"):
            items += [{"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": "verification-" + name},
                "spec": {"capacity": {"storage": "8Gi"}, "accessModes": ["ReadWriteOnce"],
                         "persistentVolumeReclaimPolicy": "Retain", "storageClassName": "",
                         "hostPath": {"path": "/verification-" + name, "type": "Directory"}}},
                {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": name, "namespace": NAMESPACE},
                 "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "8Gi"}},
                          "storageClassName": "", "volumeName": "verification-" + name}}]
        items.append({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "fixture-token", "namespace": NAMESPACE},
                      "stringData": {"token": "mul_disposable_chart_fixture"}})
        self.apply(items)

    def make_values(self):
        script = '''#!/bin/bash
set -euo pipefail
mkdir -p "$ENV_ROOT/providers/pi" "$ENV_ROOT/bin"
cat > "$ENV_ROOT/providers/pi/run" <<'PROVIDER'
#!/bin/bash
exec /usr/local/bin/verifyruntime provider "$@"
PROVIDER
chmod 0555 "$ENV_ROOT/providers/pi/run"
cat > "$ENV_MANIFEST_FILE" <<'MANIFEST'
{"schemaVersion":1,"providers":{"pi":{"entrypoint":"providers/pi/run","version":"0.85.0"}},"binDirs":["bin"],"env":{"FIXTURE_CACHE":"${WORKSPACE}/.cache"}}
MANIFEST
'''
        small = {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}
        self.values = {"runtime": {"image": {"reference": self.core, "pullPolicy": "Never"},
            "pollInterval": "1s", "heartbeatInterval": "1s"}, "environment": {
            "image": {"reference": self.environment, "pullPolicy": "Never"}, "platform": "linux/" + self.arch,
            "providers": ["pi"], "bootstrap": {"source": "inline", "script": script, "revision": "A", "resources": copy.deepcopy(small)},
            "storage": {"existingClaim": "tools", "size": "", "accessMode": "ReadWriteOnce"}},
            "workspace": {"storage": {"existingClaim": "workspace", "size": "", "accessMode": "ReadWriteOnce"}},
            "scheduling": {"singleNodeName": "verification-node"},
            "multica": {"baseURL": self.backend_origin, "controllerTokenSecret": {"name": "fixture-token", "key": "token"}},
            "controller": {"resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"cpu": "2", "memory": "1Gi"}}},
            "worker": {"taskDeadline": 300, "resources": copy.deepcopy(small)}}

    def install(self, revision):
        self.stage("Installing actual Helm chart environment " + revision)
        self.values["environment"]["bootstrap"]["revision"] = revision
        path = self.root / ("values-" + revision + ".json")
        path.write_text(json.dumps(self.values))
        self.command(["helm", "upgrade", "--install", "verify", str(self.chart), "--kubeconfig", str(self.kubeconfig),
            "--namespace", NAMESPACE, "--values", str(path), "--wait", "--timeout", "5m"], log="helm-" + revision + ".log")

    def drive(self, phase):
        self.stage("Exercising actual runtime phase " + phase)
        self.docker("exec", self.containers[self.backend], "/usr/local/bin/verifyruntime", "drive",
                    "--backend", "http://127.0.0.1:18080", "--phase", phase, log="runtime-" + phase + ".log")

    def controller_pod(self):
        selected = []
        def ready_controller():
            pods = json.loads(self.kube("-n", NAMESPACE, "get", "pods", "-l",
                "app.kubernetes.io/instance=verify,app.kubernetes.io/component=controller", "-o", "json"))["items"]
            ready = [p for p in pods if not p["metadata"].get("deletionTimestamp") and
                     any(c.get("type") == "Ready" and c.get("status") == "True" for c in p.get("status", {}).get("conditions", []))]
            if len(ready) > 1:
                raise ValueError("multiple ready controllers cannot identify one execution authority")
            if ready:
                selected.append(ready[0]["metadata"]["name"])
                return True
            return False
        self.wait("the current controller readiness and authority", ready_controller, 180)
        return selected[0]

    def prepare_b(self):
        self.stage("Preparing environment B while environment A task remains active")
        future = copy.deepcopy(self.values)
        future["environment"]["bootstrap"]["revision"] = "B"
        path = self.root / "prepare-b.json"
        path.write_text(json.dumps(future))
        rendered = self.command(["helm", "template", "verify", str(self.chart), "--namespace", NAMESPACE,
                                 "--values", str(path)])
        native = self.kube("-n", NAMESPACE, "create", "--dry-run=client", "-f", "-", "-o", "json", text=rendered)
        # kubectl prints one JSON object per YAML document rather than always
        # wrapping the documents in a List. Its native decoder owns YAML parsing.
        items = []
        decoder = json.JSONDecoder()
        remaining = native.lstrip()
        while remaining:
            document, end = decoder.raw_decode(remaining)
            items.extend(document.get("items", [document]))
            remaining = remaining[end:].lstrip()
        deployment = next(x for x in items if x["kind"] == "Deployment")
        config = copy.deepcopy(next(x for x in items if x["kind"] == "ConfigMap" and "input.json" in x.get("data", {})))
        old_name = config["metadata"]["name"]
        config["metadata"]["name"] = "prepare-environment-b"
        config["metadata"]["namespace"] = NAMESPACE
        spec = copy.deepcopy(deployment["spec"]["template"]["spec"])
        for volume in spec["volumes"]:
            if volume.get("configMap", {}).get("name") == old_name:
                volume["configMap"]["name"] = config["metadata"]["name"]
        installer = next(x for x in spec["initContainers"] if x["name"] == "environment-prepare")
        consumer = {"name": "checked", "image": installer["image"], "imagePullPolicy": "Never",
            "command": ["/opt/multica/core/runtime", "environment", "check"], "env": copy.deepcopy(installer["env"]),
            "securityContext": copy.deepcopy(installer["securityContext"]), "volumeMounts": copy.deepcopy(installer["volumeMounts"])}
        for mount in consumer["volumeMounts"]:
            if mount["name"] == "tools":
                mount["readOnly"] = True
        spec["containers"] = [consumer]
        spec["restartPolicy"] = "Never"
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "prepare-b", "namespace": NAMESPACE}, "spec": spec}
        self.apply([config, pod])
        def succeeded():
            phase = json.loads(self.kube("-n", NAMESPACE, "get", "pod", "prepare-b", "-o", "json"))["status"].get("phase")
            if phase == "Failed":
                raise ValueError("independent environment B preparation failed")
            return phase == "Succeeded"
        self.wait("independent environment B preparation", succeeded)
        self.kube("-n", NAMESPACE, "delete", "pod", "prepare-b", "--wait=true")

    def cleanup_profiles(self):
        path = self.root / "profile-resources.json"
        if not path.exists():
            return
        resources = json.loads(path.read_text())
        if resources.get("root") != str(self.root) or resources.get("node") != self.node:
            raise RuntimeError("profile resource record belongs to another disposable fixture")
        for name in resources.get("volumes", []):
            raw = self.docker("volume", "inspect", name, check=False)
            try:
                info = json.loads(raw)[0]
            except (ValueError, IndexError):
                continue
            if info.get("Labels", {}).get("multica.io/verification-root") == str(self.root):
                self.docker("volume", "rm", name, check=False, timeout=30)
        for name, identifier in resources.get("images", {}).items():
            raw = self.docker("image", "inspect", name, check=False)
            try:
                info = json.loads(raw)[0]
            except (ValueError, IndexError):
                continue
            if info["Id"] == identifier and name.startswith("multica-profiles-"):
                self.docker("image", "rm", name, check=False, timeout=30)

    def finish(self, success, failure):
        for name, identifier in self.containers.items():
            (self.evidence / (name + ".log")).write_text(self.docker("logs", identifier, check=False, timeout=30))
        if self.node in self.containers:
            for resource in ("pods", "events"):
                (self.evidence / (resource + ".json")).write_text(self.kube("-n", NAMESPACE, "get", resource, "-o", "json", check=False, timeout=30))
            raw = self.kube("-n", NAMESPACE, "get", "pods", "-o", "json", check=False, timeout=30)
            try:
                for pod in json.loads(raw).get("items", []):
                    for container in pod["spec"].get("initContainers", []) + pod["spec"].get("containers", []):
                        name = pod["metadata"]["name"]
                        (self.evidence / f'{name}-{container["name"]}.log').write_text(self.kube("-n", NAMESPACE,
                            "logs", name, "-c", container["name"], check=False, timeout=30))
            except ValueError:
                pass
        (self.evidence / "summary.json").write_text(json.dumps({"passed": self.passed, "complete": success,
            "phase": self.phase, "error": failure, "sourceSHA256": self.source["sha256"] if self.source else None}, indent=2))
        if success or not self.keep:
            self.cleanup_profiles()
            for identifier in reversed(list(self.containers.values())):
                self.docker("rm", "-f", "-v", identifier, check=False, timeout=30)
            if self.network_id:
                self.docker("network", "rm", self.network_id, check=False, timeout=30)
            for name in self.volumes:
                info = json.loads(self.docker("volume", "inspect", name))[0]
                if info.get("Labels", {}).get(OWNER_LABEL) == self.name:
                    self.docker("volume", "rm", name, check=False, timeout=30)
            for name, identifier in self.images.items():
                info = json.loads(self.docker("image", "inspect", name))[0]
                if info["Id"] == identifier:
                    self.docker("image", "rm", name, check=False, timeout=30)
        print("Local verification evidence:", self.evidence, flush=True)
        if not success and self.keep:
            print("Retained owned handles:", self.root / "state.json", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chart", type=Path, default=ROOT.parent / "helm/charts/multica-runtime-controller")
    parser.add_argument("--keep-on-failure", action="store_true")
    parser.add_argument("--resume", type=Path, help="resume the exact retained disposable work directory after its process has stopped")
    args = parser.parse_args()
    verification = Verification(args.chart, args.keep_on_failure, args.resume)
    print("Local verification work directory:", verification.root, flush=True)
    success = False
    failure = ""
    try:
        if args.resume:
            verification.runtime()
        else:
            images = verification.build()
            verification.official(*images)
            verification.cluster(*images)
        verification.verify_source_freshness()
        success = True
    except BaseException as exc:
        failure = str(exc)
        raise
    finally:
        verification.finish(success, failure)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Compare actual Helm rendering with the runtime's canonical identity command."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def command(args, *, data=None, env=None):
    result = subprocess.run(args, input=data, text=True, capture_output=True, check=True, env=env, cwd=ROOT)
    return result.stdout


def documents(text):
    decoder = json.JSONDecoder()
    remaining = text.lstrip()
    result = []
    while remaining:
        value, end = decoder.raw_decode(remaining)
        result.extend(value.get("items", [value]) if value.get("kind") == "List" else [value])
        remaining = remaining[end:].lstrip()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chart", required=True, type=Path)
    parser.add_argument("--workdir", required=True, type=Path)
    args = parser.parse_args()
    work = args.workdir.resolve()
    state = json.loads((work / "state.json").read_text())
    node = state["node"]
    expected_id = state["containers"][node]
    actual = json.loads(command(["docker", "inspect", node]))[0]
    if (actual["Id"] != expected_id or not actual["State"]["Running"]
            or actual["Config"]["Labels"].get("io.multica.localverify") != work.name):
        raise SystemExit("identity proof requires the exact running disposable fixture")
    evidence = work / "evidence" / "identity"
    evidence.mkdir(exist_ok=True)
    binary = evidence / "runtime"
    command(["go", "-C", str(ROOT / "src"), "build", "-o", str(binary), "./cmd/runtime"])
    shared = json.loads((args.chart / "files/environments/identity-cases.json").read_text())
    external_script = "#!/bin/bash\nprintf 'external identity fixture\\n'\n"
    cases = [{"name": "bundled", "values": {}}] + shared + [{
        "name": "external-configmap",
        "values": {"environment": {"bootstrap": {"source": "configMap", "configMap": {
            "name": "external-identity-fixture", "key": "install.sh",
            "sha256": hashlib.sha256(external_script.encode()).hexdigest()}}}}}]
    outcomes = []
    for case in cases:
        values = case["values"]
        values["runtime"] = {"image": {"reference": state["core_image"]}}
        values.setdefault("environment", {})["platform"] = "linux/" + state["arch"]
        values_file = evidence / (case["name"] + "-values.json")
        values_file.write_text(json.dumps(values, ensure_ascii=False))
        rendered = command(["helm", "template", "identity", str(args.chart), "--namespace", "runtime-verify", "-f", str(values_file)])
        native = command(["docker", "exec", "-i", expected_id, "kubectl", "--kubeconfig",
                          "/etc/rancher/k3s/k3s.yaml", "create", "--dry-run=client", "-f", "-", "-o", "json"], data=rendered)
        objects = documents(native)
        config = next(item for item in objects if item["kind"] == "ConfigMap" and "input.json" in item.get("data", {}))
        deployment = next(item for item in objects if item["kind"] == "Deployment")
        payload = config["data"]["input.json"]
        input_file = evidence / (case["name"] + "-input.json")
        input_file.write_text(payload)
        input_data = json.loads(payload)
        helm_id = deployment["spec"]["template"]["metadata"]["annotations"]["multica.io/environment-id"]
        go_id = command([str(binary), "environment", "identity"], env=dict(os.environ, MULTICA_ENVIRONMENT_INPUT_FILE=str(input_file))).strip()
        if go_id != helm_id or hashlib.sha256(payload.encode()).hexdigest() != go_id:
            raise SystemExit(f"Go and Helm canonical identity disagree: {case['name']}")
        script = config["data"].get("bootstrap.sh", external_script)
        if hashlib.sha256(script.encode()).hexdigest() != input_data["scriptSHA256"]:
            raise SystemExit(f"rendered script bytes differ from their identity: {case['name']}")
        if case["name"] in {item["name"] for item in shared} and script != values["environment"]["bootstrap"]["script"]:
            raise SystemExit("Helm changed supplied inline script bytes")
        outcomes.append({"case": case["name"], "environmentID": go_id, "scriptSHA256": input_data["scriptSHA256"], "passed": True})
    (evidence / "result.json").write_text(json.dumps(outcomes, indent=2) + "\n")
    print("Go/Helm identity and exact script bytes agree for bundled, inline Unicode/newline, and external sources")


if __name__ == "__main__":
    main()

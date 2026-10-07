#!/usr/bin/env python3
"""Run the controller contract proof against an isolated, unchanged backend."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True, type=Path)
    parser.add_argument("--expected-head", required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    upstream = args.upstream.resolve()
    proof = Path(tempfile.mkdtemp(prefix="multica-backend-contract-", dir="/private/tmp"))
    os.chmod(proof, 0o700)
    suffix = secrets.token_hex(6)
    network = "multica-contract-" + suffix
    database = network + "-db"
    backend = network + "-api"
    migration = network + "-migrate"
    contract = network + "-test"
    receipt = {"directory": str(proof), "upstream": str(upstream), "commands": [], "status": "running"}
    resources = []

    # Build and test processes receive no ambient application or cloud secrets.
    environment = {"PATH": os.environ["PATH"], "LANG": "C.UTF-8", "TMPDIR": str(proof)}
    for key in ("HOME", "GOCACHE", "GOMODCACHE", "GOPATH", "DOCKER_HOST", "DOCKER_CONTEXT"):
        if key in os.environ:
            environment[key] = os.environ[key]
    if environment.get("DOCKER_HOST", "unix://").startswith("unix://") is False:
        raise SystemExit("the proof refuses a non-local DOCKER_HOST")

    def run(label, command, *, env=None, check=True, timeout=600):
        result = subprocess.run(command, cwd=repo, env=env or environment,
                                capture_output=True, text=True, timeout=timeout)
        (proof / (label + ".log")).write_text(result.stdout + result.stderr)
        receipt["commands"].append({"label": label, "argv": command, "exit_code": result.returncode})
        if check and result.returncode:
            raise RuntimeError(label + " failed; inspect " + str(proof / (label + ".log")))
        return result.stdout.strip()

    def local_file(name, data):
        path = proof / name
        path.write_text(data)
        path.chmod(0o600)
        return path

    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    try:
        receipt["proof_fixture_hashes"] = {str(path.relative_to(repo)): sha(path) for path in (
            Path(__file__).resolve(), repo / "src/internal/controller/backend_contract_test.go",
            repo / "src/internal/controller/continuity_selection.go", repo / "src/internal/daemonapi/observation.go",
            repo / "src/internal/daemonapi/authority.go", repo / "src/internal/daemonapi/history.go",
            repo / "src/internal/daemonapi/execution.go", repo / "src/internal/daemonapi/mcp.go",
            repo / "src/internal/daemonapi/compatibility.go")}
        endpoint = run("docker-context", ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"])
        if not endpoint.startswith("unix://"):
            raise RuntimeError("the proof requires a local Unix-socket Docker context")
        head = run("upstream-head", ["git", "-C", str(upstream), "rev-parse", "HEAD"])
        baseline = run("upstream-before", ["git", "-C", str(upstream), "status", "--porcelain=v1"])
        if head != args.expected_head or baseline:
            raise RuntimeError("upstream revision or clean-tree precondition failed")
        receipt["upstream_head"] = head
        receipt["go_version"] = run("go-version", ["go", "version"])
        architecture = run("architecture", ["docker", "version", "--format", "{{.Server.Arch}}"])
        if architecture not in ("amd64", "arm64"):
            raise RuntimeError("unsupported local Docker architecture")
        build_env = dict(environment, GOOS="linux", GOARCH=architecture, CGO_ENABLED="0")
        for target in ("server", "migrate"):
            run("build-" + target, ["go", "-C", str(upstream / "server"), "build", "-mod=readonly",
                                   "-trimpath", "-o", str(proof / target), "./cmd/" + target], env=build_env)
            receipt[target + "_sha256"] = sha(proof / target)
            run(target + "-build-info", ["go", "version", "-m", str(proof / target)])
        run("build-contract", ["go", "-C", "src", "test", "-c", "-mod=readonly", "-o", str(proof / "contract.test"),
                               "./internal/controller"], env=build_env)
        receipt["contract_sha256"] = sha(proof / "contract.test")
        receipt["source_contract_hashes"] = {
            str(path.relative_to(upstream)): sha(path)
            for path in sorted((upstream / "server").rglob("*.go"))
        }
        receipt["migration_hashes"] = {
            path.name: sha(path) for path in sorted((upstream / "server/migrations").glob("*.up.sql"))
        }
        image = run("database-image", ["docker", "image", "inspect", "pgvector/pgvector:pg17", "--format", "{{.Id}}"])
        receipt["dependency_image"] = image
        resources.append(("network", network))
        run("create-network", ["docker", "network", "create", "--internal", network])
        password = secrets.token_hex(24)
        db_env = local_file("database.env", "POSTGRES_USER=contract\nPOSTGRES_DB=contract\nPOSTGRES_PASSWORD=" + password + "\n")
        resources.append(("container", database))
        run("start-database", ["docker", "run", "-d", "--name", database, "--network", network,
                               "--env-file", str(db_env), "--tmpfs", "/var/lib/postgresql/data:rw,mode=0700", image])
        for attempt in range(60):
            ready = subprocess.run(["docker", "exec", database, "pg_isready", "-U", "contract", "-d", "contract"],
                                   env=environment, capture_output=True, text=True)
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("isolated PostgreSQL readiness timed out")
        code = str(secrets.randbelow(900000) + 100000)
        backend_env = local_file("backend.env", "\n".join([
            "DATABASE_URL=postgres://contract:" + password + "@" + database + ":5432/contract?sslmode=disable",
            "JWT_SECRET=" + secrets.token_hex(32), "APP_ENV=development", "DO_NOT_TRACK=1",
            "MULTICA_DEV_VERIFICATION_CODE=" + code, "PORT=8080", "LOG_LEVEL=warn", "AWS_EC2_METADATA_DISABLED=true",
            "LOCAL_UPLOAD_DIR=/tmp/uploads", "",
        ]))
        receipt["database_destination"] = {"container": database, "network": network, "database": "contract", "published_ports": []}
        common = ["docker", "run", "--network", network, "--read-only", "--cap-drop", "ALL",
                  "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,mode=1777",
                  "--env-file", str(backend_env), "--workdir", "/proof",
                  "--mount", "type=bind,source=" + str(proof) + ",target=/proof,readonly",
                  "--mount", "type=bind,source=" + str(upstream / "server/migrations") + ",target=/proof/migrations,readonly"]
        (proof / "migrations").mkdir()
        resources.append(("container", migration))
        run("migrate", common + ["--rm", "--name", migration, "--entrypoint", "/proof/migrate", image, "up"])
        resources.append(("container", backend))
        run("start-backend", common + ["-d", "--name", backend, "--entrypoint", "/proof/server", image])
        origin = "http://127.0.0.1:8080"
        receipt["backend_origin"] = origin
        receipt["backend_network_namespace"] = backend
        receipt["published_ports"] = []
        test_env = local_file("contract.env", "\n".join(["MULTICA_BACKEND_CONTRACT_URL=" + origin,
                                                       "MULTICA_BACKEND_CONTRACT_CODE=" + code,
                                                       "MULTICA_BACKEND_CONTRACT_PROOF=/evidence", ""]))
        resources.append(("container", contract))
        output = run("controller-contract", ["docker", "run", "--rm", "--name", contract, "--network", "container:" + backend,
                                             "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                                             "--tmpfs", "/tmp:rw,mode=1777", "--env-file", str(test_env),
                                             "--mount", "type=bind,source=" + str(proof) + ",target=/evidence",
                                             "--entrypoint", "/evidence/contract.test", image, "-test.v", "-test.count=1",
                                             "-test.run=^TestUnchangedBackendConversationContract$", "-test.timeout=5m"])
        if "--- PASS: TestUnchangedBackendConversationContract" not in output or "--- SKIP:" in output:
            raise RuntimeError("required live contract test did not pass without skips")
        bridges = [json.loads(path.read_text()) for path in sorted(proof.glob("adapter-bridge-*.json"))]
        if {bridge["conversation_kind"] for bridge in bridges} != {"issue", "agent_dm"}:
            raise RuntimeError("live issue and native-DM preparation bridges were not observed")
        receipt["adapter_bridges"] = bridges
        receipt["status"] = "passed"
    except Exception as error:
        receipt["status"], receipt["error"] = "failed", str(error)
    finally:
        for kind, name in reversed(resources):
            if kind == "container":
                run("logs-" + name, ["docker", "logs", name], check=False)
                run("remove-" + name, ["docker", "rm", "-f", name], check=False)
            else:
                run("remove-" + name, ["docker", "network", "rm", name], check=False)
        receipt["upstream_final_status"] = run("upstream-after", ["git", "-C", str(upstream), "status", "--porcelain=v1"], check=False)
        status_known = receipt["commands"][-1]["exit_code"] == 0
        run("upstream-final-diff", ["git", "-C", str(upstream), "diff", "--exit-code"], check=False)
        diff_clean = receipt["commands"][-1]["exit_code"] == 0
        receipt["upstream_final_head"] = run("upstream-final-head", ["git", "-C", str(upstream), "rev-parse", "HEAD"], check=False)
        receipt["remaining_containers"] = run("remaining-containers", ["docker", "ps", "-a", "--filter", "name=" + network, "--format", "{{.Names}}"], check=False)
        containers_known = receipt["commands"][-1]["exit_code"] == 0
        receipt["remaining_networks"] = run("remaining-networks", ["docker", "network", "ls", "--filter", "name=" + network, "--format", "{{.Name}}"], check=False)
        networks_known = receipt["commands"][-1]["exit_code"] == 0
        receipt["cleanup_verified"] = containers_known and networks_known and not receipt["remaining_containers"] and not receipt["remaining_networks"]
        receipt["changed_proof_sources"] = [path for path, expected in receipt.get("proof_fixture_hashes", {}).items() if sha(repo / path) != expected]
        if not status_known or not diff_clean or receipt["upstream_final_head"] != args.expected_head or receipt["upstream_final_status"] or not receipt["cleanup_verified"]:
            receipt["status"] = "failed"
        if receipt["changed_proof_sources"]:
            receipt["status"] = "failed"
        local_file("receipt.json", json.dumps(receipt, indent=2) + "\n")
        print(json.dumps({key: receipt.get(key) for key in ("directory", "status", "error", "backend_origin", "upstream_head", "remaining_containers")}))
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

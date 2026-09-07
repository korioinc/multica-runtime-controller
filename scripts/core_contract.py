#!/usr/bin/env python3
"""Produce the build contract from the actual platform artifact bytes."""
import argparse
import hashlib
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"), required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--official-version", required=True)
    args = parser.parse_args()
    files = {}
    for name in ("runtime", "multica"):
        path = args.directory / name
        if not path.is_file() or path.is_symlink() or not path.stat().st_mode & 0o111:
            raise ValueError(f"artifact is not executable: {path}")
        files[name] = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
        path.chmod(0o555)
    build = json.dumps({"version": args.version, "commit": args.commit,
                        "platform": args.platform, "files": files}, sort_keys=True,
                       separators=(",", ":")).encode()
    contract = {"contractVersion": 1, "buildID": hashlib.sha256(build).hexdigest(),
                "platform": args.platform, "officialVersion": args.official_version,
                "officialSHA256": files["multica"], "files": files}
    destination = args.directory / "contract.json"
    destination.write_text(json.dumps(contract, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    destination.chmod(0o444)


if __name__ == "__main__":
    main()

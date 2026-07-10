#!/usr/bin/env python3
"""Publish every official exploration to the status page.

Officials are the explorations marked `status: done` in
.workers/promises/*.md frontmatter. Key, command, and depth are read from
the same frontmatter entry, so exploration identity and the command that
produces its evidence are paired by the spec file — never typed by hand.
After each publish the entry's `published:` field is rewritten with the new
exploration id.

Before publishing, the prepared image is checked against local HEAD: HEAD
must be pushed, and if the image lags, `wio projects prepare` runs and is
polled until the image commit matches — runs are always stamped with the
commit you meant to publish.

Usage: .workers/publish.py [--dry-run]
Env:
  WIO             wio binary (needs --exploration support; default: wio)
  WIO_PROJECT_ID  target project (default: the S2 project on prod)
"""

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

try:
    import yaml
except ImportError:
    raise SystemExit(
        "pyyaml not available in this python3 — run as `python3 .workers/publish.py`"
        " with a python that has it, or pip install pyyaml"
    )

ROOT = Path(__file__).resolve().parent.parent
WIO = os.environ.get("WIO", "wio")
PROJECT_ID = os.environ.get("WIO_PROJECT_ID", "kn7d139qrs5knsawmkq1avw8s18a8se2")


def frontmatter(text: str) -> dict:
    match = re.match(r"\A---\n(.*?)\n---\n", text, re.DOTALL)
    return yaml.safe_load(match.group(1)) if match else {}


def officials():
    for spec in sorted((ROOT / ".workers" / "promises").glob("*.md")):
        front = frontmatter(spec.read_text())
        for exploration in front.get("explorations") or []:
            if exploration.get("status") != "done":
                continue
            yield {
                "spec": spec,
                "promise": front["key"],
                "key": exploration["key"],
                "command": exploration["command"],
                "depth": exploration["depth"],
            }


def record_published(spec: Path, key: str, exploration_id: str) -> None:
    """Rewrite the `published:` line inside this exploration's entry only.

    Line-targeted so the surrounding frontmatter (comments, folded scalars)
    survives untouched — never round-trip the YAML through a dumper.
    """
    lines = spec.read_text().splitlines(keepends=True)
    in_entry = False
    for i, line in enumerate(lines):
        if re.match(rf"^\s*- key: {re.escape(key)}\s*$", line):
            in_entry = True
            continue
        if in_entry and re.match(r"^\s*- key: |^---", line):
            break
        if in_entry and re.match(r"^\s*published:", line):
            indent = line[: len(line) - len(line.lstrip())]
            lines[i] = f"{indent}published: {exploration_id}\n"
            spec.write_text("".join(lines))
            return
    raise SystemExit(f"{spec.name}: no `published:` line under exploration {key}")


PREPARE_TIMEOUT_S = 15 * 60
POLL_INTERVAL_S = 10


def git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, text=True, check=True
    ).stdout.strip()


def image_commit() -> str | None:
    result = subprocess.run(
        [WIO, "projects", "get", "--format", "json", PROJECT_ID],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"projects get failed:\n{result.stderr or result.stdout}")
    image = json.loads(result.stdout)["preparation"]["currentImage"]
    return image["commitSha"] if image else None


def ensure_image_at_head(dry_run: bool) -> None:
    """Block until the prepared image is exactly local HEAD."""
    head = git("rev-parse", "HEAD")
    upstream = git("rev-parse", "@{u}")
    if head != upstream:
        raise SystemExit(
            f"HEAD {head[:7]} is not pushed (upstream at {upstream[:7]}) — "
            "push first; the image is prepared from the remote branch"
        )
    current = image_commit()
    if current == head:
        print(f"image at HEAD ({head[:7]})")
        return
    print(f"image at {current[:7] if current else 'none'}, HEAD is {head[:7]}")
    if dry_run:
        print("  would run: wio projects prepare (then poll until it matches)")
        return
    subprocess.run(
        [WIO, "projects", "prepare", "--format", "json", PROJECT_ID],
        capture_output=True,
        text=True,
    )
    deadline = time.monotonic() + PREPARE_TIMEOUT_S
    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL_S)
        current = image_commit()
        if current == head:
            print(f"  prepared ({head[:7]})")
            return
    raise SystemExit(f"image still at {current[:7] if current else 'none'} "
                     f"after {PREPARE_TIMEOUT_S}s — check `wio projects get`")


def main() -> None:
    dry_run = "--dry-run" in sys.argv[1:]
    plan = list(officials())
    if not plan:
        raise SystemExit("no official explorations (status: done) found")
    ensure_image_at_head(dry_run)

    for entry in plan:
        argv = [
            WIO, "simulate", "create",
            "--command", entry["command"],
            "--exploration", entry["key"],
            "--depth", str(entry["depth"]),
            "--format", "json",
            PROJECT_ID,
        ]
        print(f"{entry['key']} (depth {entry['depth']}): {entry['command']}")
        if dry_run:
            print(f"  would run: {' '.join(argv)}")
            continue
        deadline = time.monotonic() + PREPARE_TIMEOUT_S
        while True:
            result = subprocess.run(argv, capture_output=True, text=True)
            if result.returncode == 0:
                break
            err = result.stderr or result.stdout
            # earlier officials' runs hold the runtime slots — wait them out
            if "worker_capacity_full" in err and time.monotonic() < deadline:
                print("  runtime slots busy, retrying in 30s")
                time.sleep(30)
                continue
            raise SystemExit(f"  publish failed:\n{err}")
        entry["published"] = json.loads(result.stdout)["explorationId"]
        print(f"  published: {entry['published']}")

    # Rewrite `published:` lines only after every publish has fired: the CLI's
    # git gate requires a clean pushed tree, so mutating a spec between
    # publishes would fail the next one.
    if not dry_run:
        for entry in plan:
            record_published(entry["spec"], entry["key"], entry["published"])
        print(f"\nStatus page: projects/{PROJECT_ID}/promises/…")
        print("commit + push the rewritten `published:` fields")


if __name__ == "__main__":
    main()

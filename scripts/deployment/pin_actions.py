"""
Pin every third-party GitHub Action in .github/workflows to an immutable commit SHA.

A mutable tag (`@v4`) lets whoever controls the action repo change what runs in our pipeline
(including the jobs that hold AWS credentials). A full SHA cannot move. The original tag is kept as a
trailing comment so Dependabot / humans can still read and bump it.

    python scripts/deployment/pin_actions.py            # rewrite `uses: owner/repo@vN` -> `@<sha>  # vN`
    python scripts/deployment/pin_actions.py --check    # exit 1 if any action is not SHA-pinned (CI)

Resolving a tag needs network access (the public GitHub REST API; set GITHUB_TOKEN to raise rate limits).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
USES = re.compile(
    r"^(?P<lead>\s*-?\s*uses:\s*)(?P<action>[\w.-]+/[\w./-]+)@(?P<ref>[^\s#]+)(?P<tail>.*)$"
)
SHA = re.compile(r"^[0-9a-f]{40}$")


def _get(url: str) -> dict:
    req = urllib.request.Request(
        url, headers={"Accept": "application/vnd.github+json", "User-Agent": "pin-actions"}
    )
    if os.environ.get("GITHUB_TOKEN"):
        req.add_header("Authorization", f"Bearer {os.environ['GITHUB_TOKEN']}")
    with urllib.request.urlopen(req, timeout=30) as resp:  # fixed https host
        return json.load(resp)


def resolve(action: str, tag: str) -> str:
    """Commit SHA a tag points to (following annotated tags)."""
    repo = "/".join(action.split("/")[:2])
    ref = _get(f"https://api.github.com/repos/{repo}/git/ref/tags/{tag}")["object"]
    if ref["type"] == "tag":  # annotated tag -> dereference to the commit
        ref = _get(f"https://api.github.com/repos/{repo}/git/tags/{ref['sha']}")["object"]
    return str(ref["sha"])


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--check", action="store_true", help="do not rewrite; fail if an action is not pinned"
    )
    args = p.parse_args(argv)

    unpinned: list[str] = []
    cache: dict[tuple[str, str], str] = {}
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        crlf = "\r\n" in text
        out = []
        changed = False
        for line in text.replace("\r\n", "\n").split("\n"):
            m = USES.match(line)
            if m and not m["action"].startswith("./") and not SHA.match(m["ref"]):
                unpinned.append(f"{path.name}: {m['action']}@{m['ref']}")
                if not args.check:
                    key = (m["action"], m["ref"])
                    cache.setdefault(key, resolve(*key))
                    line = f"{m['lead']}{m['action']}@{cache[key]}  # {m['ref']}"
                    changed = True
            out.append(line)
        if changed:
            new = "\n".join(out)
            with open(path, "w", encoding="utf-8", newline="") as fh:
                fh.write(new.replace("\n", "\r\n") if crlf else new)
            print("pinned", path.name)
    if args.check and unpinned:
        print("actions not pinned to a commit SHA:\n  " + "\n  ".join(unpinned), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

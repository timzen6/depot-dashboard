"""Cross-platform `qc dev` subcommands for developer and LLM-context workflows.

All file I/O uses explicit UTF-8 and all subprocess calls use argument lists
(no shell) so the commands behave identically on Windows, macOS, and Linux.
"""

import argparse
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger

DEFAULT_CONTEXT_DOCS = ["README.md", "docs/STRUCTURE.md", "docs/ARCH_DECISIONS.md"]
DEFAULT_PROJECT_EXPORT_OUTPUT = Path("llm_inputs/project_items.json")
DEFAULT_PR_DIFF_OUTPUT = Path("llm_inputs/pr_context_report.txt")
DEFAULT_PROJECT_OWNER = "timzen6"

# Git magic pathspecs, passed as list items (never wrapped in literal quotes).
PR_DIFF_EXCLUDES = [
    ":(exclude)uv.lock",
    ":(exclude)*.pyc",
    ":(exclude)data/*",
    ":(exclude)llm_inputs/*",
]


def _utc_timestamp() -> str:
    """Current UTC time as an ISO-8601 string."""
    return datetime.now(UTC).isoformat()


def _repo_root() -> Path:
    """Resolve the git repository root, falling back to the current directory."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        return Path(result.stdout.strip())
    except (FileNotFoundError, subprocess.CalledProcessError):
        logger.warning("Could not resolve git repo root; falling back to current directory")
        return Path.cwd()


def _atomic_write_text(path: Path, text: str) -> None:
    """Write text to path atomically: write to a .tmp file, then rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _copy_to_clipboard(text: str) -> None:
    """Copy text to the clipboard, degrading gracefully on headless systems.

    pyperclip lives in the dev dependency group, so it is imported lazily to
    keep production `qc` runs working without dev dependencies installed.
    """
    try:
        import pyperclip
    except ImportError:
        logger.warning(
            "pyperclip not installed (dev dependency group missing); skipping clipboard copy"
        )
        return
    try:
        pyperclip.copy(text)
    except pyperclip.PyperclipException as e:
        logger.warning(f"Clipboard unavailable ({e}); skipping clipboard copy")


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a command without a shell, decoding output as UTF-8."""
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
    except FileNotFoundError:
        logger.error(f"Required tool {cmd[0]!r} not found on PATH. Install it and retry.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}: {' '.join(cmd)}\n{e.stderr}")
        sys.exit(1)


def _gh_authenticated_user() -> str:
    """Resolve the authenticated GitHub user via gh, falling back to a default."""
    try:
        result = subprocess.run(
            ["gh", "api", "user", "--jq", ".login"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        login = result.stdout.strip()
        if login:
            return login
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass
    logger.warning(
        f"Could not resolve GitHub user via gh; falling back to {DEFAULT_PROJECT_OWNER!r}"
    )
    return DEFAULT_PROJECT_OWNER


def dev_context(docs: list[str], output: Path | None, no_clipboard: bool) -> None:
    """Bundle project documentation into a single Markdown payload."""
    repo_root = _repo_root()
    parts = ["# Project Context", f"Generated on: {_utc_timestamp()}", ""]
    included: list[str] = []
    for doc in docs:
        path = repo_root / doc
        if not path.is_file():
            logger.error(f"Context file not found: {doc} (resolved to {path})")
            sys.exit(1)
        rel = Path(doc).as_posix()
        content = path.read_text(encoding="utf-8")
        parts.append(f"## {rel}")
        parts.append("")
        parts.append(content)
        parts.append("")
        included.append(rel)
    payload = "\n".join(parts)
    if output is not None:
        _atomic_write_text(output, payload)
        logger.info(f"Context written to {output}")
    if not no_clipboard:
        _copy_to_clipboard(payload)
    logger.success(f"Project context ready with {len(included)} file(s): {', '.join(included)}")


def dev_project_export(project_number: int, owner: str, output: Path, no_clipboard: bool) -> None:
    """Export GitHub Project items into an LLM-friendly JSON file."""
    if shutil.which("gh") is None:
        logger.error(
            "GitHub CLI 'gh' not found on PATH. Install it from https://cli.github.com/ and retry."
        )
        sys.exit(1)
    result = _run(
        ["gh", "project", "item-list", str(project_number), "--owner", owner, "--format", "json"]
    )
    raw: dict[str, Any] = json.loads(result.stdout)
    items = [
        {
            "number": (item.get("content") or {}).get("number"),
            "title": item.get("title"),
            "status": item.get("status"),
            "body": (item.get("content") or {}).get("body"),
        }
        for item in raw.get("items", [])
    ]
    payload = json.dumps(items, indent=2, ensure_ascii=False) + "\n"
    _atomic_write_text(output, payload)
    if not no_clipboard:
        _copy_to_clipboard(payload)
    logger.success(f"Exported {len(items)} project item(s) to {output}")


def _parse_remote_base(base: str) -> tuple[str, str] | None:
    """Split a 'remote/branch' base into (remote, branch); None for local branches."""
    if "/" in base:
        remote, _, branch = base.partition("/")
        return remote, branch
    return None


def dev_pr_diff(base: str, output: Path, no_fetch: bool, no_clipboard: bool) -> None:
    """Generate a structured PR diff report against a base branch."""
    if not no_fetch:
        parsed = _parse_remote_base(base)
        if parsed is not None:
            remote, branch = parsed
            logger.info(f"Fetching latest {remote}/{branch}...")
            _run(["git", "fetch", remote, branch])
        else:
            logger.info(f"Base {base!r} has no remote prefix; skipping fetch")
    merge_base = _run(["git", "merge-base", base, "HEAD"]).stdout.strip()
    log = _run(["git", "log", "--format=- %s", f"{merge_base}..HEAD"]).stdout
    stat = _run(
        ["git", "diff", "--stat", f"{merge_base}..HEAD", "--", ".", *PR_DIFF_EXCLUDES]
    ).stdout
    diff = _run(["git", "diff", f"{merge_base}..HEAD", "--", ".", *PR_DIFF_EXCLUDES]).stdout
    report = "\n".join(
        [
            "# PR Context Report",
            f"Generated on: {_utc_timestamp()}",
            "",
            "## 1. Commit History (unique to Branch)",
            log,
            "",
            "## 2. Code Change Stats (unique to Branch)",
            stat,
            "",
            "## 3. Code Diff (unique to Branch)",
            diff,
            "",
        ]
    )
    _atomic_write_text(output, report)
    if not no_clipboard:
        _copy_to_clipboard(report)
    logger.success(f"PR context report saved to {output} (base: {base}, merge-base: {merge_base})")


def cmd_dev_context(args: argparse.Namespace) -> None:
    dev_context(args.docs, args.output, args.no_clipboard)


def cmd_dev_project_export(args: argparse.Namespace) -> None:
    owner = args.owner if args.owner is not None else _gh_authenticated_user()
    dev_project_export(args.project_number, owner, args.output, args.no_clipboard)


def cmd_dev_pr_diff(args: argparse.Namespace) -> None:
    dev_pr_diff(args.base, args.output, args.no_fetch, args.no_clipboard)

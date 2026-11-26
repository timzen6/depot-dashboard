import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src import dev_commands


def _completed(cmd: list[str], stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")


def test_context_builds_payload_and_copies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "README.md").write_text("hello", encoding="utf-8")
    out = tmp_path / "ctx.md"
    with patch.object(dev_commands, "_copy_to_clipboard") as clip:
        dev_commands.dev_context(["README.md"], out, no_clipboard=False)
    text = out.read_text(encoding="utf-8")
    assert text.startswith("# Project Context")
    assert "Generated on: " in text
    assert "## README.md" in text
    assert "hello" in text
    clip.assert_called_once_with(text)


def test_context_missing_file_exits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit):
        dev_commands.dev_context(["nope.md"], None, no_clipboard=True)


def test_context_no_clipboard_skips_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "README.md").write_text("hi", encoding="utf-8")
    with patch.object(dev_commands, "_copy_to_clipboard") as clip:
        dev_commands.dev_context(["README.md"], None, no_clipboard=True)
    clip.assert_not_called()


def test_project_export_transforms_items(tmp_path: Path) -> None:
    gh_payload = {
        "items": [
            {"title": "Task A", "status": "Todo", "content": {"number": 3, "body": "do it"}},
            {"title": "Task B", "status": "Done", "content": {"number": 4, "body": None}},
        ]
    }
    fake = _completed(["gh"], stdout=json.dumps(gh_payload))
    out = tmp_path / "llm_inputs" / "project_items.json"
    with (
        patch("shutil.which", return_value="/usr/bin/gh"),
        patch("subprocess.run", return_value=fake) as run,
        patch.object(dev_commands, "_copy_to_clipboard"),
    ):
        dev_commands.dev_project_export(1, "timzen6", out, no_clipboard=False)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data == [
        {"number": 3, "title": "Task A", "status": "Todo", "body": "do it"},
        {"number": 4, "title": "Task B", "status": "Done", "body": None},
    ]
    cmd = run.call_args[0][0]
    assert cmd == ["gh", "project", "item-list", "1", "--owner", "timzen6", "--format", "json"]


def test_project_export_requires_gh(tmp_path: Path) -> None:
    with patch("shutil.which", return_value=None), pytest.raises(SystemExit):
        dev_commands.dev_project_export(1, "owner", tmp_path / "x.json", no_clipboard=True)


def _fake_git(calls: list[list[str]]) -> Any:
    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        if cmd[:2] == ["git", "merge-base"]:
            return _completed(cmd, stdout="abc123\n")
        return _completed(cmd, stdout="some output\n")

    return fake_run


def test_pr_diff_fetches_remote_base_and_excludes(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    out = tmp_path / "llm_inputs" / "report.txt"
    with (
        patch("shutil.which", return_value="/usr/bin/git"),
        patch("subprocess.run", side_effect=_fake_git(calls)),
        patch.object(dev_commands, "_copy_to_clipboard"),
    ):
        dev_commands.dev_pr_diff("origin/main", out, no_fetch=False, no_clipboard=True)
    assert ["git", "fetch", "origin", "main"] in calls
    assert ["git", "merge-base", "origin/main", "HEAD"] in calls
    diff_calls = [c for c in calls if c[:2] == ["git", "diff"]]
    assert len(diff_calls) == 2
    for c in diff_calls:
        assert ":(exclude)uv.lock" in c
        assert ":(exclude)llm_inputs/*" in c
        assert not any("'" in part for part in c)
    report = out.read_text(encoding="utf-8")
    assert report.startswith("# PR Context Report")
    assert "## 1. Commit History (unique to Branch)" in report
    assert "## 2. Code Change Stats (unique to Branch)" in report
    assert "## 3. Code Diff (unique to Branch)" in report


def test_pr_diff_local_base_skips_fetch(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    with (
        patch("shutil.which", return_value="/usr/bin/git"),
        patch("subprocess.run", side_effect=_fake_git(calls)),
        patch.object(dev_commands, "_copy_to_clipboard"),
    ):
        dev_commands.dev_pr_diff("main", tmp_path / "r.txt", no_fetch=False, no_clipboard=True)
    assert not any(c[:2] == ["git", "fetch"] for c in calls)


def test_pr_diff_no_fetch_flag(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    with (
        patch("shutil.which", return_value="/usr/bin/git"),
        patch("subprocess.run", side_effect=_fake_git(calls)),
        patch.object(dev_commands, "_copy_to_clipboard"),
    ):
        dev_commands.dev_pr_diff(
            "upstream/feature/x", tmp_path / "r.txt", no_fetch=True, no_clipboard=True
        )
    assert not any(c[:2] == ["git", "fetch"] for c in calls)


def test_clipboard_headless_fallback_does_not_raise() -> None:
    fake_pyperclip = MagicMock()
    fake_pyperclip.PyperclipException = type("PyperclipException", (Exception,), {})
    fake_pyperclip.copy.side_effect = fake_pyperclip.PyperclipException("no clipboard mechanism")
    with patch.dict("sys.modules", {"pyperclip": fake_pyperclip}):
        dev_commands._copy_to_clipboard("payload")  # must not raise
    fake_pyperclip.copy.assert_called_once_with("payload")

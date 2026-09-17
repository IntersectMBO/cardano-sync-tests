"""Tests that the workflow upload globs claim every artifact file exactly once.

The upload paths live in the workflow YAML, and the log/monitor split is decided
by `helpers.list_sync_log_files`. Nothing else ties the two together, so a
dropped exclusion line or a changed helper would silently put a log in two
artifacts, or in none. These tests read the real workflow files and apply their
real globs to a sample workdir.
"""

from __future__ import annotations

import pathlib as pl

import pytest
import yaml

from sync_tests.utils import helpers

_REPO_ROOT = pl.Path(__file__).resolve().parents[1]
_WORKFLOWS = _REPO_ROOT / ".github" / "workflows"

# Every file a sync run can leave in the workdir. Grouped only so a failure says
# which kind of file went missing.
_NODE_FILES = (
    "node_sync_results.json",
    "sync_markers_preview.json",
    "sync_progress_preview.json",
    "ci_step.log",
    "nix_build.log",
    "node_sync.log",
    "heartbeat.log",
    "monitor.log",
    "monitor_stderr.log",
    "oom.log",
)
_DB_SYNC_FILES = (
    "db_sync_results.json",
    "db_sync.log",
    "cardano-db-sync/db_sync_perf.json",
    "postgres/postgres.log",
)


def _upload_paths(workflow: str) -> dict[str, list[str]]:
    """Return {step name: path patterns} for each upload-artifact step."""
    spec = yaml.safe_load((_WORKFLOWS / workflow).read_text())
    job = next(iter(spec["jobs"].values()))
    steps = [s for s in job["steps"] if "upload-artifact" in str(s.get("uses", ""))]
    assert steps, f"No upload-artifact step found in {workflow}"
    return {
        s["name"]: [ln.strip() for ln in str(s["with"]["path"]).strip().splitlines() if ln.strip()]
        for s in steps
    }


def _select(root: pl.Path, patterns: list[str]) -> set[str]:
    """Apply upload-artifact glob patterns, honouring `!` excludes.

    Uses `pl.Path.glob`, where `*` does not cross a directory separator. This
    matches the action's globber. `fnmatch` does cross one and would wrongly
    report a file in a subdirectory as matched.
    """
    include: set[str] = set()
    exclude: set[str] = set()
    for pattern in patterns:
        if pattern.startswith("#"):
            continue
        negated = pattern.startswith("!")
        hits = {str(p.relative_to(root)) for p in root.glob(pattern.lstrip("!")) if p.is_file()}
        (exclude if negated else include).update(hits)
    return include - exclude


@pytest.fixture
def workdir(tmp_path: pl.Path) -> pl.Path:
    """Build a sample run tree, laid out exactly as a real run leaves it."""
    for name in _NODE_FILES + _DB_SYNC_FILES:
        target = tmp_path / "test_workdir" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()
    return tmp_path


@pytest.mark.parametrize("workflow", ["node_sync_test.yaml", "db_sync_full_sync.yaml"])
def test_no_file_lands_in_two_artifacts(workdir: pl.Path, workflow: str) -> None:
    selections = {name: _select(workdir, pats) for name, pats in _upload_paths(workflow).items()}

    duplicated = {}
    for name, files in selections.items():
        for other, other_files in selections.items():
            if other <= name:
                continue
            both = files & other_files
            if both:
                duplicated[f"{name} + {other}"] = sorted(both)

    assert not duplicated, f"{workflow} uploads the same file twice: {duplicated}"


def test_db_sync_workflow_claims_every_file(workdir: pl.Path) -> None:
    # The db-sync run is the superset: it writes both node and db-sync files.
    selections = _select_all(workdir, "db_sync_full_sync.yaml")
    present = {
        str(p.relative_to(workdir)) for p in (workdir / "test_workdir").rglob("*") if p.is_file()
    }

    assert not present - selections, f"Files no artifact uploads: {sorted(present - selections)}"


def test_node_workflow_claims_every_node_file(workdir: pl.Path) -> None:
    selections = _select_all(workdir, "node_sync_test.yaml")
    node_only = {f"test_workdir/{name}" for name in _NODE_FILES}

    assert not node_only - selections, (
        f"Node files no artifact uploads: {sorted(node_only - selections)}"
    )


def test_node_workflow_leaves_db_sync_files_alone(workdir: pl.Path) -> None:
    # A node-only run never writes these, and the node workflow must not claim
    # them; the db-sync workflow owns them.
    selections = _select_all(workdir, "node_sync_test.yaml")

    assert "test_workdir/postgres/postgres.log" not in selections
    assert "test_workdir/cardano-db-sync/db_sync_perf.json" not in selections


def _select_all(root: pl.Path, workflow: str) -> set[str]:
    selected: set[str] = set()
    for patterns in _upload_paths(workflow).values():
        selected |= _select(root, patterns)
    return selected


@pytest.mark.parametrize("workflow", ["node_sync_test.yaml", "db_sync_full_sync.yaml"])
def test_logs_glob_matches_the_helper(workdir: pl.Path, workflow: str) -> None:
    """The workflow's log selection must agree with `list_sync_log_files`.

    The tests assert on the helper's output, and the workflow uploads what its
    glob matches. If these drift, a log the test vouched for is not uploaded.
    """
    logs_step = next(
        pats for name, pats in _upload_paths(workflow).items() if "logs" in name.lower()
    )
    from_workflow = _select(workdir, logs_step)
    from_helper = {
        f"test_workdir/{p.name}" for p in helpers.list_sync_log_files(workdir / "test_workdir")
    }

    assert from_workflow == from_helper, (
        f"{workflow} log glob and list_sync_log_files disagree: "
        f"only in workflow {sorted(from_workflow - from_helper)}, "
        f"only in helper {sorted(from_helper - from_workflow)}"
    )

"""E2E for the reuse of a passed pull request result by the run after a merge (scripts/quality/ci_reuse.py and .github/workflows/ci.yml).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_ci_reuse_e2e``. The dynamic cases copy the real decision script and
config/quality/ci_jobs.json into a scratch Git repository and run ``python -m scripts.quality.ci_reuse`` as the plan job does, with the event
variables of GitHub Actions and a stand-in ``gh`` first on PATH that answers the API paths from a table. Only the stand-in answers differ between
cases: a world in which every condition holds gives ``reuse=true``, and each negative case breaks exactly one condition (a different tree, no pull
request, two pull requests, no run, a failed, running, other-workflow or older-than-latest run, a skipped, missing or failing job, an API error, a
malformed answer, another event or branch, a tree that the checkout and the API disagree on) and passes only when the answer is ``reuse=false`` and
the summary names that cause. The stand-in records every call, and a case checks that each one is a read (``gh api --method GET``).
The static cases read .github/workflows/ci.yml: the job list equals config/quality/ci_jobs.json, every checkout uses the pull request head, every
heavy job waits for the plan job and runs unless it answers ``true`` (also for an empty answer), the reuse job runs the commit checks and the quick
gates, and the plan job's token is read-only. Failure modes R1 to R18 (plans, local). The report goes to
data/e2e/ci_reuse/<UTC>_<pid>/report.json with the revision and the SHA-256 of the files under test.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import clear_git_environment, run_command
from scripts.quality.repo_checks import load_yaml

ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(ROOT / ".venv" / "bin" / "python")
FILES = ("scripts/quality/ci_reuse.py", "scripts/process.py", "config/quality/ci_jobs.json")
WATCHED = (*FILES, ".github/workflows/ci.yml", "scripts/quality/run_ci_reuse_e2e.py")
REPOSITORY = "owner/project"
HEAD = "b" * 40
RUN = 555
PULL = 7
HEAD_REF = "${{ github.event.pull_request.head.sha || github.sha }}"
FAKE_GH = f"""#!{PYTHON}
import json
import os
import sys

with open(os.environ["FAKE_GH_CALLS"], "a", encoding="utf-8") as calls:
    calls.write(json.dumps(sys.argv[1:]) + "\\n")
entry = json.load(open(os.environ["FAKE_GH_ROUTES"], encoding="utf-8")).get(sys.argv[-1])
if entry is None:
    sys.stderr.write("HTTP 404 Not Found: " + sys.argv[-1])
    sys.exit(1)
if entry.get("exit"):
    sys.stderr.write(entry.get("stderr", "error"))
    sys.exit(entry["exit"])
sys.stdout.write(entry["stdout"] if isinstance(entry["stdout"], str) else json.dumps(entry["stdout"]))
"""


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(folder: Path, *args: str) -> str:
    result = run_command(
        "git",
        list(args),
        cwd=folder,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.invalid",
        },
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout.strip()


def job(name: str, conclusion: str = "success", head: str = HEAD) -> dict[str, Any]:
    return {"name": name, "status": "completed", "conclusion": conclusion, "head_sha": head}


def good_world(sha: str, tree: str, required: list[str], skipped: list[str]) -> dict[str, Any]:
    """The answers of the API when every condition for reuse holds."""
    run = {
        "id": RUN,
        "html_url": f"https://github.com/{REPOSITORY}/actions/runs/{RUN}",
        "path": ".github/workflows/ci.yml",
        "event": "pull_request",
        "head_sha": HEAD,
        "pull_requests": [{"number": PULL}],
        "created_at": "2026-10-22T10:00:00Z",
        "run_number": 12,
        "run_attempt": 1,
        "status": "completed",
        "conclusion": "success",
    }
    jobs = [job(name) for name in required] + [job(name, "skipped") for name in skipped]
    return {
        "pulls": [{"number": PULL, "merged_at": "2026-10-22T10:30:00Z", "merge_commit_sha": sha, "base": {"ref": "main"}, "head": {"sha": HEAD}}],
        "main_tree": tree,
        "head_tree": tree,
        "runs": [run],
        "jobs": jobs,
    }


def routes_of(world: dict[str, Any], sha: str) -> dict[str, Any]:
    paths = {
        f"repos/{REPOSITORY}/commits/{sha}/pulls?per_page=100": world["pulls"],
        f"repos/{REPOSITORY}/git/commits/{sha}": {"tree": {"sha": world["main_tree"]}},
        f"repos/{REPOSITORY}/git/commits/{HEAD}": {"tree": {"sha": world["head_tree"]}},
        f"repos/{REPOSITORY}/actions/runs?head_sha={HEAD}&event=pull_request&per_page=100": {"workflow_runs": world["runs"]},
        f"repos/{REPOSITORY}/actions/runs/{RUN}/jobs?filter=latest&per_page=100": {"total_count": len(world["jobs"]), "jobs": world["jobs"]},
    }
    return {path: {"stdout": answer} for path, answer in paths.items()} | world.get("overrides", {})


class Scratch:
    """A scratch repository with the real decision script, its job list and a stand-in gh."""

    def __init__(self, folder: Path) -> None:
        self.folder = folder
        for name in FILES:
            (folder / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, folder / name)
        git(folder, "init", "-q", "-b", "main")
        git(folder, "add", "-A")
        git(folder, "commit", "-q", "-m", "chore: scratch")
        self.sha = git(folder, "rev-parse", "HEAD")
        self.tree = git(folder, "rev-parse", "HEAD^{tree}")
        (folder / "bin").mkdir()
        (folder / "bin" / "gh").write_text(FAKE_GH)
        (folder / "bin" / "gh").chmod(0o755)
        config = json.loads((ROOT / "config/quality/ci_jobs.json").read_text())
        self.required: list[str] = config["required_pull_request_jobs"]
        self.skipped: list[str] = config["skipped_in_pull_request_runs"]

    def run(self, name: str, world: dict[str, Any], **environment: str) -> dict[str, Any]:
        routes, calls, output, summary = (self.folder / f"{name}.{suffix}" for suffix in ("routes", "calls", "output", "summary"))
        routes.write_text(json.dumps(routes_of(world, self.sha)))
        for path in (calls, output, summary):
            path.unlink(missing_ok=True)
        env = {
            **os.environ,
            "PATH": f"{self.folder / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_REPOSITORY": REPOSITORY,
            "GITHUB_SHA": self.sha,
            "GITHUB_OUTPUT": str(output),
            "GITHUB_STEP_SUMMARY": str(summary),
            "FAKE_GH_ROUTES": str(routes),
            "FAKE_GH_CALLS": str(calls),
            **environment,
        }
        result = run_command(PYTHON, ["-m", "scripts.quality.ci_reuse"], cwd=self.folder, env=env, timeout=120)
        read = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        return {
            "exit": result.returncode,
            "stderr": result.stderr.strip()[-300:],
            "output": output.read_text() if output.exists() else "",
            "summary": summary.read_text() if summary.exists() else "",
            "calls": read,
        }


def reuse_case(scratch: Scratch) -> dict[str, Any]:
    outcome = scratch.run("reuse_is_recorded_in_the_summary", good_world(scratch.sha, scratch.tree, scratch.required, scratch.skipped))
    summary = outcome["summary"]
    checks = {
        "exit_zero": outcome["exit"] == 0,
        "reuse_true": "reuse=true" in outcome["output"] and f"run_id={RUN}" in outcome["output"],
        "names_the_pull_request_head_tree_and_run": all(text in summary for text in (f"pull request {PULL}", HEAD, scratch.tree, f"actions/runs/{RUN}")),
        "lists_every_required_job": all(f"| {name} | success |" in summary for name in scratch.required),
        "every_call_is_a_get": bool(outcome["calls"]) and all(call[:3] == ["api", "--method", "GET"] for call in outcome["calls"]),
    }
    return {
        "case": "reuse_is_recorded_in_the_summary",
        "passed": all(checks.values()),
        "checks": checks,
        "output": outcome["output"],
        "stderr": outcome["stderr"],
    }


def negative_case(scratch: Scratch, name: str, change: Callable[[dict[str, Any], Scratch], None], cause: str, **environment: str) -> dict[str, Any]:
    """A case in which exactly one condition is broken: the answer must be reuse=false with the intended cause in the summary and an exit code of 0."""
    world = good_world(scratch.sha, scratch.tree, scratch.required, scratch.skipped)
    change(world, scratch)
    outcome = scratch.run(name, world, **environment)
    passed = outcome["exit"] == 0 and "reuse=false" in outcome["output"] and "reuse=true" not in outcome["output"] and cause in outcome["summary"]
    return {
        "case": name,
        "passed": passed,
        "cause": cause,
        "exit": outcome["exit"],
        "output": outcome["output"],
        "summary": outcome["summary"][-300:],
        "stderr": outcome["stderr"],
    }


def set_run(world: dict[str, Any], **values: Any) -> None:
    world["runs"][0].update(values)


def drop_job(world: dict[str, Any], scratch: Scratch) -> None:
    world["jobs"] = [item for item in world["jobs"] if item["name"] != "Lakehouse Gold Model Metadata E2E"]


def skip_job(world: dict[str, Any], scratch: Scratch) -> None:
    world["jobs"] = [{**item, "conclusion": "skipped"} if item["name"] == "Acquisition Checks" else item for item in world["jobs"]]


def extra_failed_job(world: dict[str, Any], scratch: Scratch) -> None:
    world["jobs"].append(job("A New Job", "failure"))


def two_pulls(world: dict[str, Any], scratch: Scratch) -> None:
    world["pulls"].append({**world["pulls"][0], "number": 8})


def older_green_newer_red(world: dict[str, Any], scratch: Scratch) -> None:
    world["runs"].append({**world["runs"][0], "id": 556, "created_at": "2026-10-22T11:00:00Z", "run_number": 13, "conclusion": "failure"})


def api_error(world: dict[str, Any], scratch: Scratch) -> None:
    world["overrides"] = {f"repos/{REPOSITORY}/commits/{scratch.sha}/pulls?per_page=100": {"exit": 1, "stderr": "HTTP 403 Forbidden"}}


def malformed(world: dict[str, Any], scratch: Scratch) -> None:
    world["overrides"] = {f"repos/{REPOSITORY}/actions/runs?head_sha={HEAD}&event=pull_request&per_page=100": {"stdout": "this is not json"}}


def missing_field(world: dict[str, Any], scratch: Scratch) -> None:
    world["overrides"] = {f"repos/{REPOSITORY}/git/commits/{HEAD}": {"stdout": {"unexpected": True}}}


def dynamic_cases(scratch: Scratch) -> list[dict[str, Any]]:
    other_tree = "c" * 40
    return [
        reuse_case(scratch),
        negative_case(scratch, "tree_differs_runs_everything", lambda w, s: w.update(head_tree=other_tree), "differs from the tree"),
        negative_case(
            scratch,
            "local_tree_mismatch_runs_everything",
            lambda w, s: w.update(main_tree=other_tree, head_tree=other_tree),
            "the tree of the checkout and the tree the API reports",
        ),
        negative_case(
            scratch, "push_without_pull_request_runs_everything", lambda w, s: w.update(pulls=[]), "no pull request was merged into main with this commit"
        ),
        negative_case(
            scratch,
            "unmerged_pull_request_runs_everything",
            lambda w, s: w["pulls"][0].update(merged_at=None),
            "no pull request was merged into main with this commit",
        ),
        negative_case(scratch, "ambiguous_pull_requests_run_everything", two_pulls, "2 pull requests name this commit"),
        negative_case(scratch, "missing_run_runs_everything", lambda w, s: w.update(runs=[]), "no pull request run of .github/workflows/ci.yml exists"),
        negative_case(scratch, "failed_run_runs_everything", lambda w, s: set_run(w, conclusion="failure"), "conclusion failure"),
        negative_case(scratch, "running_run_runs_everything", lambda w, s: set_run(w, status="in_progress", conclusion=None), "in_progress"),
        negative_case(
            scratch,
            "other_workflow_runs_everything",
            lambda w, s: set_run(w, path=".github/workflows/other.yml"),
            "no pull request run of .github/workflows/ci.yml exists",
        ),
        negative_case(scratch, "older_green_run_does_not_count", older_green_newer_red, "conclusion failure"),
        negative_case(scratch, "skipped_job_runs_everything", skip_job, "the job Acquisition Checks is completed with conclusion skipped"),
        negative_case(scratch, "missing_job_runs_everything", drop_job, "the required job Lakehouse Gold Model Metadata E2E is missing"),
        negative_case(scratch, "failing_job_outside_the_list_runs_everything", extra_failed_job, "the job A New Job is completed with conclusion failure"),
        negative_case(scratch, "api_error_runs_everything", api_error, "the GitHub API answered with an error"),
        negative_case(scratch, "malformed_response_runs_everything", malformed, "JSONDecodeError"),
        negative_case(scratch, "missing_field_runs_everything", missing_field, "KeyError"),
        negative_case(scratch, "other_events_run_everything", lambda w, s: None, "the event is not a push to main", GITHUB_EVENT_NAME="pull_request"),
        negative_case(scratch, "other_branches_run_everything", lambda w, s: None, "the event is not a push to main", GITHUB_REF="refs/heads/topic"),
    ]


def workflow() -> dict[str, Any]:
    loaded = load_yaml((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(".github/workflows/ci.yml is not a mapping")
    return loaded


def fill_matrix(template: str, combination: dict[str, Any]) -> str:
    return re.sub(r"\$\{\{\s*matrix\.(\w+)\s*\}\}", lambda found: str(combination.get(found.group(1), "")), template)


def expanded_names(jobs: dict[str, Any]) -> list[str]:
    """The job names of the workflow with matrix values filled in (a list under ``include``, or one list-valued key)."""
    names: list[str] = []
    for definition in jobs.values():
        matrix = definition.get("strategy", {}).get("matrix")
        if not matrix:
            names.append(str(definition["name"]))
            continue
        combinations = matrix["include"] if "include" in matrix else [{key: value} for key, values in matrix.items() for value in values]
        for combination in combinations:
            names.append(fill_matrix(str(definition["name"]), combination))
    return names


def condition_value(condition: str, reuse: str) -> bool:
    """Evaluate the condition forms the workflow uses: terms joined by ``&&`` of ``!cancelled()`` and a comparison of the plan's answer with 'true'."""
    text = re.sub(r"^\$\{\{\s*|\s*\}\}$", "", condition.strip())
    for term in (part.strip() for part in text.split("&&")):
        if term == "!cancelled()":
            continue
        found = re.fullmatch(r"needs\.plan\.outputs\.reuse (==|!=) 'true'", term)
        if not found:
            raise ValueError(f"the condition term {term!r} is not one the evaluator knows")
        if (reuse == "true") != (found.group(1) == "=="):
            return False
    return True


def static_cases() -> list[dict[str, Any]]:
    document = workflow()
    jobs: dict[str, Any] = document["jobs"]
    config = json.loads((ROOT / "config/quality/ci_jobs.json").read_text())
    listed = set(config["required_pull_request_jobs"]) | set(config["skipped_in_pull_request_runs"])
    names = expanded_names(jobs)
    checkouts = [step for definition in jobs.values() for step in definition["steps"] if str(step.get("uses", "")).startswith("actions/checkout@")]
    heavy = {key: definition for key, definition in jobs.items() if key not in {"plan", "reused_gates"}}
    needs = {key: [definition["needs"]] if isinstance(definition.get("needs"), str) else definition.get("needs", []) for key, definition in jobs.items()}
    reused = jobs["reused_gates"]
    reused_runs = [str(step.get("run", "")) for step in reused["steps"]]
    gate_steps = [step for step in reused["steps"] if step.get("env", {}).get("CI_PART") == "gates"]
    hooks_runs = [str(step.get("run", "")) for step in jobs["hooks"]["steps"]]
    commit_checks = [run for run in reused_runs if "repo_checks commit-range" in run and "repo_checks removed-names" in run]
    plan_steps = jobs["plan"]["steps"]
    answers = {key: [condition_value(str(definition["if"]), reuse) for reuse in ("", "false", "true")] for key, definition in heavy.items()}
    return [
        {
            "case": "required_jobs_match_the_workflow",
            "passed": sorted(names) == sorted(listed)
            and len(names) == len(set(names))
            and not (set(config["skipped_in_pull_request_runs"]) & set(config["required_pull_request_jobs"])),
            "workflow_not_listed": sorted(set(names) - listed),
            "listed_not_in_workflow": sorted(listed - set(names)),
        },
        {
            "case": "pull_request_checkouts_use_the_head",
            "passed": len(checkouts) >= 9 and all(step.get("with", {}).get("ref") == HEAD_REF for step in checkouts),
            "checkouts": len(checkouts),
            "without_the_head": [index for index, step in enumerate(checkouts) if step.get("with", {}).get("ref") != HEAD_REF],
        },
        {
            "case": "heavy_jobs_wait_for_the_plan_and_run_on_an_empty_answer",
            "passed": all("plan" in needs[key] for key in heavy) and all(answers[key] == [True, True, False] for key in heavy) and "reused_gates" in jobs,
            "answers_for_empty_false_true": {key: value for key, value in answers.items() if value != [True, True, False]},
            "without_the_plan": [key for key in heavy if "plan" not in needs[key]],
        },
        {
            "case": "reuse_job_runs_commit_checks_and_gates",
            "passed": len(commit_checks) == 1
            and commit_checks[0] in hooks_runs
            and len(gate_steps) == 1
            and gate_steps[0].get("run") == "bash scripts/run_ci.sh"
            and condition_value(str(reused["if"]), "true")
            and not condition_value(str(reused["if"]), "")
            and not condition_value(str(reused["if"]), "false"),
        },
        {
            "case": "plan_job_is_read_only_and_runs_the_decision_script",
            "passed": set(jobs["plan"]["permissions"].values()) == {"read"}
            and "python3 -m scripts.quality.ci_reuse" in [step.get("run") for step in plan_steps]
            and jobs["plan"]["outputs"]["reuse"] == "${{ steps.reuse.outputs.reuse }}",
            "permissions": jobs["plan"]["permissions"],
        },
        {
            "case": "combine_job_checks_shard_results",
            "passed": "acquisition_shard" in needs["acquisition"]
            and "!cancelled()" in str(jobs["acquisition"]["if"])
            and any(
                "needs.acquisition_shard.result" in str(step.get("env", {}).get("SHARDS", "")) and "exit 1" in str(step.get("run", ""))
                for step in jobs["acquisition"]["steps"]
            ),
        },
    ]


def main() -> int:
    clear_git_environment()
    started = datetime.now(UTC)
    before = {name: sha256_of(ROOT / name) for name in WATCHED}
    folder = Path(tempfile.mkdtemp(prefix="ci_reuse_e2e."))
    try:
        results = static_cases() + dynamic_cases(Scratch(folder))
    finally:
        shutil.rmtree(folder, ignore_errors=True)
    after = {name: sha256_of(ROOT / name) for name in WATCHED}
    if before != after:
        results.append({"case": "code_unchanged_during_run", "passed": False, "changed": sorted(name for name in before if before[name] != after[name])})
    failed = [result["case"] for result in results if not result["passed"]]
    out = ROOT / "data" / "e2e" / "ci_reuse" / f"{started:%Y%m%dT%H%M%SZ}_{os.getpid()}"
    out.mkdir(parents=True)
    revision = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip()
    report = {
        "started": started.isoformat(),
        "revision": revision,
        "code_sha256": before,
        "python": sys.version.split()[0],
        "cases": results,
        "passed": len(results) - len(failed),
        "total": len(results),
        "failed": failed,
    }
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for result in results:
        sys.stdout.write(("PASS " if result["passed"] else "FAIL ") + result["case"] + "\n")
    sys.stdout.write(f"{report['passed']}/{report['total']} passed; report {out / 'report.json'}\n")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())

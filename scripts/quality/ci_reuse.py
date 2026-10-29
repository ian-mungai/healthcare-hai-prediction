"""Decide whether the CI run of a push to ``main`` may reuse the result of the pull request that was merged.

The ``plan`` job of .github/workflows/ci.yml runs ``python3 -m scripts.quality.ci_reuse`` on every run; it uses only the standard library and the
GitHub CLI with the job's read-only token. The answer is ``reuse=true`` only when every check below holds, and ``reuse=false`` with the reason in
every other case, including any error, so a doubt runs every job:

1. the event is a push to ``refs/heads/main``;
2. exactly one pull request was merged into ``main`` with this commit as its merge commit;
3. the tree of the pushed commit (from the API and from the local checkout) is byte-identical to the tree of that pull request's head commit;
4. the latest completed ``pull_request`` run of .github/workflows/ci.yml for that head commit and head branch succeeded, and its jobs are the
   ones listed in config/quality/ci_jobs.json, each exactly once and successful, with no other job failed, cancelled or skipped except the ones listed as
   skipped in pull request runs.

GitHub empties the ``pull_requests`` list of a run once its pull request is merged, so the run is identified by workflow path, event, exact head
commit and the head branch of the pull request (``head_branch`` of the run equals ``head.ref``), not by the pull request number.

A pull request run checks out the head commit itself (every checkout of the workflow uses ``github.event.pull_request.head.sha``), so the tree
that its jobs checked is the tree compared in item 3. A push to ``main`` without a pull request has no merged pull request and runs in full.
The summary names the pull request, the head commit, the tree and the reused run. The reuse job still runs the commit-message and
removed-names checks of the pushed range and the quick gates (``CI_PART=gates``).

E2E: ``.venv/bin/python -m scripts.quality.run_ci_reuse_e2e``.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
JOBS = ROOT / "config" / "quality" / "ci_jobs.json"
WORKFLOW = ".github/workflows/ci.yml"
API_SECONDS = 60


class NoReuse(Exception):
    """The merged result cannot be matched to a passed pull request run; the message is the reason."""


def api(path: str) -> Any:
    """GET ``path`` of the GitHub API with the CLI and return the parsed JSON."""
    result = run_command("gh", ["api", "--method", "GET", "-H", "Accept: application/vnd.github+json", path], timeout=API_SECONDS)
    if result.returncode != 0:
        raise NoReuse(f"the GitHub API answered with an error for {path.split('?')[0]}: {result.stderr.strip()[:200]}")
    return json.loads(result.stdout)


def listed(value: Any, what: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise NoReuse(f"the answer for {what} is not a list of objects")
    return value


def tree_of(repository: str, sha: str) -> str:
    tree = api(f"repos/{repository}/git/commits/{sha}")["tree"]["sha"]
    if not isinstance(tree, str) or not re.fullmatch(r"[0-9a-f]{40}", tree):
        raise NoReuse(f"commit {sha} has no tree hash")
    return tree


def merged_pull_request(repository: str, sha: str) -> dict[str, Any]:
    found = listed(api(f"repos/{repository}/commits/{sha}/pulls?per_page=100"), "the pull requests of the commit")
    merged = [pull for pull in found if pull.get("merged_at") and pull.get("merge_commit_sha") == sha and pull.get("base", {}).get("ref") == "main"]
    if not merged:
        raise NoReuse("no pull request was merged into main with this commit (a push without a pull request runs in full)")
    if len(merged) > 1:
        raise NoReuse(f"{len(merged)} pull requests name this commit as their merge commit")
    return merged[0]


def latest_run(repository: str, head: str, branch: str) -> dict[str, Any]:
    runs = api(f"repos/{repository}/actions/runs?head_sha={head}&event=pull_request&per_page=100")["workflow_runs"]
    own = [
        run
        for run in listed(runs, "the runs of the head commit")
        if run.get("path") == WORKFLOW and run.get("event") == "pull_request" and run.get("head_sha") == head and run.get("head_branch") == branch
    ]
    if not own:
        raise NoReuse(f"no pull request run of {WORKFLOW} exists for the head commit {head}")
    latest = max(own, key=lambda run: (str(run["created_at"]), int(run["run_number"]), int(run["run_attempt"])))
    if latest.get("status") != "completed" or latest.get("conclusion") != "success":
        raise NoReuse(f"the latest pull request run {latest['id']} is {latest.get('status')} with conclusion {latest.get('conclusion')}")
    return latest


def check_jobs(repository: str, run: dict[str, Any], head: str, config: dict[str, Any]) -> list[tuple[str, str]]:
    answer = api(f"repos/{repository}/actions/runs/{run['id']}/jobs?filter=latest&per_page=100")
    jobs = listed(answer["jobs"], "the jobs of the run")
    if answer["total_count"] != len(jobs):
        raise NoReuse(f"the run has {answer['total_count']} jobs and {len(jobs)} were returned")
    names = [str(job["name"]) for job in jobs]
    if len(set(names)) != len(names):
        raise NoReuse("the run has two jobs with the same name")
    by_name = {str(job["name"]): job for job in jobs}
    for name in config["required_pull_request_jobs"]:
        if name not in by_name:
            raise NoReuse(f"the required job {name} is missing from run {run['id']}")
    for name, job in by_name.items():
        if name in config["skipped_in_pull_request_runs"] and name not in config["required_pull_request_jobs"]:
            continue
        if job.get("status") != "completed" or job.get("conclusion") != "success" or job.get("head_sha") != head:
            raise NoReuse(f"the job {name} is {job.get('status')} with conclusion {job.get('conclusion')} for {job.get('head_sha')}")
    return [(name, str(by_name[name]["conclusion"])) for name in config["required_pull_request_jobs"]]


def local_tree() -> str:
    result = run_command("git", ["rev-parse", "HEAD^{tree}"], cwd=ROOT, timeout=60)
    return result.stdout.strip()


def decide(environment: dict[str, str], config: dict[str, Any]) -> dict[str, Any]:
    """Return the facts of the reused run, or raise NoReuse with the reason."""
    if environment.get("GITHUB_EVENT_NAME") != "push" or environment.get("GITHUB_REF") != "refs/heads/main":
        raise NoReuse("the event is not a push to main")
    repository, sha = environment.get("GITHUB_REPOSITORY", ""), environment.get("GITHUB_SHA", "")
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository) or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise NoReuse("the repository or the commit of the run is not valid")
    pull = merged_pull_request(repository, sha)
    head, number, branch = str(pull["head"]["sha"]), int(pull["number"]), pull["head"]["ref"]
    if not isinstance(branch, str) or not branch:
        raise NoReuse(f"pull request {number} has no head branch name")
    main_tree = tree_of(repository, sha)
    if local_tree() != main_tree:
        raise NoReuse("the tree of the checkout and the tree the API reports for the pushed commit differ")
    head_tree = tree_of(repository, head)
    if head_tree != main_tree:
        raise NoReuse(f"the merged tree {main_tree} differs from the tree {head_tree} of the head of pull request {number}")
    run = latest_run(repository, head, branch)
    return {
        "pull_request": number,
        "head": head,
        "tree": main_tree,
        "run_id": run["id"],
        "run_url": run["html_url"],
        "jobs": check_jobs(repository, run, head, config),
    }


def write_outputs(values: dict[str, str]) -> None:
    target = os.environ.get("GITHUB_OUTPUT")
    text = "".join(f"{name}={value}\n" for name, value in values.items())
    if target:
        with open(target, "a", encoding="utf-8") as stream:
            stream.write(text)
    sys.stdout.write(text)


def summarize(lines: list[str]) -> None:
    sys.stdout.write("\n".join(lines) + "\n")
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as stream:
            stream.write("\n".join(lines) + "\n")


def main() -> int:
    try:
        facts = decide(dict(os.environ), json.loads(JOBS.read_text(encoding="utf-8")))
    except (NoReuse, OSError, ValueError, KeyError, TypeError, AttributeError, IndexError, TimeoutExpired) as reason:
        text = str(reason).replace("\n", " ")
        summarize(
            [
                "### Pull request result reuse",
                "",
                f"No reuse, every job runs: {type(reason).__name__}: {text}" if not isinstance(reason, NoReuse) else f"No reuse, every job runs: {text}",
            ]
        )
        write_outputs({"reuse": "false"})
        return 0
    summarize(
        [
            "### Pull request result reuse",
            "",
            f"The jobs of this run are skipped. They passed on pull request {facts['pull_request']}.",
            f"Its head commit {facts['head']} has tree {facts['tree']}, byte-identical to the tree of this commit.",
            f"Reused run: {facts['run_url']} (id {facts['run_id']}). The quick gates and the commit checks run in this run.",
            "",
            "| Required job | Conclusion in the reused run |",
            "| --- | --- |",
            *[f"| {name} | {conclusion} |" for name, conclusion in facts["jobs"]],
        ]
    )
    write_outputs({"reuse": "true", "run_id": str(facts["run_id"]), "pull_request": str(facts["pull_request"]), "tree": facts["tree"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

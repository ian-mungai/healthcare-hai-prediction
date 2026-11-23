"""Exercise source lint gates through their real pre-commit hooks using synthetic sources.

Run ``.venv/bin/python -m scripts.quality.run_lint_coverage_e2e``. Every negative sample must produce its named
diagnostic and every corrected sample must pass. ``--mutant`` replaces the fixture's dispatch with a visible no-op
to prove that the same cases detect missing enforcement. Reports and complete hook logs remain under data/e2e/.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from scripts.process import clear_git_environment, run_command
from scripts.quality.hook_stage import hook_stage

ROOT = Path(__file__).resolve().parents[2]
COPIED = (
    ".pre-commit-config.yaml",
    "pyproject.toml",
    ".yamllint",
    ".sqlfluff",
    "config/quality_tools.json",
    "scripts/process.py",
    "scripts/quality/repo_checks.py",
    "scripts/quality/document_paths.py",
    "scripts/quality/markdown_links.py",
    "scripts/quality/scan_secrets.py",
    "scripts/quality/lint_sources.py",
)
SNAPSHOT = (*COPIED, "scripts/quality/run_lint_coverage_e2e.py")
PROJECT = "name: hai\nversion: '1.0.0'\nconfig-version: 2\nprofile: hai\nmodel-paths: [models]\nmacro-paths: [macros]\n"
PROFILE = "hai:\n  target: lint\n  outputs:\n    lint:\n      type: duckdb\n      path: ':memory:'\n      threads: 1\n"
YAML_PROFILE = (ROOT / ".yamllint").read_text()
# The project config sets large_file_skip_byte_limit = 0, so SQLFluff never skips a file.
# The size-skip samples run with SQLFluff's default limit to prove the hook fails when it does.
SQLFLUFF_SIZE_LIMIT = (ROOT / ".sqlfluff").read_text().replace("large_file_skip_byte_limit = 0", "large_file_skip_byte_limit = 20000")
if "large_file_skip_byte_limit = 20000" not in SQLFLUFF_SIZE_LIMIT:
    raise SystemExit(".sqlfluff no longer sets large_file_skip_byte_limit = 0; adapt the size-skip samples")
NO_IMAGES = ("infra/services/images.json", '{"images": {}}\n')
HEAD = "FROM debian:12\nWORKDIR /app\n"
COVER = "RUN find /app -type d -exec chmod 0555 {} +\n"
AIRFLOW_HEADER = "# docker build --tag hai-airflow:3.4.0-python3.12 --build-context hai=services/hai services/orchestration\n"


@dataclass(frozen=True)
class Sample:
    name: str
    hook: str
    path: str
    bad: str
    good: str | bytes
    diagnostic: str
    executable: bool = False
    support: tuple[tuple[str, str], ...] = ()
    expected_line: int | None = None
    good_path: str | None = None
    good_diagnostic: str | None = None


def copy_mode(name: str, bad: str, good: str, *tracked: str, diagnostic: str = "cannot enter") -> Sample:
    """A Dockerfile sample for the COPY --chmod rule: both bodies follow a base image and a working folder; each tracked path is a source file."""
    return Sample(name, "hadolint", "Dockerfile", HEAD + bad, HEAD + good, diagnostic, support=tuple((path, "x = 1\n") for path in tracked))


SAMPLES = (
    *(
        Sample(f"ruff_{folder}", "ruff", f"{folder}/lint sample.py", "value: int = missing_name\n", "value: int = 1\n", "F821")
        for folder in ("dags", "orchestration", "services", "new_folder")
    ),
    Sample("ruff_hidden", "ruff", "services/.probe.py", "value: int = missing_name\n", "value: int = 1\n", "F821"),
    Sample("ruff_format", "ruff", "dags/format.py", "value = [1,2]\n", "value = [1, 2]\n", "File would be reformatted"),
    Sample("shell_syntax", "shell-syntax", "services/probe.sh", "#!/usr/bin/env bash\nif then\n", "#!/usr/bin/env bash\ntouch body_executed\n", "syntax error"),
    Sample(
        "shell_without_extension",
        "shell-syntax",
        "new_folder/syntax probe",
        "#!/usr/bin/env bash\nif then\n",
        "#!/usr/bin/env bash\ntouch body_executed\n",
        "syntax error",
        True,
    ),
    *(
        Sample(
            name,
            "shell-syntax",
            "new_folder/non_shell",
            "#!/usr/bin/env bash\nif then\n",
            content,
            "syntax error",
            good_diagnostic="Shell syntax: 0 nonignored shell sources",
        )
        for name, content in (
            ("shell_non_shell_split_utf8", b"a" * 511 + "é".encode()),
            ("shell_non_shell_binary", b"\xff\xfe"),
        )
    ),
    Sample(
        "shell_malformed_shebang_path",
        "shell-syntax",
        "new_folder/malformed",
        '#!/usr/bin/env "bash\n',
        "#!/usr/bin/env bash\ntouch body_executed\n",
        "new_folder/malformed:1: invalid shell shebang",
    ),
    Sample(
        "shellcheck",
        "shellcheck",
        "services/probe.sh",
        "#!/usr/bin/env bash\nvalue='has spaces'\nprintf '%s\\n' $value\n",
        "#!/usr/bin/env bash\nvalue='has spaces'\nprintf '%s\\n' \"$value\"\n",
        "SC2086",
    ),
    Sample(
        "shell_without_execute_bit",
        "shell-syntax",
        "new_folder/sourced probe",
        "#!/usr/bin/env bash\nif then\n",
        "#!/usr/bin/env bash\ntouch body_executed\n",
        "syntax error",
    ),
    Sample(
        "actionlint",
        "actionlint",
        ".github/workflows/probe.yml",
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: echo '${{ does_not_exist.value }}'\n",
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: echo '${{ github.sha }}'\n",
        "[expression]",
    ),
    Sample(
        "actionlint_local_action_metadata",
        "actionlint",
        ".github/actions/setup/action.yml",
        "name: Probe\ndescription: Probe action\nruns:\n  steps:\n    - run: echo ok\n      shell: bash\n",
        "name: Probe\ndescription: Probe action\nruns:\n  using: composite\n  steps:\n    - run: echo ok\n      shell: bash\n",
        "local action",
        support=(
            (
                ".github/workflows/local_action.yml",
                "on: push\njobs:\n  probe:\n    runs-on: ubuntu-24.04\n    steps:\n      - uses: ./.github/actions/setup\n",
            ),
        ),
    ),
    Sample("yaml_duplicates", "yamllint", "config/probe.yaml", "setting: true\nsetting: false\n", "setting: true\n", "key-duplicates"),
    Sample("yaml_profile_duplicates", "yamllint", ".yamllint", "extends: default\n" + YAML_PROFILE, YAML_PROFILE, "key-duplicates"),
    Sample("hadolint", "hadolint", "Dockerfile", "FROM debian:12\nRUN cd /tmp\n", "FROM debian:12\nWORKDIR /tmp\n", "DL3003"),
    Sample(
        "hadolint_pipefail",
        "hadolint",
        "Dockerfile",
        "FROM ubuntu:24.04\nRUN echo ok | cat\n",
        'FROM ubuntu:24.04\nSHELL ["/bin/bash", "-o", "pipefail", "-c"]\nRUN echo ok | cat\n',
        "DL4006",
    ),
    # The model API layout of Nov 15 2026: Docker 29 gave every folder that COPY --chmod=0444 created the mode 0444.
    copy_mode(
        "copy_mode_created_folder",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nUSER 10010\n",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\n" + COVER + "USER 10010\n",
    ),
    copy_mode(
        "copy_mode_nested_folders_after_a_missing_cover",
        "COPY --chmod=0444 a/__init__.py /app/a/\nCOPY --chmod=0444 b/c/main.py /app/b/c/main.py\nUSER 10010\n",
        "COPY --chmod=0444 a/__init__.py /app/a/\nCOPY --chmod=0444 b/c/main.py /app/b/c/main.py\n" + COVER + "USER 10010\n",
    ),
    copy_mode("copy_mode_enterable_mode", "COPY --chmod=0444 pkg/main.py /app/pkg/\nUSER 10010\n", "COPY --chmod=0555 pkg/main.py /app/pkg/\nUSER 10010\n"),
    copy_mode(
        "copy_mode_chmod_after_user",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nUSER 10010\n" + COVER,
        "COPY --chmod=0444 pkg/main.py /app/pkg/\n" + COVER + "USER 10010\n",
    ),
    copy_mode(
        "copy_mode_chmod_of_another_folder",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nRUN chmod -R 0755 /srv\nUSER 10010\n",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nRUN chmod -R 0755 /app\nUSER 10010\n",
    ),
    copy_mode(
        "copy_mode_chmod_without_search_bit",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nRUN find /app -type d -exec chmod 0600 {} +\nUSER 10010\n",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nRUN find /app -type d -exec chmod 0755 {} +\nUSER 10010\n",
    ),
    copy_mode(
        "copy_mode_folder_made_first",
        "COPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "RUN install -d -m 0755 /app/pkg\nCOPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "pkg/main.py",
    ),
    copy_mode(
        "copy_mode_folder_made_with_a_closed_mode",
        "RUN mkdir -m 0700 /app/pkg\nCOPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "RUN mkdir -m 0755 /app/pkg\nCOPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "pkg/main.py",
    ),
    copy_mode(
        "copy_mode_folder_made_by_an_earlier_copy",
        "COPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "COPY pkg/__init__.py /app/pkg/\nCOPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "pkg/main.py",
        "pkg/__init__.py",
    ),
    copy_mode(
        "copy_mode_relative_destination",
        "WORKDIR /app\nCOPY --chmod=0444 pkg/main.py pkg/main.py\nUSER 10010\n",
        "WORKDIR /app\nCOPY --chmod=0444 pkg/main.py pkg/main.py\nRUN chmod -R 0755 /app/pkg\nUSER 10010\n",
        "pkg/main.py",
    ),
    copy_mode(
        "copy_mode_cover_in_a_later_stage",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nUSER 10010\nFROM debian:12\n" + COVER,
        "COPY --chmod=0444 pkg/main.py /app/pkg/\n" + COVER + "USER 10010\n",
    ),
    # The five layouts that the round 1 review planted: each passed the first version of the check.
    copy_mode(
        "copy_mode_folder_source_without_a_trailing_slash",
        "COPY --chmod=0444 pkg /app/pkg\nUSER 10010\n",
        "COPY --chmod=0444 pkg /app/pkg\n" + COVER + "USER 10010\n",
        "pkg/main.py",
    ),
    copy_mode(
        "copy_mode_nested_folder_of_a_folder_source",
        "COPY --chmod=0444 pkg/ /app/\nUSER 10010\n",
        "COPY --chmod=0444 pkg/ /app/\n" + COVER + "USER 10010\n",
        "pkg/nested/main.py",
    ),
    copy_mode(
        "copy_mode_new_first_level_folder",
        "COPY --chmod=0444 pkg/main.py /closed/\nUSER 10010\n",
        "RUN install -d -m 0755 /closed\nCOPY --chmod=0444 pkg/main.py /closed/\nUSER 10010\n",
        "pkg/main.py",
    ),
    copy_mode(
        "copy_mode_find_with_another_test",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nRUN find /app -type d -name unrelated -exec chmod 0555 {} +\nUSER 10010\n",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\n" + COVER + "USER 10010\n",
        "pkg/main.py",
    ),
    copy_mode(
        "copy_mode_attached_mkdir_mode",
        "RUN mkdir -m0700 /app/pkg\nCOPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "RUN mkdir -m0755 /app/pkg\nCOPY --chmod=0444 pkg/main.py /app/pkg/main.py\nUSER 10010\n",
        "pkg/main.py",
    ),
    # Valid layouts that the first version refused or flagged: a variable path with no mode, a variable that ENV sets and an image that runs as root.
    copy_mode(
        "copy_mode_variable_destination_without_a_mode",
        "COPY --chmod=0444 pkg/main.py ${APP}/pkg/main.py\nUSER 10010\n",
        "ENV APP=/app\nCOPY pkg/main.py ${APP}/pkg/main.py\nUSER 10010\n",
        "pkg/main.py",
        diagnostic="cannot resolve",
    ),
    copy_mode(
        "copy_mode_variable_set_by_env",
        "ENV APP=/app\nCOPY --chmod=0444 pkg/main.py ${APP}/pkg/\nUSER 10010\n",
        "ENV APP=/app\nCOPY --chmod=0444 pkg/main.py ${APP}/pkg/\n" + COVER + "USER 10010\n",
        "pkg/main.py",
    ),
    copy_mode(
        "copy_mode_image_that_runs_as_root",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nUSER 10010\n",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\n",
        "pkg/main.py",
    ),
    Sample(
        "copy_mode_from_stage",
        "hadolint",
        "Dockerfile",
        "FROM debian:12 AS base\nFROM debian:12\nWORKDIR /app\nCOPY --from=base --chmod=0444 /etc/hostname /app/pkg/hostname\nUSER 10010\n",
        "FROM debian:12 AS base\nFROM debian:12\nWORKDIR /app\nCOPY --from=base --chmod=0555 /etc/hostname /app/pkg/hostname\nUSER 10010\n",
        "cannot enter",
    ),
    copy_mode(
        "copy_mode_variable_destination",
        "COPY --chmod=0444 pkg/main.py $APP/pkg/\nUSER 10010\n",
        "COPY --chmod=0444 pkg/main.py /app/pkg/\nRUN chmod -R 0755 /app\nUSER 10010\n",
        diagnostic="cannot resolve",
    ),
    Sample(
        "build_stage_reference",
        "build-contexts",
        "services/probe/Dockerfile",
        "FROM debian:12 AS earlier\nFROM debian:12\nCOPY --from=typo /one /two\n",
        "FROM debian:12 AS earlier\nFROM debian:12\nCOPY --from=earlier /one /two\n",
        "unknown COPY --from=typo",
        support=(("docker-compose.yaml", "services:\n  probe:\n    build: ./services/probe\n"), NO_IMAGES),
    ),
    Sample(
        "build_context_reference",
        "build-contexts",
        "docker-compose.yaml",
        (
            "services:\n  probe:\n    build:\n      context: ./services/probe\n"
            "  other:\n    build:\n      context: ./services/other\n      additional_contexts:\n        hai: ./services/hai\n"
        ),
        "services:\n  probe:\n    build:\n      context: ./services/probe\n      additional_contexts:\n        hai: ./services/hai\n",
        "unknown COPY --from=hai",
        support=(("services/probe/Dockerfile", "FROM debian:12\nCOPY --from=hai /one /two\n"), NO_IMAGES),
    ),
    Sample(
        "build_header_context",
        "build-contexts",
        "services/orchestration/Dockerfile",
        "# docker build --tag hai-airflow:3.4.0-python3.12 services/orchestration\nFROM debian:12\nCOPY --from=hai /one /two\n",
        AIRFLOW_HEADER + "FROM debian:12\nCOPY --from=hai /one /two\n",
        "header build command declares no contexts",
        support=(("docker-compose.yaml", "services: {}\n"), NO_IMAGES),
    ),
    Sample(
        "build_header_missing",
        "build-contexts",
        "services/orchestration/Dockerfile",
        "FROM debian:12\nCOPY --from=hai /one /two\n",
        AIRFLOW_HEADER + "FROM debian:12\nCOPY --from=hai /one /two\n",
        "missing header build command",
        support=(("docker-compose.yaml", "services: {}\n"), NO_IMAGES),
    ),
    Sample(
        "build_image_contexts",
        "build-contexts",
        "infra/services/images.json",
        '{"images": {"probe": {"kind": "build", "context": "services/probe"}}}\n',
        '{"images": {"probe": {"kind": "build", "context": "services/probe", "build_contexts": ["hai"]}}}\n',
        "unknown COPY --from=hai",
        support=(("docker-compose.yaml", "services: {}\n"), ("services/probe/Dockerfile", "FROM debian:12\nCOPY --from=hai /one /two\n")),
    ),
    Sample("gofmt", "gofmt", "services/probe/main.go", "package main\nfunc main() {}\n", "package main\n\nfunc main() {}\n", "gofmt formatting differs"),
    Sample(
        "go_vet",
        "go-vet",
        "services/probe/main.go",
        'package main\n\nimport "fmt"\n\nfunc main() { fmt.Printf("%d", "text") }\n',
        'package main\n\nimport "fmt"\n\nfunc main() { fmt.Printf("%s", "text") }\n',
        "format %d has arg",
    ),
    Sample(
        "go_vet_standalone_programs",
        "go-vet",
        "services/probe/first.go",
        'package main\n\nimport "fmt"\n\nfunc main() { fmt.Printf("%d", "text") }\n',
        'package main\n\nimport "fmt"\n\nfunc main() { fmt.Printf("%s", "text") }\n',
        "format %d has arg",
        support=(("services/probe/second.go", "package main\n\nfunc main() {}\n"),),
    ),
    Sample(
        "sql_postgres",
        "sqlfluff",
        "services/lakehouse/polaris/create-polaris-schema.sql",
        "CREATE SCHEMA IF NOT EXISTS polaris_schema;\n",
        "create schema if not exists polaris_schema;\n",
        "CP01",
    ),
    Sample("sql_duckdb", "sqlfluff", "services/analytics/resources.sql", "SELECT 1 AS id\n", "select 1 as id\n", "CP01"),
    Sample(
        "sql_duckdb_cli_masking",
        "sqlfluff",
        "services/analytics/resources.sql",
        ".output /dev/null\nSELECT 1 AS id;\n",
        ".output /dev/null\nselect 1 as id;\n",
        "CP01",
        expected_line=2,
    ),
    *(
        Sample(
            name,
            "sqlfluff",
            path,
            small + "-- " + "x" * 30000 + "\n",
            small,
            "SQLFluff skipped this file for size",
            support=((".sqlfluff", SQLFLUFF_SIZE_LIMIT),),
        )
        for name, path, small in (
            ("sql_size_skip_model", "dbt/models/probe.sql", "select 1 as id\n"),
            ("sql_size_skip_postgres", "services/lakehouse/polaris/create-polaris-schema.sql", "create schema if not exists polaris_schema;\n"),
            ("sql_size_skip_masked_cli", "services/analytics/resources.sql", ".output /dev/null\nselect 1 as id;\n"),
        )
    ),
    Sample(
        "sql_bronze_exclusion",
        "sqlfluff",
        "services/analytics/unlisted.sql",
        "LOAD iceberg;\nCREATE SECRET (TYPE s3, PROVIDER credential_chain);\nATTACH 'probe' AS bronze (TYPE iceberg);\n",
        "LOAD iceberg;\nCREATE SECRET (TYPE s3, PROVIDER credential_chain);\nATTACH 'probe' AS bronze (TYPE iceberg);\n",
        "SQL has no declared runtime and dialect",
        good_path="services/analytics/bronze.sql",
        good_diagnostic=(
            "excluded services/analytics/bronze.sql: DuckDB CLI init script whose LOAD, CREATE SECRET and ATTACH statements are outside the pinned grammar"
        ),
    ),
    Sample(
        "sql_unlisted_feature",
        "sqlfluff",
        "features/unlisted.sql",
        "select 1 as id\n",
        "select 1 as id\n",
        "SQL has no declared runtime and dialect",
        good_path="dbt/macros/features/scd2.sql",
    ),
    Sample(
        "sql_unclassified_path",
        "sqlfluff",
        "unclassified/probe.sql",
        "select 1 as id\n",
        "select 1 as id\n",
        "SQL has no declared runtime and dialect",
        good_path="services/analytics/resources.sql",
    ),
    Sample(
        "sql_unused_macro",
        "sqlfluff",
        "dbt/models/probe.sql",
        "select 1 as id\n",
        "{{ one() }}\n",
        "unused macro one",
        support=(("dbt/macros/probe.sql", "{% macro one() %}\nselect 1 as id\n{% endmacro %}\n"),),
    ),
    Sample(
        "sql_hook_only_macros",
        "sqlfluff",
        "dbt/dbt_project.yml",
        PROJECT,
        PROJECT + "on-run-start:\n  - '{{ one() }}'\non-run-end:\n  - '{{ two() }}'\n",
        "unused macro one",
        support=(
            ("dbt/macros/probe.sql", "{% macro one() %}\nselect 1\n{% endmacro %}\n{% macro two() %}\nselect 2\n{% endmacro %}\n"),
            ("dbt/models/probe.sql", "select 1 as id\n"),
        ),
    ),
    Sample(
        "sql_yaml_generic_test_macro",
        "sqlfluff",
        "dbt/models/probe.yml",
        "version: 2\nmodels:\n  - name: probe\n",
        "version: 2\nmodels:\n  - name: probe\n    data_tests: [one]\n",
        "unused macro test_one",
        support=(
            ("dbt/macros/probe.sql", "{% test one(model) %}\nselect * from {{ model }} where id is null\n{% endtest %}\n"),
            ("dbt/models/probe.sql", "select 1 as id\n"),
        ),
    ),
    Sample(
        "sql_macro",
        "sqlfluff",
        "dbt/macros/probe.sql",
        "{% macro one() %}\nselect 1\n",
        "{% macro one() %}\nselect 1\n{% endmacro %}\n",
        "Reached EOF without finding a close tag for macro",
        support=(("dbt/models/probe.sql", "{{ one() }}\n"),),
    ),
)


def hashes() -> dict[str, str]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SNAPSHOT}


def git(repo: Path, *arguments: str) -> None:
    run_command("git", list(arguments), cwd=repo, timeout=60, check=True)


def fixture(repo: Path, mutant: bool) -> None:
    repo.mkdir()
    git(repo, "init", "--quiet")
    for name in COPIED:
        target = repo / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    if mutant:
        path = repo / "scripts/quality/lint_sources.py"
        text = path.read_text().replace(
            "def lint(check: str) -> int:\n",
            'def lint(check: str) -> int:\n    sys.stdout.write("MUTANT disabled " + check + "\\n")\n    return 0\n',
        )
        path.write_text(text)
    for name in (".venv", ".tools"):
        (repo / name).symlink_to(ROOT / name)
    (repo / ".gitignore").write_text(".venv\n.tools\n__pycache__/\nignored.py\n")
    (repo / "ignored.py").write_text("invalid ignored Python source\n")
    (repo / "dbt").mkdir()
    (repo / "dbt/dbt_project.yml").write_text(PROJECT)
    (repo / "dbt/profiles.yml").write_text(PROFILE)


def sample_case(sample: Sample, out: Path, mutant: bool) -> dict[str, object]:
    observations = []
    with tempfile.TemporaryDirectory(prefix="hai_lint_case_") as temporary:
        repo = Path(temporary) / "repo"
        fixture(repo, mutant)
        for name, support_content in sample.support:
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(support_content)
            git(repo, "add", "--", name)
        for variant, content in (("bad", sample.bad), ("good", sample.good)):
            name = sample.good_path if variant == "good" and sample.good_path else sample.path
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if name != sample.path:
                git(repo, "mv", "--", sample.path, name)
            payload = content.encode() if isinstance(content, str) else content
            path.write_bytes(payload)
            if sample.executable:
                path.chmod(0o700)
            git(repo, "add", "--", name)
            result = run_command(
                str(ROOT / ".venv/bin/pre-commit"),
                ["run", sample.hook, "--hook-stage", hook_stage(repo / ".pre-commit-config.yaml", sample.hook), "--all-files", "--verbose"],
                cwd=repo,
                env={**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools/lint_caches/pre_commit")},
                timeout=360,
            )
            output = result.stdout + result.stderr
            log = out / f"{sample.name}_{variant}.log"
            log.write_text(output)
            passed = result.returncode == 0 if variant == "good" else result.returncode != 0 and sample.diagnostic in output
            if variant == "good" and sample.good_diagnostic is not None:
                passed &= sample.good_diagnostic in output
            if variant == "bad" and sample.expected_line is not None:
                passed &= re.search(rf"L:\s*{sample.expected_line}\b", output) is not None
            unchanged = path.read_bytes() == payload
            observations.append(
                {
                    "variant": variant,
                    "path": name,
                    "exit": result.returncode,
                    "passed": passed and unchanged,
                    "source_unchanged": unchanged,
                    "expected_line": sample.expected_line,
                    "good_diagnostic": sample.good_diagnostic,
                    "log": log.name,
                    "diagnostic": sample.diagnostic,
                }
            )
        body_not_executed = not (repo / "body_executed").exists()
    return {
        "case": sample.name,
        "hook": sample.hook,
        "passed": all(row["passed"] for row in observations) and body_not_executed,
        "body_not_executed": body_not_executed,
        "observations": observations,
    }


def main() -> int:
    """Run real hook cases and retain every expected rejection, pass and cleanup result."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mutant", action="store_true", help="prove the cases reject a fixture dispatch that does no checking")
    args = parser.parse_args()
    clear_git_environment()
    before = hashes()
    out = ROOT / "data/e2e/lint_coverage" / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    out.mkdir(parents=True)
    results = [sample_case(sample, out, args.mutant) for sample in SAMPLES]
    unchanged = before == hashes()
    passed = sum(bool(row["passed"]) for row in results)
    report = {
        "started_utc": out.name,
        "mode": "full",
        "mutant": args.mutant,
        "sha256": before,
        "code_unchanged": unchanged,
        "passed": passed,
        "total": len(results),
        "cases": results,
        "cleanup": "done",
        "limits": "Synthetic sources and static tool checks only. No SQL, script body, Docker command or container ran.",
    }
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    for result in results:
        sys.stdout.write(f"{'PASS' if result['passed'] else 'FAIL'} {result['case']}\n")
    sys.stdout.write(f"{passed}/{len(results)} passed. Report: {out / 'report.json'}\n")
    return int(passed != len(results) or not unchanged)


if __name__ == "__main__":
    raise SystemExit(main())

---
title: Quality checks
description: Every repository check the pre-commit hooks and CI run, what each blocks and how to run it on its own.
last_updated: 2026-10-15
---

# Quality checks

- **Secrets and credential files:** gitleaks scans the staged index; `.env`
  (but not `.env.example`), Terraform state and saved plans, keys and cloud
  credential files are blocked.
- **Data files:** `.csv`, `.tsv`, `.parquet`, `.xlsx`, `.xls`, `.jsonl`,
  `.ndjson`, `.avro`, `.db` and `.sqlite` files are allowed only in
  `tests/fixtures/` for synthetic samples and in `dbt/seeds/` for the reviewed
  dbt seeds (file labels and checksums from the S3 manifests, no source values). Any file over 5 MB is blocked
  unless Git LFS stores it. Acquired data stays in S3 and ignored local folders.
  Git LFS stores two acquisition config files over the limit,
  `config/acquisition/source_registry.json` and
  `config/acquisition/planning_v1/acquisition_plans_v1.json` (listed in
  `.gitattributes`); a clone needs `git lfs install` to fetch them.
- **Privacy scan:** every tracked or untracked non-ignored file, not only the
  diff, is scanned for home-directory and machine temporary paths, email
  addresses outside reserved domains (`example.invalid`, `.test`), phone
  numbers, AWS account IDs (including those in ARNs) and values of identifying keys in `.env`.
  Findings give file, line and type, never the value. Files that cannot be read
  as text are listed as unreviewed; inspect them before publishing. Intentional
  values go in `.privacy_allowlist` as `<type> <path glob> -- <reason>`,
  matched by type and path only; an entry without a reason is itself a finding.
  The synthetic account IDs in the Terraform, infrastructure and configuration
  tests are listed there. The hook runs
  `.venv/bin/python -m scripts.quality.repo_checks privacy-scan --all`, where
  `--all` means the same scope: hidden files are scanned when Git does not
  ignore them. Ignored files (such as the local `data/` folder) are never
  scanned, because they cannot reach GitHub (`--warn` reports without failing).
- **Writing check:** the prose of every tracked or untracked non-ignored
  Markdown file (front matter, code spans and blocks, URLs and `tests/fixtures/`
  excluded) is checked for a comma before a final "and", "or" or "nor" and for
  ISO dates in prose, which are written as `Sep 30 2026`. It also blocks the time-bound
  words `currently`, `recently`, `soon`, `eventually`, `as of this writing`,
  `at present`, `in the future` and `for now`. Files declared with
  `--articles <glob>` are also checked for em dashes. Exceptions go in
  `.writing_allowlist` as `<type> <path glob> -- <reason>`; an entry without a
  reason is itself a finding. It blocks: the existing prose was corrected before
  each rule was enabled. Run it with
  `.venv/bin/python -m scripts.quality.repo_checks writing-check` (`--warn`
  reports without failing).

  Three more kinds report in all prose: em and en dashes (`dash`), semicolons
  (`semicolon`) and slashes between words such as `and/or` (`slash`). They warn
  by default: each finding prints with a `WARN` prefix and the check exits
  with code 0. `--block <kind>` makes one kind block (`--block dash --block
  slash`) once a whole-repository run shows no finding of that kind. Any other
  kind name is a usage error. The `.writing_allowlist` entry type is the kind
  name, so `semicolon docs/*.md -- <reason>` keeps the semicolons of those
  files. In a declared article an em dash is one `em dash in article text`
  finding, not also a `dash` warning. The hook and the local CI command pass
  `--block dash --block slash`, so dashes and slashes block there and semicolons warn. A whole-repository run without
  `--block` lists them, one `WARN` line each.
- **Markdown syntax:** `markdownlint-cli2` checks every staged Markdown file
  outside `data/` against the Conventional Docs baseline in
  `.markdownlint-cli2.jsonc` (dash bullets, ATX headings, sequential numbered
  lists, a language on every code fence). It blocks: a whole-project run was
  clean when it was added.
- **Front matter:** every Markdown document outside `data/`, except READMEs,
  well-known files and `.github/` templates, starts with YAML front matter that
  parses and holds `title` (equal to the H1), a `description` of at most 120
  characters and `last_updated` as `YYYY-MM-DD`. It uses PyYAML, pinned in
  `requirements.txt`. It blocks: a whole-project run was clean when it was added.
- **Commit messages:** Conventional Commits and no AI attribution, checked in
  the full message by the `commit-msg` hook and, in GitHub Actions, for every
  pushed commit. AI credit and agent-session lines are rejected; human
  co-authors and factual tool mentions are allowed. Detection uses an explicit
  agent and vendor list, so new forms need a failing sample before the check is
  extended. Review pull request text separately; a Git hook cannot read it.
- **Docs match the code:** every environment variable the code reads is listed
  in `.env.example` and every variable listed there is read by code, a script,
  a workflow or a template. Local commits require an ignored `.documentation_review.json`, a per-document
  review outcome bound to the complete staged source and configuration snapshot. Before the first release tag,
  the document inventory is `README.md` only. Other documents, including explicit extra formats, do not require
  per-commit review during development. A version tag such as `v1.0.0` restores the full inventory and requires
  its review. Other tags do not change the inventory. Draft the record with
  `.venv/bin/python -m scripts.quality.documentation_review prepare --reviewer <name> --reviewed-at <UTC>`
  after staging, then fill each outcome and note locally. Read the README in full during development.
  Once released, read in full every changed or new document and every unchanged one that the staged change affects.
  After a staged change, `prepare --refresh --carry-forward` keeps the
  outcome of each document whose staged content is unchanged and whose earlier entry is valid, with the earlier
  review time in its note; set it back to `pending` if the change affects it. Never stage
  this record. The local hook validates its freshness against the index and rejects
  tracked or unignored evidence. GitHub Actions checks that the record is untracked and ignored and runs
  synthetic hook tests; it cannot verify the private local review itself.
- **Removed names:** a name the staged change removes is no longer referenced:
  a deleted script, a command-line flag, an environment variable, a top-level
  Python function or class, a config key with `_`, `-` or camelCase, a Terraform
  variable, output, module, resource or data source, a deleted dbt model or a
  dependency that leaves every requirements file. Code names are searched in
  code and config and in the code spans and fences of Markdown, not in prose;
  `CHANGELOG.md` is history and is not searched. GitHub Actions runs the same
  check over every pushed range. A reference that must stay goes in
  `.cleanup_allowlist` as `<kind> <path glob> -- <reason>`; an entry without a
  reason is itself a finding. It blocks: a replay over every earlier commit
  found only deliberate references (a removed config key that the bronze loader
  refuses by name).
- **Unused code, dependencies and files:** vulture 2.16 (60%
  confidence) reports functions, classes and variables nothing uses; deptry
  0.25.1 reports requirements nothing imports and imports no requirements file
  declares; `orphan-files` reports tracked files no other file names by path,
  file name, folder or import (including `python -m`); tflint (`terraform_unused_declarations`, each
  folder that holds a tracked `.tf` file) reports unused Terraform declarations. vulture and deptry are pinned
  in `requirements.txt`. They block. deptry checks each runtime on its
  own code and requirements, as `config/quality/dependency_runtimes.json` maps
  them: the host (every file no other runtime claims), the review tools, the
  Spark jobs, the analytics image and the SQLFluff tools (no project code). A
  package a runtime gets outside pip, such as PySpark, is listed there with its
  source. A hash-locked requirements file also pins transitive packages, so only
  its missing imports are reported; a file two runtimes share reports a package
  as unused only when neither imports it. Exceptions go in `.cleanup_allowlist` with a
  reason, under the kinds `unused-code`, `unused-dependencies`, `orphan` and
  `terraform-unused`; dbt's folder-loaded models and macros are listed there.
  A name used in a way vulture cannot see (a parser callback, a pytest autouse
  fixture, a field written out through `dataclasses.asdict`) goes in
  `scripts/quality/vulture_whitelist.py`, one name per entry with its reason, so
  the rest of its file is still checked. The whitelist also keeps one dead WONDER
  constant until the next WONDER code version, because its file is in the
  approved code hashes of 9 collectors. Run one check with
  `.venv/bin/python -m scripts.quality.repo_checks unused-code` (`--warn`
  reports without failing).
- **Lint settings:** Ruff keeps rule sets `E`, `F`, `I`, `B`, `UP`, `S`, `SIM`
  and `T20` at line length 160 with no ignore or exclude settings; MyPy keeps
  `--check-untyped-defs` and `--disallow-untyped-defs`. The settings check covers every nonignored MyPy TOML and
  INI profile, module overrides and literal command flags. A command cannot select a generated or dynamic profile.
  `scripts/quality/typecheck.py` checks every host-compatible Python source. Dependency-bound leaves and their required
  types are listed in `config/quality/mypy_runtimes.json`. This is the only hand-written exclusion list. The selector
  derives their transitive local importers, including package initialization. Calls such as `importlib.import_module`
  are not imports for this purpose, because MyPy does not follow them. It reports
  each as pending with the cause "imports a pending module". The leaves and their importers remain pending until their
  dependencies and types are available. Every host run checks that the leaf set equals the files with import diagnostics
  in an unrestricted MyPy run. An extra leaf, a missing leaf or any other type diagnostic fails the check.
  The acquisition and orchestration host scopes use this same inventory without import suppressions.
  Run a declared runtime with
  `.venv/bin/python -m scripts.quality.typecheck --runtime <name> --check-untyped-defs --disallow-untyped-defs`
  in its compatible environment. The only allowed
  suppression is Ruff `S603` in `scripts/process.py`, the one process launcher;
  other scripts start programs through it and run as modules
  (`.venv/bin/python -m scripts.quality.scan_secrets`). Tests use
  `tests/support.py` `check()` instead of `assert`.

The strict command above prints the selected file count and every pending file with its cause for the chosen runtime.
The check E2E runs the host, acquisition and orchestration scopes with separate empty MyPy caches and verifies synthetic
import chains and manifest mismatches.

**Acquisition checks** (`bash scripts/acquisition/run_checks.sh`) run Ruff, MyPy,
registry validation, the acquisition regression tests and every offline E2E
suite twice, with a 90% coverage gate that also counts collector command lines
run as child processes. They take 12 to 14 minutes (up to 29 in CI), so they run only
when acquisition code, configuration or their dependencies change: through the
`acquisition-checks` pre-commit hook and in a separate GitHub Actions "Acquisition
Checks" job (30-minute limit, LFS checkout, pinned Terraform). The CI job uses the
hook's own path pattern through `scripts/acquisition/run_checks_ci.sh`, which also
matches the CI workflow; a manual run, the first push of a branch or rewritten
history runs every check. `bash scripts/acquisition/run_checks_ci_e2e.sh` tests
that gate. CI runs on every push to any branch and on every pull request; each push gets its own run, so a newer
push never cancels the check of an earlier range. They need no AWS credentials and no
collected `data/`. Off macOS, the HUD workbook and WONDER suites substitute only the
macOS browser-download metadata (the "downloaded from" attribute and file creation
time); on a Mac the real reads run. When the private archive is absent (as in CI), the
registry-additions suite skips its single legacy-archive check and the code-version suite its archived-original
checks; both record the skips. Each gate run writes `run.json` beside the suite outcomes in `data/e2e/acquisition_checks/`
with the seconds of every suite and a hash of the acquisition code and configuration; a change during the run fails it.
`ACQUISITION_E2E_MODE=iteration` runs each offline suite once instead of twice for a faster loop and marks the run
"not commit evidence"; the hook and CI always run the full mode. The coverage gate leaves out
the tools in `scripts/acquisition/one_off/`, which ran once and are evidenced by their run records; a
gate test loads every committed collector plan through its contract; it needs the collected `data/`, so CI
skips it.

Each check has a bad and a good sample run through the real `pre-commit` entry
point (`scripts/quality/run_checks_e2e.py` and
`scripts/quality/run_documentation_review_e2e.py`); both run in local CI.
Every detection check was run on the whole repository before it was enabled,
with no false positives, so they block from the start. The writing check blocked
only after the existing prose was corrected. Its dash and slash kinds block since the prose was corrected on
Oct 15 2026. Its semicolon kind warns until a whole-repository run has no semicolon finding. The unused code, dependency and file
checks blocked only after their findings were deleted or listed with a reason.

## How the checks run

CI checks Ruff formatting and security rules, MyPy, outgoing-source secret
scanning, repository checks, configuration and quality E2E, retained regression
safeguards, Terraform mock tests, TFLint, Checkov and `trivy config` (its
embedded checks only, run offline). Pre-commit scans the actual staged index,
including changes hidden by a clean working copy, before local CI. The source
scan excludes ignored acquisition data; neither scan sends files to an external
service. Secret values and matching source fragments are omitted from scanner
diagnostics.

Pre-commit (`pre-commit` and `commit-msg` stages) and `scripts/run_ci.sh`
enforce these repository rules at the pinned versions. On a commit the
`local_ci_affected` hook runs `scripts/run_ci.sh` for the staged paths. The full
set (`.venv/bin/pre-commit run --all-files --hook-stage manual`, whose
`local_ci` hook runs the full local CI) runs before every release or push.
GitHub CI runs the manual stage on every push and pull request as parallel
jobs: the hooks without `local_ci` (file-based hooks check the changed files of
the range), then `scripts/run_ci.sh` as `CI_PART=core` and, when a change
touches a path the launcher reads, the launcher E2E in two shards
(`CI_PART=launcher`, `LAUNCHER_SHARD=K/2`), each for the groups the range
touches (`CI_SCOPE=range`). Without a usable range (a manual run, the first push
of a branch or rewritten history) every check runs. Each job caches only what a
lock file or hash checks again before use (npm packages, Terraform providers,
the dbt package archive and the SQLFluff packages); the Python environment is
built fresh from `requirements.txt` without a pip cache. Its timeout is about
twice its measured time. Locally `scripts/run_ci.sh` runs every part: the gates
first, then the launcher E2E and then the independent check groups, which run
concurrently. It prints each step's seconds (GitHub CI adds them to the job
summary). In the full set gitleaks scans every outgoing source file instead of
the staged index. The acquisition checks
run in their own CI job and through their hook only when acquisition files
change. The full local CI also runs them. Run them directly before a release
with `bash scripts/acquisition/run_checks.sh`.

The checks cover secrets and credential files, data files (allowed only in `tests/fixtures/` and `dbt/seeds/`), a
privacy scan of every non-ignored file, prose style, Markdown syntax, front matter, commit messages, documentation that
matches the code, removed names, unused code, dependencies and files, lint settings and Docker calls outside the
launcher. Each has a failing and a passing sample run in local CI. Local commits need an ignored
`.documentation_review.json`, a review of `README.md` before the first release tag and the full document inventory
from that tag onward, bound to the complete staged snapshot
(`.venv/bin/python -m scripts.quality.documentation_review prepare`). There are no bypasses:
never use `SKIP=` or `git commit --no-verify`. If a check blocks a valid change,
fix the check or ask the repository owner.

`CI_SCOPE` selects the check groups: `full` (the default: every group, the acquisition gate included), `staged` (the
commit hook) or `range` with `RANGE_START` and `RANGE_END` (GitHub CI). `CI_LIST_ONLY=1` prints which groups would run
and why. It runs nothing. The groups and the paths they depend on are in `config/quality/ci_groups.json`. A path that
no group lists and that is not passive selects every applicable group. Markdown outside `dbt/`, anything under `docs/`,
`.gitignore` and `.gitattributes` are passive: they do not select every group, but a group whose list matches one still
runs. A change of passive paths that no group lists runs no group. The privacy, secret, credential, data-file,
writing, Markdown lint and documentation gates always run. They run in every scope and every part, also when no group
is selected. The Markdown lint gate checks every Markdown document the markdownlint hook checks, so a change to any file
markdownlint reads (its configuration in any folder, a file a configuration extends or the pin) cannot skip it. The
acquisition gate is the exception: it runs only in the full scope and its own hook and CI job cover commits and pushes.
A change to `.sqlfluff` or the SQLFluff pins lints every dbt model and test (the `sql_style` group).

Each CI run retains configuration evidence under `data/e2e/configuration/` and
quality evidence under `data/e2e/quality/`. E2E reports include source hashes,
expected and observed process outcomes, repeat commands and tested limits.
GitHub Actions retains these synthetic artifacts for 14 days. Local TFLint
needs permission to start its bundled plugin; a sandbox denial is a blocked
check, not a passing result.

Before every commit, stage the intended files by name, then draft, complete and
retain the ignored local documentation review record (see Docs match the code above).

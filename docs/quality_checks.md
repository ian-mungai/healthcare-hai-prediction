---
title: Quality checks
description: Every repository check the pre-commit hooks and CI run, what each blocks and how to run it on its own.
last_updated: 2026-10-29
---

# Quality checks

This document lists every repository check that the pre-commit hooks and CI run, what each check blocks and how to run it on its own. It is for developers who change code, configuration or documentation in this repository. The first part lists the checks that run in the commit hook and in CI.

- **Python lint and formatting:** Ruff checks every nonignored first-party Python source from the Git inventory,
  including `dags/`, `orchestration/` and service code. Both lint and formatting use the existing `pyproject.toml`
  settings. The `ruff` hook and the CI source gate use `.venv/bin/python -m scripts.quality.lint_sources ruff`.
  Paths remain separate arguments, including hidden files and names with spaces. Source symlinks are rejected.
- **Shell syntax:** the `shell-syntax` hook checks every nonignored shell source with its declared interpreter's
  syntax-only mode. Shell suffixes and declared shell scripts without a suffix are inventoried. Files without a shell suffix
  are skipped when their raw bytes do not start with `#!`, including binary files and UTF-8 split at the read boundary.
  Only the shebang line is decoded, replacing invalid UTF-8. A malformed shebang diagnostic names the path. Script bodies never run.
- **Language linters:** ShellCheck 0.11.0 checks tracked shell sources. actionlint 1.7.12 checks Actions workflows.
  It validates local `action.yml` metadata only through the workflow's `uses:` reference. Shell scripts and expressions
  inside composite steps are not linted. yamllint 1.38.0 checks tracked YAML, including `action.yml` and `.yamllint` itself and treats duplicate keys
  as errors. Hadolint 2.15.1 checks every tracked Dockerfile. gofmt and go vet use installed Go 1.27.3, with vet targeting
  Linux amd64 and arm64 for the Linux-only programs. Each tracked Go file is vetted separately, so standalone programs
  can share a folder. Files that form one multi-file package are not supported. A vet error in any file fails the gate.
  No Go toolchain or module is downloaded. The native tool archives
  and raw binaries are checksum-pinned in `config/quality_tools.json`. `scripts/quality/install_tools.py --tool <NAME>`
  selects one native tool without reinstalling the others. yamllint is pinned in `requirements.txt`.
  `.venv/bin/python -m scripts.quality.lint_sources <CHECK>` runs each tool through the same entry point as its hook.
  `<NAME>` is `shellcheck`, `actionlint` or `hadolint` for these native tools. `<CHECK>` is the hook ID shown above,
  with `gofmt` and `go-vet` for Go. Unsupported input, missing tools and rule violations fail visibly.
- **Build contexts:** the `build-contexts` hook checks every Dockerfile `COPY --from=` reference against an earlier
  stage or the building service's `docker-compose.yaml` `additional_contexts`. Airflow is built separately, so
  `services/orchestration/Dockerfile` uses the `--build-context` declarations in its header's build command instead.
  A missing Airflow header command or one with no contexts fails. No build command executes. The check supports the
  project's Dockerfile layout and fails visibly on unsupported escape directives or heredocs.
- **Container imports:** the `container-mounts` hook checks first-party Python imports against each container's declared file locations.
  The same command runs in `repository_code_checks`: `.venv/bin/python -m scripts.quality.container_mounts`.
  See [Container imports](#container-imports) for source declarations, supported imports, samples and limits.
- **Hadolint exceptions:** DL3022 is ignored only at the five `COPY --from=hai` instructions. The build-context check
  verifies their named context independently. DL3008 is ignored only for Spark's apt-install instruction: packages
  follow the Ubuntu archive on the build day. Dated snapshot pins are a lead-managed issue-register action for the next
  image build. Spark's final stage uses Bash with pipefail, keeping all three checksum pipelines. Its changed runtime
  behavior remains unverified until that image build. No Hadolint rule is ignored globally.
- **SQL style:** SQLFluff uses the pinned environment under `.tools/sqlfluff`. Active `dbt/models/`, `dbt/tests/`
  and `dbt/macros/` use DuckDB with the dbt templater. `services/analytics/resources.sql` uses raw DuckDB.
  `services/lakehouse/polaris/create-polaris-schema.sql` uses raw PostgreSQL. All profiles use `.sqlfluff`.
  `services/analytics/bronze.sql` is listed as excluded from SQLFluff: it is a DuckDB CLI
  init script whose LOAD, CREATE SECRET and ATTACH statements are outside the pinned grammar. Its source stays unchanged.
  The 6 SQL files of `tests/orchestration/fixtures/tiny` are listed as excluded too: they form the synthetic dbt project of
  the transform scheduler E2E, which builds and checks them.
  The feature-phase SQL in `dbt/models/features/`, `dbt/macros/features/` and `dbt/tests/features/` is linted with the other dbt SQL
  (see `docs/feature_phase_saved.md`).
  A tracked SQL file outside the active paths and the explicit excluded list fails.
  `.venv/bin/python -m scripts.quality.lint_sources sql` runs these profiles without executing SQL. dbt packages are
  a compilation prerequisite. Macro bodies are style-checked only through the models and tests that use them.
  A fresh local dbt parse supplies the macro call graph. Each project macro must have a caller recorded in the manifest:
  a model, singular or YAML generic test, project on-run-start or on-run-end hook or another macro. A recorded call does not
  prove that the caller executes. Hook-only and YAML-only macro bodies are not directly style-checked by this command.
  Unused macros and unreadable or inconsistent inventories fail. Invalid Jinja fails compilation.
  SQLFluff skips a file over its byte limit (20,000 by default) with only a warning. `.sqlfluff` sets
  `large_file_skip_byte_limit = 0`, so no file is skipped; the generated feature matrix models exceed the default. The SQL
  lint still fails when a skip happens to any actively linted model, test or service SQL file. The lint coverage
  samples `sql_size_skip_*` prove it with their own 20,000-byte limit. A skipped macro file stays covered through its callers.
  No wrapper model or separate project is created.
  DuckDB dot-command lines in `resources.sql` are replaced with comments in temporary lint copies, preserving line numbers and source
  bytes. Only the two `getenv` SET lines in `services/analytics/resources.sql` ignore PRS because the pinned parser
  cannot parse that valid DuckDB syntax. A style error elsewhere still fails. Other parse errors remain failures.
- **Secrets and credential files:** gitleaks scans the staged index. `.env`
  (but not `.env.example`), Terraform state and saved plans, keys and cloud
  credential files are blocked.
- **Data files:** `.csv`, `.tsv`, `.parquet`, `.xlsx`, `.xls`, `.jsonl`,
  `.ndjson`, `.avro`, `.db` and `.sqlite` files are allowed only in
  `tests/fixtures/` for synthetic samples, in `dbt/seeds/` for the reviewed
  dbt seeds (file labels and checksums from the S3 manifests, no source values),
  and as the one named file
  `config/lakehouse/identity_classification.csv` (codes, classes and public
  source citations). A path that only starts with that name is blocked. Any file over 5 MB is blocked
  unless Git LFS stores it. Acquired data stays in S3 and ignored local folders.
  Git LFS stores two acquisition config files over the limit,
  `config/acquisition/source_registry.json` and
  `config/acquisition/planning_v1/acquisition_plans_v1.json` (listed in
  `.gitattributes`). A clone needs `git lfs install` to fetch them.
- **Privacy scan:** every tracked or untracked non-ignored file, not only the
  diff, is scanned for home-directory and machine temporary paths, email
  addresses outside reserved domains (`example.invalid`, `.test`), phone
  numbers, AWS account IDs (including those in ARNs) and values of identifying keys in `.env`.
  Findings give file, line and type, never the value. Files that cannot be read
  as text are listed as unreviewed. Inspect them before publishing. Intentional
  values go in `.privacy_allowlist` as `<type> <path glob> -- <reason>`,
  matched by type and path only. An entry without a reason is itself a finding.
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
  `.writing_allowlist` as `<type> <path glob> -- <reason>`. An entry without a
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

  The `internal-reference` kind also warns by default. It reports numbered or dated owner decisions and approval records,
  amendment labels, work-package codes, numbered waves, review-round labels, issue numbers used as provenance and local plan paths.
  Each finding names its pattern and source line. Repeated matches of one pattern on one line produce one finding.
  This kind scans tracked UTF-8 text only, including tracked fixture folders. Markdown code spans, fenced blocks and link destinations
  remain in scope because they can contain local references. Review labels require round wording or an explicit review context.
  Dates, statistical metrics and row labels do not count as review rounds. Compact amendment labels use uppercase and exclude fragments of universally unique identifiers (UUIDs)
  and base64 data. The public labels `A1` and `A4` require amendment wording directly before or after them.
  Work-package labels require uppercase `P` and a lowercase suffix. They exclude base64 fragments. Technical names such as `p4d.24xlarge` and `P2P` are excluded.
  Generic approval wording requires a directly following marked record number with a hash sign or a `D` prefix. A nearby date also qualifies it. Owner and user decision or approval wording also counts with a directly following plain number. It also counts with a directly following label of one uppercase letter and optional digits joined to a number by a hyphen. A nearby day and capitalized month name also qualify it. A number elsewhere in ordinary approval prose is excluded.
  Decision labels also count when they cite a section or follow a provenance word. Tracked public seed CSVs remain in scope.
  Python contributes comments and docstrings. Shell, SQL, Terraform,
  JavaScript, TypeScript, Go, C, C++, Java and CSS contribute comments. Dockerfiles and Makefiles contribute hash comments.
  JSON contributes decoded string values, including nested values, with their physical source lines. JSON keys and executable
  string literals are excluded. Other text and configuration files contribute complete lines. URLs, binary files, symlinks and
  the allowlist itself are excluded from this kind. Pattern detection does not establish whether a reference is public or private.

  `internal-reference docs/examples.md -- required public examples of rejected references` is a reasoned allowlist entry.
  Exceptions match the kind and path glob. They also apply when the kind blocks. `--block internal-reference` promotes the kind
  once a whole-repository run is clean. The hook and CI leave this kind in warning mode. `--warn` reports all findings without
  failing, including a kind selected with `--block`. The other writing kinds retain their existing scopes.
- **Markdown syntax:** `markdownlint-cli2` checks every staged Markdown file
  outside `data/` against the documentation baseline in
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
  pushed commit. AI credit and agent-session lines are rejected. Human
  co-authors and factual tool mentions are allowed. Detection uses an explicit
  agent and vendor list, so new forms need a failing sample before the check is
  extended. It also rejects internal plan references. A plan or task code is
  one capital letter and one or two digits (`G4`, `A54`, `U4`, `T9`). It counts
  after `unit`, `lane`, `failure mode`, `task`, `slice` or `step` (`lane A`,
  `unit U4`, `failure mode T9`) or alone in brackets (`(P7)`, `(P1, P3 to P5)`).
  `S3` and the public registry source families `S13` to `S16` are never codes.
  A bracketed list counts when any of its codes is not one of them. An issue ID (capital
  letters, a digit, a dash and digits, such as `AB1-123`) counts after `Refs`,
  `Issue`, `Ticket`, `Closes`, `Fixes` or `Resolves` or in brackets. `SHA-256`
  and `CVE-2024-1234` do not. Review-round names are `recheck 2`, `fix round 2`,
  `review #3` and, on a line that also says review, recheck or reviewer, a
  number word beside the round (`over five rounds`, `round two`). The phrases `round trip`,
  `round two decimals` and `two round numbers` pass. A patch or diff hash prefix (12 or more hex
  characters after `hash`, `SHA-256` or `checksum`) counts only on a line that
  names a patch or diff and says accepted, reviewed or staged, so a downloaded
  patch's checksum passes. A snapshot ID in any hex form (7 to 40 characters)
  counts only on a line that says worktree, so a data snapshot ID passes. "Not
  independently reviewed" is also rejected. A bare `B1` (hex, CSS class) and a
  short commit hash pass. A finding names the form,
  never the text. Review pull request text separately. A Git hook cannot read it.
- **Docs match the code:** every environment variable the code reads is listed
  in `.env.example` and every variable listed there is read by code, a script,
  a workflow or a template. Local commits require an ignored `.documentation_review.json`, a per-document
  review outcome bound to the complete staged source and configuration snapshot. Before the first release tag,
  the document inventory is `README.md` only. Other documents, including explicit extra formats, do not require
  per-commit review during development. A version tag such as `v1.0.0` restores the full inventory and requires
  its review. Other tags do not change the inventory. The review reads local tags, so commit from a full clone
  that has the release tags. Draft the record with
  `.venv/bin/python -m scripts.quality.documentation_review prepare --reviewer <name> --reviewed-at <UTC>`
  after staging, then fill each outcome and note locally. Read the README in full during development.
  Once released, read in full every changed or new document and every unchanged one that the staged change affects.
  After a staged change, `prepare --refresh --carry-forward` keeps the
  outcome of each document whose staged content is unchanged and whose earlier entry is valid, with the earlier
  review time in its note. Set it back to `pending` if the change affects it. Never stage
  this record. The local hook validates its freshness against the index and rejects
  tracked or unignored evidence. GitHub Actions checks that the record is untracked and ignored and runs
  synthetic hook tests. It cannot verify the private local review itself.
- **Removed names:** a name the staged change removes is no longer referenced:
  a deleted script, a command-line flag, an environment variable, a top-level
  Python function or class, a config key with `_`, `-` or camelCase, a Terraform
  variable, output, module, resource or data source, a deleted dbt model or a
  dependency that leaves every requirements file. Code names are searched in
  code and config and in the code spans and fences of Markdown, not in prose.
  `CHANGELOG.md` is history and is not searched. GitHub Actions runs the same
  check over every pushed range. A reference that must stay goes in
  `.cleanup_allowlist` as `<kind> <path glob> -- <reason>`. An entry without a
  reason is itself a finding. It blocks: a replay over every earlier commit
  found only deliberate references (a removed config key that the bronze loader
  refuses by name).
- **Unused code, dependencies and files:** vulture 2.16 (60%
  confidence) reports functions, classes and variables nothing uses. Deptry
  0.25.1 reports requirements nothing imports and imports no requirements file
  declares. `orphan-files` reports tracked files no other file names by path,
  file name, folder or import (including `python -m`). Tflint (`terraform_unused_declarations`, each
  folder that holds a tracked `.tf` file) reports unused Terraform declarations. vulture and deptry are pinned
  in `requirements.txt`. They block. deptry checks each runtime on its
  own code and requirements, as `config/quality/dependency_runtimes.json` maps
  them: the host (every file no other runtime claims), the review tools, the
  Spark jobs, the analytics image and the SQLFluff tools (no project code). A
  package a runtime gets outside pip, such as PySpark, is listed there with its
  source. A hash-locked requirements file also pins transitive packages, so only
  its missing imports are reported. A file two runtimes share reports a package
  as unused only when neither imports it. Exceptions go in `.cleanup_allowlist` with a
  reason, under the kinds `unused-code`, `unused-dependencies`, `orphan` and
  `terraform-unused`. The folder-loaded models and macros of dbt are listed there.
  A name used in a way vulture cannot see (a parser callback, a pytest autouse
  fixture, a field written out through `dataclasses.asdict`) goes in
  `scripts/quality/vulture_whitelist.py`, one name per entry with its reason, so
  the rest of its file is still checked. The whitelist also keeps one dead WONDER
  constant until the next WONDER code version, because its file is in the
  approved code hashes of 9 collectors. Run one check with
  `.venv/bin/python -m scripts.quality.repo_checks unused-code` (`--warn`
  reports without failing).
- **Lint settings:** Ruff keeps rule sets `E`, `F`, `I`, `B`, `UP`, `S`, `SIM`
  and `T20` at line length 160 with no ignore or exclude settings. MyPy keeps
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
  suppression is Ruff `S603` in `scripts/process.py`, the one process launcher.
  Other scripts start programs through it and run as modules
  (`.venv/bin/python -m scripts.quality.scan_secrets`). Tests use
  `tests/support.py` `check()` instead of `assert`.

The strict command above prints the selected file count and every pending file with its cause for the chosen runtime.
The check E2E runs the host, acquisition and orchestration scopes with separate empty MyPy caches and verifies synthetic
import chains and manifest mismatches.

**Acquisition checks** (`bash scripts/acquisition/run_checks.sh`) run Ruff, MyPy,
registry validation, the acquisition regression tests and every offline E2E
suite twice, with a 90% coverage gate that also counts collector command lines
run as child processes. They take about 20 minutes on this machine (the recorded profile estimate), so they run only
when acquisition code, configuration or their dependencies change: through the
`acquisition-checks` pre-push hook (`scripts/acquisition/run_checks_pushed.sh`), which is one unsharded run and in GitHub Actions, where the pytest run is four shard jobs
and a combining job (see Sharded acquisition tests below for the LFS checkout and the pinned Terraform). The CI jobs use the
hook's own path pattern through `scripts/acquisition/run_checks_ci.sh`, which also
matches the CI workflow. A manual run, a push with no earlier commit or rewritten
history runs every check. `bash scripts/acquisition/run_checks_ci_e2e.sh` tests
that gate. CI runs on every pull request, on every push to `main` and on a manual dispatch. A push to any other
branch runs no CI, because its pull request run checks the same commits. Each push to `main` gets its own run, so a newer
push never cancels the check of an earlier range. They need no AWS credentials and no
collected `data/`. Off macOS, the HUD workbook and WONDER suites substitute only the
macOS browser-download metadata (the "downloaded from" attribute and file creation
time). On a Mac the real reads run. When the private archive is absent (as in CI), the
registry-additions suite skips its single legacy-archive check and the code-version suite its archived-original
checks. Both record the skips. Each gate run writes `run.json` beside the suite outcomes in `data/e2e/acquisition_checks/`
with the seconds of every suite and a hash of the acquisition code and configuration. A change during the run fails it. Every command of
the gate prints its start, end and seconds (see Command timings below).
`ACQUISITION_E2E_MODE=iteration` runs each offline suite once instead of twice for a faster loop and marks the run
"not commit evidence". The hook and CI always run the full mode. The coverage gate leaves out
the tools in `scripts/acquisition/one_off/`, which ran once and are evidenced by their run records. A
gate test loads every committed collector plan through its contract. It needs the collected `data/`, so CI
skips it.

Each check has a bad and a good sample run through the real `pre-commit` entry
point (`scripts/quality/run_checks_e2e.py` and
`scripts/quality/run_documentation_review_e2e.py`). Both run in local CI.
The source lint cases also run through `scripts/quality/run_lint_coverage_e2e.py`, which retains complete positive and
negative hook logs under `data/e2e/lint_coverage/`. Its `--mutant` mode verifies that a dispatcher which does no checking
fails the same cases. The samples are synthetic. No script body, SQL statement or container runs in that suite.
Each repository policy check ran on the whole repository before the project enabled it. None gave a false positive, so these checks block from the start. The writing check blocks a kind of finding only after the existing prose is corrected. Its dash and slash kinds block, because the project corrected that prose on
Oct 15 2026. Its semicolon kind warns until a whole-repository run has no semicolon finding. The unused code, dependency and file
checks block, because the project deleted their findings or listed each with a reason.

## Docker calls

`scripts/quality/docker_calls.py` refuses every process start it cannot prove is free of Docker outside `orchestration/launcher.py`.
The `docker-calls` hook and the `repository_code_checks` group run it. Process starts that a reviewer read and cleared are listed in
`config/quality/docker_calls_reviewed.json`. Each entry binds the exact bytes of its file, the repository modules that file imports,
its finding class, its number of findings and the SHA-256 of its stripped code line. Any edit of the file or of an imported module
brings the finding back until the entry is rebound.

Rebind the entries after such an edit, then read the printed changes:

```bash
.venv/bin/python -m scripts.quality.docker_calls rebind
```

The command refreshes each entry's file hash and dependency hashes and moves its `line` pointer. A pointer moves only to a line whose
stripped text has the entry's code hash. Among identical lines it keeps the entry's position in the reviewed copy of the file: the
working copy, the staged copy, `HEAD` or one of the file's last 200 versions on any ref, whichever has the recorded file hash. It prints every moved pointer and every hash change. It
never adds, removes or widens an entry and never changes `reason`, `problem` or `occurrences`. A second run prints `no change`.

The command fails and writes nothing when any entry's reviewed line no longer exists or became ambiguous. A line is ambiguous when the
number of identical lines changed or when the old pointer did not name the code. Without any reviewed copy the entry always fails,
because an edited duplicate could otherwise be taken for its twin. Each failure names the file and the old line. Review that site again and update its entry by hand. A rebind does not review
code. Read every moved pointer and hash change before committing. Then run the check.

The cases run in scratch Git repositories:

```bash
.venv/bin/python -m scripts.quality.run_docker_calls_rebind_e2e
```

They cover a moved line, identical lines with the ordinal kept, a vanished line, an edited line that must not be re-pointed, the same edit
committed (the reviewed copy only in history or gone after an amend), a dependency change, a rerun with no change and the two `scripts/quality/run_ci_groups_e2e.py` pointers that drifted in past sessions.
Evidence is `data/e2e/docker_calls_rebind/<UTC>/report.json`.

## Container imports

`scripts/quality/container_mounts.py` reads the Compose declarations, launcher job tables and renderer syntax.
`scripts/quality/container_imports.py` follows first-party imports against each invocation's file locations.
The check runs no application imports, container commands, installs or network requests. It does not read mounted data or interpolate host environment values.

Run the same command locally and in CI:

```bash
.venv/bin/python -m scripts.quality.container_mounts
```

The `container-mounts` pre-commit hook always runs. It takes about two seconds, so it stays in the commit stage with the other fast checks. The `repository_code_checks` CI group runs that command and its hook samples.
Its path list covers Python sources, Dockerfiles and Compose files at the repository root or under `orchestration/`, `services/` and `config/`.
Each service keeps its own mounts. A quality job does not gain the mounts of a pipeline job that shares its image.
The mount declarations remain the authority. The check keeps no second mount allowlist.
`PYTHON_SOURCES` is the remaining hand-kept source-role classification. It classifies `scripts/` and `orchestration/` as import dependencies.
It classifies `dags/`, `services/lineage/`, `services/analytics/` and `feature_store/` as framework or image entry directories.
A bind that contains repository Python outside these prefixes fails as an unclassified Python mount, with its declaration location.
Mount helpers are followed through their declared `Mount` return types, including lists and tuples, without a helper-name list.

The supported declarations are:

- Compose short and long bind volumes, including merged service anchors, `working_dir`, `PYTHONPATH` and Python module or script commands.
  Airflow's mounted DAG files and lineage provider files are entry points. Renamed packages retain their container names.
- Launcher `Mount` calls and pipeline mount tuples with literal paths under the checkout.
  The Spark and gold job tables supply their module names. Renderer defaults and fixed pipeline layer choices supply the other entry points.
  The `workdir=` argument of each renderer's `assemble` call supplies its working folder. Pipeline specifications use their launcher's declared working folder.
  The training renderer's modules come from the `JobSpec` entries of `training_render.build`. Its snapshot binds come from `TRAINING_ROOTS` in `code_digest.py`
  and the `/workspace` mount rule. Its action mounts shadow the snapshot beneath them. An unresolved root, mount target or module fails the check.
  A conditional module resolves only when both branches are literal strings. The mount rule must be the comprehension
  `(str(path), f"/workspace/{path.name}", True) for path in sorted((folder / "code").iterdir())`. Any other form fails and names the part that differs.
  The folder must be a name that the builder assigns once and passes to a call named `verify_snapshot`.
  Accepted detector limit: the check does not prove that call is the real `code_digest.verify_snapshot` or that it runs before the mount.
- Literal Python files copied by a final Dockerfile stage to an absolute destination, including the analytics UI relay.
  The check reads the Dockerfile in the declared build context. It does not build or inspect an image.

Imports include normal imports, `from` imports, aliases, relative imports, package initialization and literal dynamic imports.
The check follows import-time code and referenced functions and classes in imported helpers. It checks every function of a declared entry point.
An unused helper command under `if __name__ == "__main__"` is not a second container entry point.
Individual file mounts grant access to those files. They do not grant access to every sibling in the package.
The mount destination must also place the module on the container's Python search path.
Direct Compose module commands require a declared `working_dir` or absolute `PYTHONPATH` roots. Relative script commands also require `working_dir`.
Image working folders are not inferred. An unresolved working folder fails at its declaration.
Script imports use the script directory and explicit `PYTHONPATH` roots. A script does not gain imports from its working folder alone.

The check skips the bodies of recognized `typing.TYPE_CHECKING` guards and retains their runtime `else` branches.
It still checks first-party imports guarded by `try` and `except ImportError`. There is no per-import exemption.

A failure groups each source file, line and imported module once, with the affected containers and entry points on one line.
Use a module available at that container's mounted paths or correct the declaration. Ask the owner if the rule is wrong for the case.
An unresolved Python entry point or unsupported conditional Python source mount fails with its declaration location.
Shell-wrapped Python commands fail when their entry point cannot be resolved. Import findings and invalid declarations block the hook.

Computed import names, generated code, runtime path changes and third-party package availability remain outside this static check.
The check does not follow another Python program started by a container's Python code, including a `subprocess` command.
A package invoked with `-m` does not have its `__main__.py` execution resolved. Declare an executable module for checked entry traversal.
The Ruff process-start guard example concerns whether a tool start can be proved. That example is outside this check's first-party import scope.
A bronze writer importing acquisition code absent from its declared files is an in-scope missing-import case.
It does not prove that a container starts or that a real job completes. Container runs remain separate verification.

The source-only hook samples run through pre-commit itself:

```bash
.venv/bin/python -m scripts.quality.run_container_mounts_e2e
.venv/bin/python -m scripts.quality.run_container_mounts_e2e --mutants
```

The samples reconstruct an unmounted training contract imported by a DAG runtime and an unmounted pipeline protocol imported through a lakehouse engine.
They check both quality and Spark mounts. Other cases cover transitive imports, package initialization, aliases, relative imports, cycles, copied scripts and changed mounts.
Cases also cover pipeline mount edits, gold job additions, unreferenced entry functions, class methods and unclassified Python mounts.
A new launcher job is checked without updating a module allowlist. Positive cases cover unchanged source, unused host commands and unavailable third-party packages.
The runner also checks CI path selection and runs the real dispatcher with a timing wrapper that records leaf commands without executing them.
It requires the checker and sample runner commands exactly once. `--mutants` plants weaker checker copies and a missing CI command to prove that the cases detect them.

Evidence is `data/e2e/container_mounts/<UTC>/report.json`. It includes source hashes, fixture changes, exact hook commands, expected diagnostics and observed exit codes.
The runner checks that its source snapshot stays unchanged and removes its temporary repositories.
Each hook runs under the repository's process-group deadline entry point, with a 30-second command deadline.
A hook that the deadline stops before its checker began is run again with twice the seconds, up to three runs in all. The fixture's Python start-up writes a marker when the checker process begins. A stop after that marker is a result that fails the case at once. The record names the runs, the seconds of the last one and whether the checker started.
The 720-second run deadline includes setup and reserves 70 seconds for process-group cleanup.
An unconfirmed cleanup stops the run and retains its workspace. A case labeled `harness` verifies the deadline entry point and its process-group cleanup.

## Model metadata

`scripts/quality/model_metadata.py` reads the dbt YAML and SQL in every folder that `dbt/dbt_project.yml` lists for models, seeds, analyses, tests, macros and snapshots
(`.yml` and `.yaml`), the gold contract `data_contracts/lakehouse/gold_contract.json` and the bronze table map `config/lakehouse/bronze_tables.json`.
The installed `.tools/sqlfluff` environment compiles models through its dbt templater with a private in-memory `lint` profile. SQLFluff parses the compiled SQL.
Compilation uses the installed dbt packages, writes temporary artifacts and starts no build or container. It needs no catalog, credentials or network.
The `model-metadata` hook runs when a dbt YAML or SQL file, a macro, the contract, the table map or the check itself changes.
A model whose SQL starts a `config(enabled=false)` is disabled for dbt and for this check. The check does not compile it and does not require its YAML entry.

Run `.venv/bin/python -m scripts.quality.model_metadata`. It exits 0 when the project is clean, 1 for a problem and 2 when it cannot read the project,
for example after a malformed YAML, JSON or Jinja file. A cannot-run report names the file, line, cause, repair and who to ask. It prints no traceback.
Add `--lineage-report <report.json>` to retain output-column origins, compiled SQL hashes and the conservative-model list.

It blocks in these cases. The rule name in brackets starts each problem line.

- A source table has no `meta.classification` (`source-class`). A model has no `meta.classification` or no YAML entry (`model-class`).
- A model marked Public outputs a named confidential source column, including a rename or a derived expression (`class-inheritance`).
  `Confidential` is higher than `Public`. Reading a Confidential table alone does not raise the model's class.
- A Confidential source has no nonempty `meta.confidential_columns` list of explicit column names or the explicit value `all`. All-column mode requires
  a nonempty `meta.confidential_columns_reason`. Every source column then carries a confidential origin, including renamed or derived outputs.
  `meta.proposed_confidential_columns` records candidate columns for later review and never narrows an active `all` declaration. A Public source declares confidential columns,
  or a model tries to declare its own confidential columns (`confidential-columns`). These declarations belong on sources.
- A source or model declares obsolete `meta.classification_override` or `meta.lineage_columns` metadata (`lineage-override`).
  Output lineage determines classification. No model identity, reason or self-declared column list grants an exemption.
- A Confidential dbt source, bronze collection, bronze table override or loader table has no `exception` (`exception`). On a dbt source it is `meta.exception`.
  It names the documented ingestion exception.
- A bronze table has no class, a dbt source in the `bronze` namespace is lower than its bronze table or a source has no bronze table (`bronze-class`).
  The table map's `classification` object holds `collections`, `loader_tables` and an optional table-level `classification` override.
- A gold fact (a `fct_` model under `dbt/models/gold`) has no `meta.fact_type` (`fact-type`).
- A measure column of a gold fact has no `meta.additivity` on its column entry (`additivity`). The gold contract supplies the inventory: every numeric fact column
  except the date-part columns (`DATE_PARTS`, for example `data_year`) and lineage counts (`LINEAGE_COUNTS`, for example `source_row_number`).
- A column labeled `semi_additive` has no nonempty list of axis names in `meta.additive_across` (`additive-across`).
- A semi-additive `fct_cost_report` column lacks the exact restriction in `meta.additivity_note`: `sum across hospitals only after choosing one report and period per hospital` (`additivity-note`).
- A fact model has no contract entry or a contract fact has no model (`fact-contract`).
- A model, source table or macro is defined twice or two SQL files share a model name (`duplicate-entry`).
- A `ref`, `source` or macro call cannot be resolved (`unresolved-reference`). This includes unresolved callable aliases and unknown context calls.
- A value is outside its set (`allowed-values`). Classes are `Public` and `Confidential`. Fact types are `transaction`, `periodic_snapshot` and
  `accumulating_snapshot`. Additivity is `additive`, `semi_additive` and `non_additive`.

Reference discovery statically evaluates Jinja variables, loops, macro arguments, return macros and project variables, including macros in subfolders.
It resolves `builtins.ref`, `builtins.source` and qualified project calls. Only explicitly supported calls and methods count as having no dependencies.
`dbt_utils.get_column_values` is the supported package macro. The statements `include`, `extends` and `import` are rejected.
Sources are keyed by source name and table name. Seeds hold Public or Synthetic data and cannot raise a class.

Parsed lineage follows output expressions, aliases, CTEs, supported wildcards and positional union branches. Each source occurrence has its own parsed position.
Relations are registered by their unqualified table name and alias. Earlier select aliases resolve within the same select list.
Declared columns resolve before earlier aliases. An earlier alias also retains the possible origins of an undeclared real column with the same name:
one candidate relation adds its unknown-column policy. Several candidates add their combined confidential origins.
An unknown name with one candidate relation uses that relation's policy:
a Public source contributes no confidential origin, an explicit-list source protects only listed names and an all-column source protects every publisher name.
For the two ONC sources, loader lineage `_object_key` and `_member_sha256` are exempt at the loader boundary. The exemption applies at the source boundary.
A publisher field renamed to either loader name retains its confidential origins.
An unknown model or CTE output retains the union of that relation's confidential output origins. With several candidate relations and no declared match,
an unknown name retains their combined confidential origins for that reference. Registering both a table name and its alias does not duplicate the relation.
A two-part reference without a local relation qualifier uses the first part as its struct-column name. Correlated references used only in WHERE, HAVING or JOIN ON do not contribute to output origins.
Correlated FROM sources and select-list references remain unsupported, including a FROM source whose outer reference occurs in its predicate.
Table functions in FROM, lateral sources, correlated sources or select-list references, dotted function calls, positional column references and other unsupported SQL constructs require whole-model conservative handling.
A construct the check cannot trace propagates every named confidential column from its input models' outputs to every output.
That input set includes declared references and known relations found in parsed SQL. Compilation or SQL-parse failures retain all named confidential source columns.
A SQL analysis exception retains all named confidential source columns for the affected model and lets the worker continue checking other models.
Class-inheritance messages append the conservative reason when one applies. The lineage report also retains the type and text of caught analysis exceptions.
The lineage report names each such model and its reason. The compiler and parser worker has a 90-second deadline and reports an exceeded deadline with the wait duration. An unresolved compiled relation blocks the check.
Write the documented ingestion exception on every Confidential source, collection and table override.

`meta.additive_across` lists allowed aggregation axes, for example `[hospital]`. The check requires the list and does not restrict axis names.
`meta.additivity_note` carries a free-text restriction. Every semi-additive `fct_cost_report` measure requires the exact note above: choose one report and period per hospital before summing across hospitals.
WONDER population sums hold one database fixed and exclude repeated-export and held rows.

The same module provides `scan-personal-data --manifest <manifest.json> --output <report.json> [--inventory <inventory.json>]`
for explicitly listed local CSV, JSON, JSONL, NDJSON or text exports.
The manifest has a `tables` list. Each entry names a `collection`, a `table` and a nonempty `files` list of export paths. Relative paths resolve from the manifest.
An optional `columns` list declares the table's schema names. Otherwise the first CSV file's header supplies them. JSON keys outside this list become `field_N`
in emitted paths, including lowercase keys that look like identifiers. Nested unknown keys are replaced too. Text exports use the fixed `line_text` column.
An inventory is a JSON object with a nonempty `tables` list of `collection` and `table` pairs. Duplicate pairs or a missing inventoried table make the scan incomplete.
Without `--inventory`, `complete` confirms only that the listed files were read. It does not prove that every table was listed.
Only listed files are scanned. Supply the full approved export inventory to assess a collection. Unsupported or unreadable files make the report incomplete.
The scanner cannot read Parquet, spreadsheets, PDFs, binary documents, archives or database table storage directly. They need a supported local text export.
Findings contain column paths, pattern kinds, their basis and counts. No stored value is emitted. The patterns flag possible names, addresses, identifiers,
dates of birth, emails and phone numbers in text or column names. They can flag organizational text and can miss personal data without a supported pattern.
A result with no matches is a pattern result, not formal anonymization or collection-wide clearance. The scanner refuses symlink exports and report paths that overlap inputs.
It exits 0 for no matches, 1 for possible personal data and 2 for incomplete scanning. JSON files over 64 MiB need a streaming JSONL export.

When a rule seems wrong, ask the owner for the change. Never skip the hook.
`.venv/bin/python -m scripts.quality.run_model_metadata_e2e` checks bad and good samples through the real hook and direct CLI.
The full run also tests the scanner on synthetic exports, including value-free reports, malformed inputs and repeat runs.
It writes `data/e2e/model_metadata/<UTC>_<pid>/report.json`. `--case <NAME>` runs one metadata case.
`--checker` and `--hook-config` select another checker or hook configuration, for example a mutant with one rule removed.
`scan-personal-data --output-directory <folder>` runs only the scanner fixtures through the E2E module.

## Linters beside lint_sources

Seven more linters run through `.venv/bin/python -m scripts.quality.lint_tools <CHECK>`. Each check verifies that its tool runs at the version pinned in
`config/quality_tools.json` before it scans and fails visibly if the tool is missing or differs; the check itself never installs or updates a tool. The hosted
setup action installs every tool from the same pins and the seven hooks run in the manual stage, so they are in the code partition of the hosted hooks job
(`hook_groups` in `config/quality/ci_groups.json`). A finding prints as
`path:line: <CHECK>: problem`. The single exception list is `.linter_allowlist`, one line per exception as `<CHECK> <key> -- <reason>`. The key is `path:rule`
(`path:package==version` for OSV-Scanner) and may use `*`. An entry that has no reason, an unknown check name or no matching finding is itself a
finding, so the list cannot rot.

| Hook (check) | Tool and pin | What it blocks | Configuration | Stage |
| --- | --- | --- | --- | --- |
| `tflint` | TFLint 0.65.0 with the AWS ruleset 0.50.0 | Terraform that breaks the recommended preset or an AWS rule, in every folder that holds a tracked `.tf` file | `.tflint.hcl` | commit, hosted |
| `trivy-config` | trivy (the one release binary in `.tools/bin`) | misconfigurations in `infra/`, from trivy's embedded checks (no network); `*.tfvars`, `*.tfvars.json`, `terraform.tfstate*` and `.terraform` are skipped by name and never read | skip flags in `lint_tools.py` | commit, hosted |
| `osv-scanner` | OSV-Scanner 2.7.0 | known vulnerabilities in the tracked requirements files and `services/orchestration/constraints-3.12.txt` | `.linter_allowlist` | pre-push, hosted |
| `zizmor` | zizmor 1.31.1 | workflow security findings, offline audits only | `.linter_allowlist` | commit, hosted |
| `import-linter` | import-linter 2.15 in `.tools/import-linter` | an import that breaks a boundary in `.importlinter` | `.importlinter` | commit, hosted |
| `typos` | typos 1.51.1 | common misspellings in staged text files | `_typos.toml` | commit, hosted |
| `check-jsonschema` | check-jsonschema 0.39.0 in `.tools/check-jsonschema` | a workflow, action or mapped JSON contract that does not satisfy its schema; a schema that breaks its metaschema | `config/quality/json_schemas.json` | commit, hosted |

- **TFLint:** the existing `terraform-unused` hook keeps running only the unused-declarations rule with the cleanup allowlist. The `tflint` hook runs the full
  `.tflint.hcl` configuration instead. The AWS ruleset is installed once with `tflint --init` (the hosted setup action runs it and TFLint verifies the release
  checksums and signature of the plugin). The check refuses to run when version 0.50.0 is absent from the plugin folder (`TFLINT_PLUGIN_DIR`, default
  `~/.tflint.d/plugins`). The release archive hash in `config/quality_tools.json` is 5a71c634...ac71 for macOS arm64. `install_tools.py` replaces nothing that
  differs from the pin, so a binary placed by hand or a receipt from an older pin stops the whole installer until the old binary and receipt are moved away and
  the tool is installed again (`.venv/bin/python scripts/quality/install_tools.py --tool tflint`).
- **trivy config:** one trivy serves the project, the release binary in `.tools/bin` (pin `trivy` in `config/quality_tools.json`, hashes from the publisher checksum file). `static_checks.py`
  and the `trivy-config` hook both refuse to run when it differs from the pin, locally and on the hosted runner.
- **OSV-Scanner:** it queries osv.dev, so it needs the network and runs at the pre-push stage:
  `.venv/bin/pre-commit run osv-scanner --hook-stage pre-push --all-files`. The push gate runs the hook as its own step, `osv_scanner`, over the pushed range
  in every scope and also for a small change. The hosted hooks job runs it in the manual stage too. It skips itself when the range changes no requirements file. The constraints file is scanned as a requirements file without dependency
  resolution. Every pinned version with known vulnerabilities is allowlisted today with the reason that a pin change needs its own E2E and the owner's decision.
  Remove an entry when the pin moves; the check then fails on any new vulnerability for that package.
- **zizmor:** the only finding today is the style suggestion `self-repository` (write the local setup action as `$/.github/actions/setup`), allowlisted until
  the workflow is next edited.
- **import-linter:** `.importlinter` holds forbidden-import contracts for the boundaries that hold today: container services import no script or orchestration
  code, the launcher imports no pipeline package, acquisition and infrastructure stay below the pipeline packages, review scripts import only the launcher and
  training scripts import no acquisition, quality or service code. The `orchestration` runtime package imports no orchestration script, quality or review code.
  No script package except training imports the orchestration scripts. Lakehouse, orchestration, acquisition and infrastructure import each other in both
  directions today, so no contract covers those pairs. The training scripts import the orchestration runtime, the orchestration scripts, the lakehouse and the
  infrastructure scripts. The runtime imports `scripts.training.step_records`. No contract covers those pairs either. A contract is added only after the code
  satisfies it.
- **typos:** `_typos.toml` excludes machine-written data (`config/`, `data_contracts/`, `dbt/seeds/`, the generated architecture page and the pip constraints) and
  accepts a few domain terms everywhere (`hsa`, `ehr`, `froms`, `unparseable`). Other accepted words apply only to the named files; a type glob matches a file name, so
  keep those names unique.
- **check-jsonschema:** `config/quality/json_schemas.json` lists each schema with the tracked files that must satisfy it. A pattern that matches no tracked file is an
  error. Its note names what is not mapped and why.

Install the two Python tools with `.venv/bin/python -m scripts.quality.install_lint_tools` (hash-pinned requirements in `scripts/quality/lint_requirements/`, one
environment per tool). Add `--check` to compare the installed environments with the pins without installing. trivy, TFLint, OSV-Scanner, zizmor and typos are
release binaries that `scripts/quality/install_tools.py` verifies by SHA-256 into `.tools/bin` (the hosted setup action runs it). OSV-Scanner publishes a checksum
file. zizmor and typos publish none, so their hashes are the asset digests that GitHub records for each release file, checked against a download. A check uses
the `.tools/bin` binary when it exists and otherwise the Homebrew binary at the pinned version (`brew install <formula>`, with the formula and executable in
`config/quality_tools.json`).

All seven hooks run in the manual stage, so the hosted hooks job runs them in its code partition. The setup action (`.github/actions/setup`) installs the pinned
tools: the release binaries through `install_tools.py` (SHA-256 checked on every run, nothing restored from a cache), the two Python tools through
`install_lint_tools.py` (hash-pinned requirements, pip cache keyed by those files) and the AWS ruleset through `tflint --init`. The added hosted time of the
code job is an estimate (about 2 minutes) until a complete hosted run measures it. The setup adds an estimated 1 minute to every job that uses the action.
`.venv/bin/python -m scripts.quality.run_lint_tools_e2e` runs the real hooks on a scratch copy of the working tree, once clean and once with a planted
defect per tool. It writes `data/e2e/lint_tools/<UTC>/report.json`. Add `--offline` to skip the OSV-Scanner case.

## How the checks run

CI checks Ruff formatting and security rules, MyPy, outgoing-source secret
scanning, repository checks, configuration, secret store and quality E2E, retained regression
safeguards, Terraform mock tests, TFLint, Checkov and `trivy config` (its
embedded checks only, run offline). Pre-commit scans the actual staged index,
including changes hidden by a clean working copy, before local CI. The source
scan excludes ignored acquisition data. Neither scan sends files to an external
service. Secret values and matching source fragments are omitted from scanner
diagnostics.

Pre-commit (`pre-commit`, `commit-msg` and `pre-push` stages) and `scripts/run_ci.sh`
enforce these repository rules at the pinned versions. Install all three hook types once with
`.venv/bin/pre-commit install` (`default_install_hook_types`), then register the pre-push gate once with
`.venv/bin/python -m scripts.quality.pre_push install-shim` (a Git config hook, Git 2.54 or newer. It is repeatable).
One commit per reviewed change stays the rule.

- **Commit stage (fast, about two minutes on a typical change):** secrets on the staged files, credential and
  data files, the privacy scan, the writing check, front matter, Markdown lint of the staged files, Ruff, shell
  syntax, ShellCheck, actionlint, yamllint, Hadolint, gofmt, go vet, the documentation review, the lint-settings,
  suppression, subprocess-import and Docker-call guards, removed names and the commit message. No commit runs a
  check group or a heavy hook.
- **Pre-push stage:** the gate (`.venv/bin/python -m scripts.quality.pre_push run --hook`, registered as a Git config hook) runs once
  for the pushed range, from its merge base with `origin/main` to the pushed head, with the selector and path lists
  GitHub CI uses, so the local result predicts the hosted run. **First** it runs the changed-file checks
  (`scripts/quality/ci_changed_files.py`, a few minutes): Ruff format and check and mypy on the changed Python files (mypy skips the files that wait for their runtime in
  `config/quality/mypy_runtimes.json`), SQLFluff on the changed dbt SQL files, the Docker-call guard and the check group file guard. A failed check stops the gate before the long
  groups start and names itself; each check has the deadline `pre_push.changed_file_seconds`. It then runs the commit-message and removed-name checks
  of the hosted hooks job, `CI_SCOPE=range RANGE_START=<base> RANGE_END=<head> bash scripts/run_ci.sh` and the three
  hosted hook partitions over the same range. The steps run in parallel, except the acquisition hook, the OSV-Scanner hook and the three hook
  partitions: they run one after the other in one worker (the partitions take about 3 minutes together), because
  pre-commit takes the worktree's index lock and two pre-commit runs at once fail with `index.lock: File exists`. These partitions run the hooks that left the commit
  stage (`unused-code`, `unused-dependencies`, `orphan-files`, `terraform-unused`, `sqlfluff`, `build-contexts`,
  `model-metadata`, `env-example` and the two check E2Es). It runs the full scope instead (`CI_SCOPE=full`) when
  the range changes a path in `pre_push.full_paths` of `config/quality/ci_groups.json` (the group selector and its
  configuration, `scripts/run_ci.sh`, `scripts/ci/`, the CI scripts, the workflow, the setup action and
  `.pre-commit-config.yaml`) and when `origin/main` or the merge base is missing. Every step has a deadline
  (`pre_push.timeout_seconds`: 45 minutes in the range scope, 60 in the full scope) and a step that exceeds it
  fails with its name. The pre-push run also starts the `acquisition-checks` hook once over the same range
  (`pre-commit run acquisition-checks --hook-stage pre-push --from-ref <base> --to-ref <head>`, as
  `scripts/acquisition/run_checks_ci.sh` does), so the hook's own path list decides whether the 12 to 14 minute suite
  runs and an acquisition change earlier in the range still counts. In the full scope the shards of `scripts/run_ci.sh`
  run it. The native hook of the push skips. `pre_push plan` prints the scope and steps without running them.
  **Small changes.** A range in the range scope that changes at most `pre_push.small_change.max_files` files and `max_changed_lines` lines, touches no path on `not_paths` (infrastructure,
  Terraform, Dockerfiles, Compose, service folders, shell scripts) and no CI file runs only the changed-file checks, the commit-message and removed-name checks and the affected groups of
  `scripts/run_ci.sh`. The acquisition hook and the three hook partitions stay with hosted CI, which remains the full gate. `pre_push plan` says when a range is small.
  **Docker release:** after the last step ends, in a `finally` path that also covers a failed step, a step deadline and Ctrl-C, the
  gate runs `.venv/bin/python -m orchestration.launcher docker-release --owner --since <start of the gate>` under one 600 second
  deadline (`RELEASE_SECONDS`). The launcher skips the release unless a container was created since the gate started, stops this
  registry's idle catalog and quits Docker Desktop to free the VM's memory. It refuses while a container it did not create runs or other
  registry work is open. The gate prints one line and records `docker_release` (`outcome`, `detail`, `seconds`) in `report.json`, except after Ctrl-C,
  which prints the line but writes no report:
  `Docker release: Docker Desktop quit in N s (VM memory freed)`, `skipped (<reason>)`, `refused (<reason>); Docker left running` or
  `failed (<cause>); the gate result stands; run .venv/bin/python -m orchestration.launcher docker-start --owner before the next heavy run`.
  A skipped, refused or failed release never changes the gate's exit code or blocks the push. Another project's session that is about to
  start a container but has none yet is not detected, so the quit can interrupt it. The gate sets `HAI_DOCKER_RELEASE=outer` for its
  `scripts/run_ci.sh` step so that script skips its own release. The next heavy run starts Docker Desktop again
  (`scripts.lakehouse.catalog up` calls `docker-start` first). `--release-registry DIR` passes `--test-registry` to the launcher and is for
  tests of the gate only.
  The gate is the Git config hook `hook.hai-push-input`, which `install-shim` registers. Git starts it for every push
  with the whole push input, before the hook of pre-commit, so the gate does not depend on the reference that
  pre-commit hands its own hooks. The gate judges each updated reference. A deletion passes without checks. A
  reference whose commit is already in `origin/main` passes without checks and is recorded in `report.json`. A
  reference at the checked-out commit is checked. Any other reference refuses the whole push. The gate also refuses a
  push when the pushed head is not the checked-out commit or when tracked files differ from HEAD (staged or
  unstaged), unless every reference passed without checks. Untracked files do not block. The `local_ci_pushed`
  pre-commit hook (`pre_push check-shim`) refuses a push when the Git config hook is missing, disabled or changed.
  It runs no check and cannot gate a push that pre-commit does not dispatch, so the Git config hook is the gate.
  `git push --no-verify` skips both. The gate and every step start without the variables that change what
  `scripts/run_ci.sh` or pre-commit runs (`EXECUTION_CONTROLS` in `scripts/quality/pre_push.py`: `CI_SCOPE`,
  `CI_PART`, `CI_UNITS`, `CI_LIST_ONLY`, `RANGE_START`, `RANGE_END`, `LAUNCHER_SHARD`, `FEATURE_SHARD`, the
  `ACQUISITION_*` selectors, `GITHUB_ACTIONS`, `HAI_DOCKER_RELEASE`, the `PRE_COMMIT_*` reference variables, `SKIP`,
  `PRE_COMMIT_ALLOW_NO_CONFIG`, `PRE_COMMIT_RUNNING_LEGACY` and `_PRE_COMMIT_SKIP_POST_CHECKOUT`) and without the Git
  repository variables of the caller. A case scans the installed pre-commit source and fails on a new variable that
  is neither cleared nor classed as harmless.
- **Manual stage:** unchanged. `.venv/bin/pre-commit run --all-files --hook-stage manual` runs the whole set
  (its `local_ci` hook runs the full local CI). Run the full set before every release and also when hosted CI
  is unavailable. Hosted CI stays the merge gate and runs this stage unchanged.
GitHub CI runs the manual stage on every pull request and push to `main` as parallel
jobs after a quick plan job (see Main-run reuse below): three hook partitions without `local_ci` (file-based hooks check the changed files of the range), `scripts/run_ci.sh` as `CI_PART=core`, one job each for bronze, silver and gold (`CI_PART=bronze`, `silver` or `gold`, see Lakehouse layers below), eight feature-source shards and four feature-column shards (`CI_PART=feature_sources` or `feature_columns`, `FEATURE_SHARD=K/N`), a feature evidence combining job and a job for the
gold model metadata E2E (`CI_PART=gold` with `CI_UNITS=model_metadata_e2e`), the launcher E2E in two shards
(`CI_PART=launcher`, `LAUNCHER_SHARD=K/2`), the reuse, shard and gate stage cases of the CI groups E2E in their own job (`CI_PART=reuse_gate`, the check group `reuse_gate_checks`, apart from Local CI Core, which runs only the selection cases, so that Core keeps its headroom under its 30 minute limit) and the acquisition shards with their combining job, each for the groups the range
touches (`CI_SCOPE=range`). Every checkout of a pull request run is the head commit of the pull request, not GitHub's merge of the
head into the base, so the tree a pull request run checks is the tree that Main-run reuse compares. Without a usable range (a manual run, the first push
of a branch or rewritten history) every check runs. A job whose group the range does not reach
reports success and writes the reason to its job summary. Each job caches only what a
lock file or hash checks again before use (npm packages, Terraform providers,
the dbt package archive and the SQLFluff packages). The Python environment is
built fresh from `requirements.txt` without a pip cache. Each job's timeout is about
twice its measured time (see Measured durations below). Locally `scripts/run_ci.sh` runs every part the way the GitHub jobs do: the gates
first, then the launcher E2E, then the units that must run alone (the gold writer setup) and then the independent check groups and units, which run
concurrently: Local CI Core, the reuse and gate stage cases, bronze, silver and gold, eight feature-source shards, four feature-column shards, the three hook partitions, the E2E of the gold model metadata with its workers and the four acquisition shards. The feature combining steps require every numbered report and exactly one passing result for every expected case. After them the acquisition combining step merges the four acquisition shard folders and enforces the 90% coverage gate. The hosted target is about 15 to 20 minutes. The new feature and hook durations are estimates until a complete hosted run measures them. The local launcher E2E runs first and alone, as the same number of shards as the hosted matrix (`launcher_shards` in `config/quality/ci_groups.json`, checked against the workflow), at once. Before this split it took about 13 minutes in one process. Its wall time with the split is not measured yet. Each shard has its own log, which prints in shard order after the shards end. A line says every minute how many shards still run. The shards share the deadline `launcher_shard_seconds` (`CI_LAUNCHER_LIMIT_SECONDS` replaces it). Past it the run stops each shard with its processes and fails with the shard names. `LAUNCHER_SHARD=K/N` still runs one shard. Its time, the gold writer setup (which runs alone) and the final acquisition combine add to the parallel critical path, so this layout does not establish the 15 to 20 minute local target. A separate measured trial runs the launcher E2E in the parallel set five times on this Mac (16 cores, 48 GiB). Only if all five runs pass does the launcher join the parallel set. The exception to the run alone rule is recorded with the evidence. Until then the launcher stays alone, first. `CI_SCOPE=staged` runs the groups the staged paths select on demand (no hook runs it) and reuses no earlier result. The run prints each group's seconds and each command's
start, end and seconds (GitHub CI adds both to the job summary). In the full set gitleaks scans every outgoing source file instead of
the staged index. The acquisition checks
run in their own CI jobs and, before a push, through the pre-push run (their hook, once over the pushed range) only when acquisition files
change. The full local CI runs them as the same four shards and combining step as GitHub CI, each shard in its own folder
under the quality evidence folder of the run. The hook and `bash scripts/acquisition/run_checks.sh` run them unsharded. Run
them directly before a release with that command.

The checks cover secrets and credential files, data files (allowed only in `tests/fixtures/`, `dbt/seeds/` and the one file `config/lakehouse/identity_classification.csv`), a
privacy scan of every non-ignored file, prose style, Markdown syntax, front matter, commit messages, documentation that
matches the code, removed names, unused code, dependencies and files, lint settings and Docker calls outside the
launcher. Each has a failing and a passing sample run in local CI. Local commits need an ignored
`.documentation_review.json`, a review of `README.md` before the first release tag and the full document inventory
from that tag onward, bound to the complete staged snapshot
(`.venv/bin/python -m scripts.quality.documentation_review prepare`). There are no bypasses:
never use `SKIP=` or `git commit --no-verify`. If a check blocks a valid change,
fix the check or ask the repository owner.

`CI_SCOPE` selects the check groups: `full` (the default: every group, the acquisition gate included), `staged` (the
groups the staged paths touch, on demand, with no hook that runs it because commits run only fast hooks) or `range` with `RANGE_START` and `RANGE_END` (GitHub CI and the pre-push hook). `CI_LIST_ONLY=1` prints which groups would run
and why. It runs nothing. The groups and the paths they depend on are in `config/quality/ci_groups.json`. A path that
no group lists and that is not passive selects every applicable group. Markdown outside `dbt/`, anything under `docs/`,
`.gitignore` and `.gitattributes` are passive: they do not select every group, but a group whose list matches one still
runs. A change of passive paths that no group lists selects no conditional group. The `sql_style` group runs in every
full and range run (the run before a push and GitHub CI). In the staged and paths scopes it runs when a listed SQL input changes:
`dbt/`, `.sqlfluff` and `services/` SQL, the dbt package archive configuration and the SQLFluff pins and
installer.
The source, privacy, secret, credential, data-file, writing, Markdown lint and documentation gates always run.
They run in every scope and every part, also when no conditional group
is selected. The Markdown lint gate checks every Markdown document the markdownlint hook checks, so a change to any file
markdownlint reads (its configuration in any folder, a file a configuration extends or the pin) cannot skip it. The
acquisition gate is the exception: it runs only in the full scope and its own hook and CI job cover commits and pushes.
The `sql_style` group inventories every tracked SQL file and checks each active source with its runtime profile.
Files copied by the repository check E2E runners select `repository_code_checks`, including `README.md`, this guide,
`.env.example`, the dbt project and profile files and the copied intermediate-model YAML fixture.

Each CI run retains configuration evidence under `data/e2e/configuration/` and
quality evidence under `data/e2e/quality/`. The secret store E2E writes its report under `data/e2e/secret_store/` and the gold writer setup
check under `data/e2e/gold_publish/`. `configuration_checks` also runs the E2E for the AWS catalog setup (`scripts.lakehouse.run_aws_catalog_setup_e2e --mutations`). A change to `scripts/lakehouse/aws_catalog_setup.py`, its E2E or `scripts/lakehouse/principal_credentials.py` selects that group. The E2E writes its report under the run's quality evidence in `aws_catalog_setup/`. `configuration_checks` also runs the Glue mirror E2E (`scripts.lakehouse.run_glue_mirror_e2e`), which uses a fake Glue client and a fake Polaris pointer source and writes `glue_mirror/` beside it. A change to `scripts/lakehouse/glue_mirror.py` or its E2E selects that group. `configuration_checks` also installs the pinned DVC (`scripts.quality.install_dvc`) and then runs two DVC E2Es. They are the setup E2E (`scripts.quality.run_dvc_e2e`) and the training baseline E2E (`scripts.training.run_dvc_baseline_e2e`). A change under `scripts/training/`, under `.dvc/` or to `.dvcignore` selects that group. Their reports are under `data/e2e/dvc_setup/` and `data/e2e/dvc_baseline/`. Both need network access to the package index for the pinned wheels. The setup E2E also downloads the wheels of the platform the runner is not. E2E reports include source hashes,
expected and observed process outcomes, repeat commands and tested limits.
GitHub Actions uploads the configuration, quality, broker and feature evidence and retains it for 14 days. Feature shards retain their reports under `data/e2e/quality/feature_shards/<suite>/shard_<index>/`. The combining job retains both combined reports and their input shard reports. Hook partitions retain their logs and report under `data/e2e/hooks/`. The secret store and gold setup reports stay on the runner and are not uploaded. Local TFLint
needs permission to start its bundled plugin. A sandbox denial is a blocked
check, not a passing result.

Before every commit, stage the intended files by name, then draft, complete and
retain the ignored local documentation review record (see Docs match the code above).

## Check group files

`scripts/run_ci.sh` is the dispatcher: the gates, timing, parts, scopes, the parallel unit runner and the exit trap. The commands of each check group are in
`scripts/ci/groups/<group>.sh`, one file for each group of `config/quality/ci_groups.json` (the gold layer's three units are in `lakehouse_gold.sh` and the
launcher E2E is in `launcher.sh`). The groups of the acquisition gate have no file because their commands are the scripts in `scripts/acquisition/`.

`scripts/run_ci.sh` also releases Docker. Its exit trap calls `release_docker` after the timing table. The function runs
`docker-release --owner --since <start of the run> --deadline 600`, so the launcher skips it unless a group created a container. It prints
`Docker release skipped: <reason>` and does nothing when `HAI_DOCKER_RELEASE=outer` (the pre-push gate releases once after all its steps),
`GITHUB_ACTIONS=true`, `uname` is not `Darwin`, `CI_LIST_ONLY=1` or the part is `gates`. A failed release prints one line and never changes
the exit status of the run.

- **Each group runs in its own shell process.** The dispatcher starts `bash --noprofile --norc` for every group, loads only `scripts/quality/timing.sh` (the
  `timed` wrapper) and that group's file and calls the entry function: the group name, `launcher_e2e` for the launcher and `lakehouse_gold_<unit>` for each
  gold unit. The process gets these variables beside the exported environment of the run and nothing else of the dispatcher: `python_bin`, `repo_root`,
  `scratch`, `quality_output`, `e2e_output` and `CI_TIMING_FILE` (`MODEL_METADATA_WORKERS`, `LAUNCHER_SHARD` and `TF_PLUGIN_CACHE_DIR` come from the exported
  environment). Give a group any other dispatcher value in `run_group` of `scripts/run_ci.sh`. A missing or unreadable file stops the run with
  the file and the group named.
- **What the separate shell isolates.** The shell of a group isolates shell state only: its functions (a redefined `timed`,
  a Bash builtin or the name of another group's function), its variables, its shell options and traps, its working directory, its exported functions and its
  exported environment. A change of any of these exists only in that group's process, so it cannot reach another group or the dispatcher.
- **What it does not isolate.** The groups share the host. Group files are reviewed code. A group can write files in the checkout
  or in the scratch folders. That includes the files of other groups and the timing table of the run. It can also signal the process group of the run. The guard
  checks the shape of the files and does not defend against these effects. Operating-system isolation (separate users, containers or read-only mounts) would be
  needed for that and is not part of this change.
- A body file is listed in its own group's path list and in no other list. An edit to it selects that group, the lakehouse layers downstream of it (a bronze file
  selects all four layers, a silver file selects silver, gold and features and a gold file selects gold and features) and `repository_code_checks`, which matches every `.sh` file and runs the guard. It selects no
  other group in the core and layer parts. The launcher list holds no other file of this folder (the computed list below never names them), but the launcher
  part selects every path while the always-select modules below are unreviewed, so it also runs for an edit to a body file.
- The dispatcher, the timing wrapper, the selector, `config/quality/ci_groups.json`, `.github/workflows/`, `.github/actions/`, `.pre-commit-config.yaml`, the
  requirements files and `pyproject.toml` still select every group. Other files under `.github/` (the pull request template) are documents and select no group of the core and layer parts. The launcher part still selects them (see "Launcher selection").
- The guard `scripts/quality/ci_group_files.py` runs in `repository_code_checks`. It fails with the file and the group named when a group has no body file or
  a body file belongs to no group, when a file lacks its entry function, when the dispatcher text declares a function of a group (continued lines, quotes,
  backslashes and `$'...'` escapes in names are removed first), when a body file is missing from its group's list or another list also matches it. It also requires every configured group to have a hosted job that runs its part through `scripts/run_ci.sh`. A missing feature matrix entry names the workflow and the uncovered feature group. Bash lists the
  functions: the guard sources each file alone in a clean shell (`env -i`, `--noprofile --norc`, an empty temporary folder, 30 seconds at most) and prints
  `declare -F`. A DEBUG trap stops that shell at the first command the file would run, so a body file holds function definitions and comments only and listing it
  runs none of its commands. The trap keeps the listing free of side effects. It is not a security boundary, because a file can shadow the commands of the trap. The guard no longer checks function names across files, builtin names, nested definitions or
  redirections, because each group runs in its own shell: their shell-state changes stay within that group. File writes and process
  signals remain shared, as described above.
- The guard needs Bash 3.2 (macOS `/bin/bash`) or later. The cases of `run_ci_groups_e2e` that plant drift in the body files run in the "Local CI Core" job of
  GitHub CI (`repository_code_checks`, command "CI groups selection E2E"), whose runner has Bash 5. The job log names the Bash version they used.
- To add a group, add its entry to `groups` in `config/quality/ci_groups.json` (the order of the file is the run order) with the pattern
  `^scripts/ci/groups/<group>\.sh$`. Add the file `scripts/ci/groups/<group>.sh` with a function named exactly like the group, because the dispatcher calls it by
  that name. The exceptions are the launcher (`launcher_e2e`), the gold layer (one function `lakehouse_gold_<unit>` for each unit in `gold_units` of the dispatcher) and features (`lakehouse_features_sources`, `lakehouse_features_columns` and `lakehouse_features_combine`). Helper functions are allowed. Add the group's expected selection to the body-file cases of `run_ci_groups_e2e` and its job to the workflow.
- `run_ci_groups_e2e` proves the selection of each body file, plants drift for every guard rule and runs the dispatcher with a `timed` that only prints the
  command. These dry runs prove that a missing body file stops a run, that the full local plan names the same groups, gold units and acquisition shards as the
  GitHub jobs and that each of 15 hostile group files leaves the shell state of the other groups and of the dispatcher unchanged. Before the move the plans (`CI_LIST_ONLY=1`) and command lists of every part
  and scope were captured and compared with the result.

## Launcher selection

The `launcher` group (the launcher E2E suite, `scripts/lakehouse/run_launcher_e2e.py`, about 14 minutes) runs only when a change can affect what that suite
runs. It no longer matches `^orchestration/`, `^scripts/`, `^config/`, `^services/`, `^data_contracts/` and `^dbt/` as a whole. The list is computed and
checked. Until the always-select modules below are reviewed one at a time, every path still selects the group (the narrowing applies once they are narrowed):

- `config/quality/launcher_inputs.json` holds the hand-written `entry_points`, `extra_inputs`, `ignored_reads` and `inventory_reads` and the computed `files`,
  `folders`, `dynamic_sites` and `inventory_sites`. Each `ignored_reads` entry names a module and a text, the reason the suite does not read it and the
  stripped source `lines` of the occurrences it covers. `--check` fails when those occurrences change. `--write` never updates them. An ignore removes
  only its own text: a file that another text of the module names stays listed. Each `inventory_reads` entry names a module that lists files instead of
  naming them (a `git ls-files` call, a `glob`, `iglob`, `rglob`, `iterdir`, `walk`, `scandir` or `listdir` call over any folder or a `find` or `ls` program),
  the path `pattern` of the files it reads (or none, with the reason) and why. `docker_calls.check`, which the suite runs, parses every Python file, so `\.py$`
  selects the group for any Python file, present or future. The pattern `.` selects the group for every path (see "Always-select modules").
- `scripts/quality/launcher_inputs.py` computes the list from the entry point: every module a Python file imports (also inside functions and in code held in
  strings), every module a string, an f-string or a shell, YAML or template file names, every tracked file a string, a `/` join (also one that starts at
  `ROOT`) or an f-string names (`scripts/quality/source_reads.py`, moved from the E2E runner) and the same for each file found. A folder that a module names is
  read whole: its files are listed, a new file in it selects the group (prefix in `folders`) and its own imports are not followed. A call that imports by a
  computed name cannot be followed; the number of such calls per file is in `dynamic_sites`. Run `--check` (default), `--write`, `--explain PATH` or
  `--trace FILE`.
- `scripts/quality/ci_groups.py` adds each `inventory_reads` pattern, one exact-path pattern per listed file and one prefix per folder to the group. The hand-written patterns are the group
  body, the tool and the list. `global` paths and the unclassified-path rule still select every group: the pattern `.` is kept out of the group lists (`launcher_always` in the loaded configuration), so it selects the launcher for every changed path and never makes an unlisted path look classified.
- A change to a module the suite imports or a file it reads selects the group. So does a change to any Python file. While the always-select modules below
  carry the pattern `.`, a change to any path also selects it, also an unrelated one (`infra/main.tf`, `config/quality/mypy_runtimes.json`, a document or the pull request template:
  the `launcher_always_selected_by_*` cases). A new import or read in a listed module changes the computed list, so `launcher_list_equals_computed` in `run_ci_groups_e2e` fails until
  `--write` is run and the diff is reviewed. The same E2E fails when a listed file is not selected by the group, when a pattern matches no tracked file and
  for planted defects in a small sample repository (stale entry, missing module, package, read file, module named in a text file, folder file or folder, a
  new dynamic import, a changed entry point, a missed file in a trace, an undeclared or stale inventory read, a new occurrence of an ignored text and an exact
  read beside an ignored folder). It also adds an exact read of `config/quality/mypy_runtimes.json` to the suite's `docker_calls_check` case for its own
  process only and requires the list check to name that file. It also adds a wrapped `rglob`, an `os.walk(top=ROOT)` and a `find` run by path to the same case and
  requires the list check to name the suite's changed inventory count. A sample module that calls a bare `listdir` is named as an undeclared inventory.
- **Always-select modules.** The inventory scan counts every enumeration call and listing command whatever its receiver: it cannot prove which folder a call
  lists or whether the result depends on file contents, so a counted site is an inventory of the whole repository. Owner decision 1 (project decision 15):
  every module the scan flags carries an `inventory_reads` entry with the pattern `.` and the reason "not reviewed", so any change selects the group, as it did
  before this selection existed (the suite itself has 21 sites). The entries are 34 modules with 87 sites:
  `orchestration/` `launcher` and `lineage`; `scripts/acquisition/` `batch`, `bls_api_contract`, `history_routes`, `redownload_controls`, `reuse_capture` and
  `s3_store`; `scripts/infrastructure/sts_stand_in`; `scripts/orchestration/` `in_image_checks`, `ledger` and `run_control`; and 22 modules of
  `scripts/lakehouse/` (`catalog_journal`, `dbt_package_archive`, `geography_file_periods`, `gold_checks`, `gold_publish`, `ownership_release_periods`,
  `package_fixture`, `pipeline_build`, `pos_file_periods`, `provenance`, `run_bronze_e2e`, `run_gold_e2e`, `run_gold_publish_cases`, `run_launcher_e2e`,
  `run_launcher_real_fixture`, `run_lock_e2e`, `run_pins_e2e`, `run_silver_e2e`, `run_staging_e2e`, `silver`, `silver_input` and `silver_quality`). With
  `docker_calls` (`\.py$`, 1 site) the list holds 35 modules and 88 sites. Narrow one module at a time: review its sites, replace `.` with a hand-written
  pattern (or `null` with the reason that the result does not depend on file contents) in a reviewed change and run `--write`. Do not narrow several at once.
- **Accepted limit (not built).** The inventory scan counts enumeration calls by name and shape. It misses an aliased import (`from os import listdir as entries; entries(".")`), a saved function (`lister = os.listdir`) and a shell inventory started through `os.popen("git ls-files")`. A followed module that lists files in one of these ways has no `inventory_reads` entry. Likelihood: low (no module uses these forms; the `.` entries select the group for every path today). Cost to close: 1 to 2 hours. Closing it means counting aliased and saved references and literal shell inventories, with the three forms as cases.
- Not provable by a static scan: a path built at run time from a variable, an environment variable or a glob over `ROOT`. Control: run the suite once under
  file-open tracing and check the trace with `--trace` (commands in the report of the change). Until that run is recorded, `extra_inputs` and every folder
  read whole stay in the list.

## Lakehouse layers

The lakehouse checks are four groups in `config/quality/ci_groups.json`: `lakehouse_bronze`, `lakehouse_silver`, `lakehouse_gold` and `lakehouse_features`, listed in
`layers` from upstream to downstream. Each group has its own path list. Bronze, silver and gold have one GitHub job each (`CI_PART=bronze`, `silver` or `gold`). Features have source and column shard jobs that both select the `lakehouse_features` group. The feature store's own gates have one more job (`CI_PART=feature_store`) that selects the same group. The bronze list is the
broad lakehouse list. It holds the code every layer shares: the launcher, memory plan, provenance, lock and compose file, the dbt project, macros and tests.
The silver, gold and feature lists name only the files that belong to that layer. Each file is listed for the layer that reads it first. No later layer's pattern
may match it. A file that an upstream layer's checks read although a later layer's list also matches it is named in `upstream_inputs` for the
earliest layer that reads it and belongs to that layer. The bronze entries are the gold macros and dimension models, the bronze staging models, the
silver build marker model, the saved suites and the features publication contract that the bronze storage, provenance and ECS stage runners read. The silver suite check reads the saved suites in `features/great_expectations`, the suites and baselines in `data_contracts/great_expectations`
(the gold suite folder included) and the gold contract, so those select silver, gold and features. A path that several lists match belongs to the most downstream
of them. A changed path selects its layer and every layer downstream of it. A bronze or shared change selects all four. A silver change selects silver, gold and features. A gold change selects gold and features. A change to a feature check module or to the feature store's own files selects features. Feature models, tests and recipe seeds still select gold and features because the gold model metadata and bridge checks read them. The gold list also names `config/training/targets.json`, `scripts/training/targets.py`, `scripts/training/run_targets_e2e.py` and `scripts/training/__init__.py`, because the training target check compares the target lists with the gold control bridge, so a change to the bridge, the lists or the check runs it. The training guard (`check` in `scripts/training/guards.py`) contains the same check. The `guard` action of the training job module (`python -m scripts.training.job guard`, `scripts/training/job.py`) calls it before any data step. `scripts.training.capstone_replication` fits without it. The data actions read the mounted extract and the outputs of earlier steps. Each input is checked against its manifest or completion record before use. Each action writes its outputs under temporary names before it writes its own completion record. A repeat of a finished action reads that record and changes nothing. A run folder that holds a file the action does not write is refused with `run_dir_not_clean`. `version_extract` copies each exported matrix into a scratch DVC project, runs `dvc add` and `dvc push` against the remote in `DVC_REMOTE_URL` and writes the metafile and a baseline record. `fit` fits the six models for each requested type after it repeats the target and earlier-outcome check against the gold control bridge. It writes `bundles/bundle_<type>/` as `data_contracts/training/bundle.json` describes. Before the model save it refuses a candidate that holds a class outside the library defaults and the `serialization.trusted_types` list of the model contract, with `untrusted_model_type`. `mediation` writes `mediation_<type>.json`. `hac_validate` ranks the selected model's holdout predictions against the HAC flags with the year map in `config/training/hac_year_mapping.json` and refuses with `hac_year_mapping_missing` when that reviewed file is absent. `log_run` logs one parent run and its child runs for each type to `MLFLOW_TRACKING_URI` as `data_contracts/training/model_contract.json` describes. It adds the DVC metafile as an artifact with its hash tag and the Iceberg snapshot IDs. It sets `bundle_sha256` last as the completion mark. A parent run without that mark is an interrupted attempt. The next try adopts it and each child by its step key, logs only what is missing and deletes no run. Each run and each model version is created once under a pending marker, so a lost response never leads to a second create. A marker whose create left nothing stops the step with `pending_create`. Two complete parent runs for one training run fail with `mlflow_duplicate_runs`. A deadline exits with code 4 and the name `deadline`. Every other refusal exits with code 1 and one JSON line that names the failure. `promotion_check` refuses any run whose population is not the `population` of `config/training/promotion.json` (`primary`) with `population_not_primary`, before it registers anything. It registers each type's candidate once as a model version of `hai_model_<type>`. It scores it and the `production` champion on the same newest-year holdout. It applies the owner's rule from `config/training/promotion.json` in `scripts/training/promotion.py`. It writes and rechecks `comparison_<type>.parquet` and `comparison_<type>.json` before it sets any version tag. A pass sets the alias `challenger`. It never moves the alias `production`. `promote` fails with the named failure `action_not_implemented`. `python -m scripts.training.run_job_action_checks --output <new_directory>` runs the synthetic cases of these actions with explicit stand-ins for Parquet, the model libraries and the MLflow client. The option `--case <name>` runs one of them. These cases do not verify native statistics, the MLflow server, the skops model format or the container mounts. The `smoke` action imports every action module. A job whose layer the range does not reach exits 0 and writes `skipped <group>: <reason>` to the job summary. The
gates never skip. `lakehouse_bronze` runs the platform checks (resource limits, run lock, provenance, storage single copy and contracts), `lakehouse_silver` runs the silver
suite check and `lakehouse_gold` runs the gold reference seeds, identity and label checks, the county-change seed check and its E2E, the training target lists check and its E2E, gold suites, the gold publish contract check, the gold and silver writer setup check against a stand-in Polaris, the model metadata check and the model metadata E2E, as three units (see Gold units below).

`lakehouse_features` runs two checks:

1. The feature-phase source check (`feature_sources --check` and its E2E `run_feature_sources_e2e`). It works in these steps:
   - A text pass runs first.
   - The check builds and binds every feature model and test on a DuckDB file. That file holds only the gold tables of the contract (without the raw bridge), the feature seeds, the live seeds and the filter results. A read of silver, bronze or a retired seed therefore fails however it is spelled.
   - The check reads the bridge in two stages. In the first stage the check builds 15 filter tables itself in DuckDB, each `bridge_measure_control` filtered to one `source_seed`. The 15 filter models, pinned to one template, must compile to that query. In the second stage the check builds and binds every other feature model and test on a database that holds the filter tables and no `bridge_measure_control`, so any other read of it fails.
   - The check binds or rejects every selected node by name. A hook, a macro that overrides dbt and a relation added or rewritten by the run fail. The check compares rows by digest.
   - Every feature model has an enforced column contract.
   - Before anything builds, the check applies a closed allow list. The check stops at the first admission problem. E2E cases record the process starts to prove that rejected code never ran. The list covers the Jinja constructs and calls, the dbt macros, the config keys and values, the YAML Jinja strings, the hooks, the `dbt_project.yml` keys that the feature build may use and the `ref()` targets that `dbt/models/features/_feature_inputs.json` pins for each model and test (a direct read is bound on the pinned relations only). It also pins the SHA-256 of the two run-hook macros and the project macros they reach. SQL outside the compiled query of a node therefore fails by content: `run_query`, `statement`, `adapter.execute`, `sql_header`, `persist_docs` and a changed hook body.
2. The feature column check (`feature_columns --check` and its E2E `run_feature_columns_e2e`). It requires every output column of a feature model to be documented with its data type and description. It compares the columns on empty gold tables without the dbt contract.

The `orchestration_checks` group also lists `scripts/lakehouse/`, `dbt/`, `data_contracts/lakehouse/` and `data_contracts/great_expectations/`, because its publish contract and pipeline
checks read them, so a change to those paths runs it beside the layers. Put a new check in the most downstream layer whose files it reads. `.venv/bin/python -m scripts.quality.run_ci_groups_e2e` checks selection for each layer and shared changes
and the real inputs. It also requires `configuration_checks` to start `scripts.infrastructure.run_secret_store_e2e` and `lakehouse_gold` to start
`scripts.lakehouse.run_gold_publish_e2e --mode writer_setup`. It also requires `lakehouse_features` to start its five feature store commands. The sources of those offline checks must select their group. It reads the source of the modules each layer's function starts (and of the gold checks the silver suite check calls and of the storage
metadata comparison the bronze storage runner calls) and requires every tracked file they name to select that layer. A name is a module-level
constant, a path string or a `/` join inside a function or an f-string whose placeholders are strings of the module. A folder that a function
only creates is not a read. A path built from a variable the scan cannot resolve is not seen, so a new reader of another layer's file needs an
`upstream_inputs` entry written by hand. It requires every file the runner copies to select
`repository_code_checks`. A check whose inputs no path list can name
completely belongs in `always` in `config/quality/ci_groups.json`.

`lakehouse_features` also runs the feature store's offline gates, as the item `lakehouse_features_store` (`CI_PART=feature_store`; the feature sources and columns suites run in their own shard items). Its path list also names the feature store's own files: `feature_store/`, `scripts/features/`, `data_contracts/features/`, `data_contracts/lakehouse/features_publish.json` and `services/feature_store/`; the publication contract is also a bronze upstream input (the ECS stage runtime cases read it), so a change to it selects every layer. The gates are the schema check (`scripts.features.feature_schema check`: the schema file, the matrix models, their YAML and `feature_store/definitions.py` are what the generator writes from the frozen control seeds and the reviewed rules), the static cases of `scripts.features.run_feature_store_e2e --mode static` (generator defects, publish contract, gold pin, publish plan, closed Feast settings, no `teardown`), the Feast job and historical helper against stub catalogs (`scripts.features.feast_cases --part local`), the online protocol against an in-process fake Redis (`scripts.features.online_cases --part fake`) and the light cases of the feature pipeline's renders, broker approval and DAG graphs (`scripts.orchestration.run_features_pipeline_light`). The matrix models stay under the gold list (`dbt/(models|macros|tests)/features/`), so a feature-model change selects gold and features. The builds (`run_feature_store_e2e --mode build_local` under `.tools/sqlfluff/bin/python`), the Spark publish cases, the Feast image and the real Redis service run in the heavy lane, not in this group. The Feast image's runtime is `feature_store` in `config/quality/dependency_runtimes.json` and `feature_store/definitions.py` is a pending leaf of `config/quality/mypy_runtimes.json`, because Feast is not installed on the host. The generated wide models are larger than SQLFluff's default 20000-byte limit, so `.sqlfluff` sets `large_file_skip_byte_limit = 0`: a skipped file would pass the style check without being read.

## Command timings

`scripts/quality/timing.sh` wraps every check command of `scripts/run_ci.sh` and the acquisition scripts in `timed <label> <command>`. The log gets
`[timing] start <label> at <UTC time>` and `[timing] end <label> at <UTC time>: <seconds> s, exit <code>` and the command keeps its own exit code. Each
command adds a row to a file of its own unit and when the run ends the rows of the gates, the launcher E2E and every unit are listed in the job
summary as a table of command, start, end, seconds and exit code. When a work batch contains one item, including a hosted lakehouse job or feature shard, its output streams to the job log while the item runs. The same output is retained in the per-item log and its duration remains in the summary. Batches with several parallel items retain separate logs and print them in order after every item finishes, so their output does not interleave. Feature-source and feature-column shards also print and flush a timing line when each case starts and ends. The times in each line are those of the case or command. A wrapped function holds one command or one pipeline, because a shell function that runs under `||` ignores
`set -e`. `scripts/quality/run_ci_groups_e2e.py` checks the wrapper, the failing and the interrupted command and the rows of parallel units.

## Measured durations

The longest hosted time of each job over six hosted runs and the job's timeout, in minutes. A timeout is between 1.5
and 2.6 times the measured time (at least 5 minutes). A job marked "estimate" has no hosted measurement yet. Resize it from its first complete hosted run. The
CI-groups E2E compares this table with `.github/workflows/ci.yml`. The seconds of every command are in the job summary of each run. The longest commands are the 258 cases of the model
metadata E2E (about 39 minutes one after another) and the 38 offline workflow tests of the acquisition gate (about 39 minutes in one process).

| Job | Measured (minutes) | Timeout (minutes) | Basis |
| --- | ---: | ---: | --- |
| CI Plan | 1 | 5 | estimate |
| Quick Gates of a Reused Result | 4 | 10 | estimate |
| Repository Hooks (privacy) | 5 | 12 | estimate, secret and data checks |
| Repository Hooks (documentation) | 12 | 30 | estimate, prose, Markdown and review checks |
| Repository Hooks (code) | 24 | 40 | estimate, 22 measured Oct 30 2026 for the remaining manual hooks plus about 2 for the seven linters |
| Repository Hooks (samples) | 10 | 20 | measured Oct 30 2026, the `test-checks` hook alone |
| Local CI Core | 26 | 45 | hosted, measured Oct 31 2026 |
| CI Reuse and Gate E2E | 7 | 12 | estimate, 98 seconds on the Mac (88 and 10) plus the setup and the gates |
| Lakehouse Bronze Checks | 8.9 | 20 | hosted |
| Lakehouse Silver Checks | 2.4 | 5 | hosted |
| Lakehouse Gold Checks | 11 | 25 | hosted, supplied 10 to 11 minute range |
| Lakehouse Features Checks | 8 | 15 | estimate, offline feature store gates |
| Feature Sources (Shard 1 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Sources (Shard 2 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Sources (Shard 3 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Sources (Shard 4 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Sources (Shard 5 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Sources (Shard 6 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Sources (Shard 7 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Sources (Shard 8 of 8) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Columns (Shard 1 of 4) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Columns (Shard 2 of 4) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Columns (Shard 3 of 4) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Columns (Shard 4 of 4) | 14 | 30 | estimate, case-list partition plus setup and gates |
| Feature Combined Evidence | 4 | 10 | estimate, setup and report validation |
| Lakehouse Gold Model Metadata E2E | 14 | 30 | estimate |
| Launcher E2E (Shard 1 of 2) | 8.4 | 20 | hosted |
| Launcher E2E (Shard 2 of 2) | 9 | 20 | hosted |
| Acquisition Checks (Shard 1 of 4) | 13 | 30 | estimate |
| Acquisition Checks (Shard 2 of 4) | 13 | 30 | estimate |
| Acquisition Checks (Shard 3 of 4) | 13 | 30 | estimate |
| Acquisition Checks (Shard 4 of 4) | 13 | 30 | estimate |
| Acquisition Checks | 4 | 10 | estimate |

The source runner has 157 cases, including 104 admission or resolved cases and one built-view checksum case. Eight strided shards have 19 or 20 cases each and distribute the build cases across all eight workers. The column runner has 12 cases. Four strided shards each have two documented-column cases and one extra-column, contract, admission or filter-hook case. A shard builds a base database only when its selected cases require one. The source check and column check each run once, in shard 1 of their suite. The counts are in `feature_shards` of `config/quality/ci_groups.json`. The 14-minute estimate allows repeated base setup and the shared gates. Resize it from the first complete hosted run.

## Feature shards and repository hooks

`run_feature_sources_e2e` and `run_feature_columns_e2e` accept `--shard K/N` (one-based) and `--list-cases`. The optional `--only` substring filter applies before striding. Listing cases starts no build. `config/quality/feature_cases.json` preserves the complete expected case inventory independently of the runners. Each shard report records the selected and executed cases, the full inventory and hashes of the feature implementation and dbt inputs. Inputs changing during a run fail it. `scripts.lakehouse.feature_shards combine` requires every numbered shard, the same candidate, successful outcomes and exact membership. Missing, repeated, failed or foreign cases fail the combining gate. The hosted gate requires both shard matrices to succeed and uses range selection to skip combining when the feature group is not selected. Its downloads are limited to this run and attempt. After a shard fails, choose **Re-run all jobs**. **Re-run failed jobs** does not recreate artifacts for the shards that passed in the earlier attempt, so the feature evidence job cannot pass. The acquisition combining job has the same full-rerun requirement.

The `hook_groups` mapping in `config/quality/ci_groups.json` partitions every manual-stage hook exactly once, except the separate `local_ci` aggregate. Privacy covers secrets, credential files, data files, environment declarations and the privacy scan. Documentation covers writing, front matter, Markdown, the local-record tracking check and documentation-review E2E. Samples holds `test-checks` alone, which takes about 10 minutes. Code holds all remaining manual hooks. `scripts.quality.ci_hooks` validates that partition before running any hook. It invokes each unchanged hook ID through the pinned pre-commit with its original range or all-files selection, manual stage and failure diff. It continues after ordinary hook failures and returns a failed group verdict. Each group deadline comes from its hosted job timeout with two minutes reserved for evidence upload. The first job step records `JOB_STARTED` before checkout. The hook runner subtracts elapsed checkout, setup and commit-check time from its budget. Privacy stops by minute 10 of its 12-minute job. Documentation stops by minute 28 of 30. Code stops by minute 38 of 40. An exhausted budget writes failed evidence without starting a hook. Local runs without `JOB_STARTED` count from the hook invocation with the same margin. A hook that exceeds the group deadline stops the partition, retains its partial stdout and stderr in the hook log and writes a failed report naming the hook and deadline. Full local CI runs the same three partitions in its parallel set. Commit-range messages and removed-name checks run in the hosted privacy partition, with the reused-run path retaining its own checks.

`run_ci_groups_e2e --sharding-only` runs light case-list, synthetic report combination, recording hook-entry and print-only dispatcher cases. It checks the timestamp from the first job step, budgets after setup and hook timeout evidence. Deadline cases cover local invocation and exhausted admission. Further cases check named failures for incomplete partitions after moving a hook between groups. Output cases check live output from one item, buffered parallel output and case timing with substituted feature builds. It runs no feature build and labels the report as iteration evidence. The full E2E also covers unconditional gates in both new feature parts. Planted cases prove that a missing shard, repeated case, missing hook and failed parallel item cannot produce a combined pass.

## Gold units

The gold layer runs as three units that share no scratch folder, report folder, port or process (each writes `data/e2e/<its own name>/<UTC
second>` and the gold publish modes add the mode to the folder name). `CI_UNITS` selects units. The default is all three.

- `checks`: the reference seeds, identity splits, source labels and county-change seed checks, the county-change seed E2E, the training targets check and its E2E
  (`scripts.training.targets --check` and `scripts.training.run_targets_e2e`, reports in `data/e2e/training_targets`), the gold suites
  check, the gold publish contract E2E, the catalog registration E2E and the model metadata check. The job summary of each run gives each command's time.
- `writer_setup`: the gold and silver writer setup E2E against a stand-in secret store. It has timing assertions (a lock wait of 0.3 seconds), so it
  runs alone: in the gold job before the other unit of that job and in a local run after the launcher E2E and before the concurrent units.
- `model_metadata_e2e`: the 258 cases of the E2E of the model metadata, each with a scratch repository and a pre-commit hook run. Run one after another they take about 39 minutes,
  so the cases run at the same time: `--workers auto` is the CPU count, at most 4 and `--workers 1` is the serial mode.
  `--prove-parallel-equal` runs the cases both ways on the same code and fails unless the outcomes are equal.
  Each hook or checker run has a deadline of `--case-seconds` (180 by default) and the pool has a total deadline that names the unfinished cases.
  GitHub CI runs it as its own job ("Lakehouse Gold Model Metadata E2E").

The jobs together take as long as the longest unit instead of the sum. The CI-groups E2E requires the gold units to hold their fifteen commands and the feature group to hold its four commands once each, to start no
launcher mode, to be covered once by the gold jobs and to run the unit that runs alone before the others.

## Sharded acquisition tests

In GitHub CI the acquisition pytest run is four jobs ("Acquisition Checks (Shard 1 of 4)" to "(Shard 4 of 4)") and a combining job ("Acquisition
Checks"). The local hook and `bash scripts/acquisition/run_checks.sh` stay one unsharded run with the same 90% gate.

- `scripts/acquisition/shards.py` gives every collected test to exactly one shard: tests in order of falling weight, each to the shard with the least
  weight so far. The weights are the seconds of the long tests in `config/acquisition/shard_weights.json`. A test without a weight counts 0.1 second. A wrong
  weight changes the balance only, never which tests run. Each shard writes a manifest (its tests, their outcomes, the hash of all collected test ids, the
  pytest exit status and the CI run) and its coverage data and enforces no coverage target. A shard whose pytest run, plugin or coverage data fails writes a failed
  manifest and its own job fails. Coverage leaves out `registry_additions_probe.py`, the changed copy of the registry validator that a boundary case loads
  from a temporary folder: coverage cannot read that copy after the folder is removed.
- The combining job runs Ruff, MyPy and the registry check, then the gate of `scripts/acquisition/shards.py`. It fails unless every shard job succeeded and
  every shard has a manifest of this run (same commit, run and attempt) that passed. The gate trusts no field that it can recompute: a manifest has exactly the fields
  of a manifest, every outcome is `passed` or `skipped` (a `skipped` outcome stands for a test that skips itself, such as the one that needs collected data), the counters
  equal the counters recomputed from the outcomes and every assigned test is reported once. It collects the tests itself. It requires the union of the shards' tests to
  be exactly that set and each shard to have run the tests that the gate's own assignment gives it. Each coverage file must match the hash in its manifest and measure the
  number of files that the manifest states. It merges the coverage files and enforces the 90% target of `config/acquisition/coverage.ini` on the merged data. One shard's
  coverage is never judged alone.
- A range that does not reach the acquisition checks makes each shard write a `skipped` manifest and the combining job accepts only a set of manifests that are all
  skipped. A mix of skipped and run shards fails.
- `.venv/bin/python -m scripts.acquisition.run_shards_e2e` checks the partition of the real tests, that two collections a few seconds apart give the same test ids (each shard and the gate collect on their own), the balance of the committed weights and, on a synthetic
  project, every failure of the gate (missing, failed, mixed, foreign and tampered shards, a contradictory outcome, counter, field or coverage claim in a manifest, a dropped
  test, swapped coverage data, a measured file in a removed folder and a merged coverage below the target). It also checks that every probe copy that acquisition code writes is
  left out of coverage. It also checks that the matrix, the shard argument, the job names, `ACQUISITION_SHARD_COUNT`, the
  default of the combine script, `config/quality/ci_jobs.json` and `shards.CI_SHARD_COUNT` state one shard count (the launcher shard job has one count of its own). The workflow must also never set `ACQUISITION_E2E_MODE`. `bash scripts/acquisition/run_checks_ci_e2e.sh` checks the skip path with `shards.CI_SHARD_COUNT` skipped manifests.

## Main-run reuse

The first job of the workflow, "CI Plan", runs `python3 -m scripts.quality.ci_reuse` with a read-only token. The run after a merge to `main` skips its jobs only
when all of these conditions hold:

- The push is to `main`.
- Exactly one merged pull request has the pushed commit as its merge commit.
- The tree of the pushed commit (from the API and from the checkout) is byte-identical to the tree of the head commit of that pull request.
- The latest completed pull request run of `ci.yml` for that head commit and head branch succeeded. Every job listed in `config/quality/ci_jobs.json` is present once and successful. No other job failed, cancelled or skipped (except the reuse job).

Any other result and any
error of the API runs every job. GitHub empties the `pull_requests` list of a run once its pull request is merged, so the run is matched by workflow path, event, head commit and
`head_branch` (equal to `head.ref` of the pull request) and not by pull request number. A run whose list names another pull request does not count. Reuse also stops when
the pull requests GitHub lists for the head commit include another one with that head. That list can be incomplete, so the match relies on every pull request
targeting `main`. A run of a pull request into another base selects its checks from that base. A push to `main` without a pull request runs in full. Pull request runs check out the head commit, so the tree that their jobs checked is the tree
compared. When the run is reused, the jobs of the workflow are skipped. "Quick Gates of a Reused Result" runs the commit-message and removed-names checks of the pushed range and
`CI_PART=gates` (the privacy, secret, credential, data-file, writing, Markdown, documentation and source gates). The plan job's summary names the pull request, the head commit, the
tree, the reused run and the conclusion of each required job. `.venv/bin/python -m scripts.quality.run_ci_reuse_e2e` checks the reuse and every case that must run in full.

## Reuse of local passes

A full local run of `scripts/run_ci.sh` (part `all`, no `CI_UNITS`, `LAUNCHER_SHARD` or `FEATURE_SHARD`) skips a check group when a full run on a commit of `origin/main` recorded a pass of that
group with the same inputs (`scripts/quality/ci_main_passes.py`). The inputs of a group are the files of the working tree, tracked or new and not ignored, whose change would select the group in
`scripts/quality/ci_groups.py`. A path that no group lists or that selects every group counts for every group. Every file counts for a full-only group, which has no path list. A reuse lookup that fails runs every group. The input hash covers each such file's path and Git blob hash. The run prints
`reused <group>: passed in a full run on main commit <commit> with identical inputs` and adds the line to the job summary.

A passed full-scope run records its groups only when the checked-out commit is in `origin/main` and the working tree has no change. The records are files in the Git common folder
(`hai_ci_main_passes/<group>.json`), shared by every worktree and never committed. A record is used only while its commit is still in `origin/main`. `repository_hooks` (it checks the pushed
range) is never recorded or reused (`reuse.never` in `config/quality/ci_groups.json`).

`HAI_CI_REUSE=off` runs every group. Hosted CI never reuses a local pass. The pre-push gate never reuses one: it sets `HAI_CI_REUSE=off`. The marker `HAI_DOCKER_RELEASE=outer`, which only the gate sets,
turns reuse off in `scripts/run_ci.sh` as well. `HAI_CI_PASSES_DIR` and `HAI_CI_MAIN_REF` replace the folder and the main reference for the E2E cases only.

## Practice

Keep edits to CI-wide files rare: `scripts/run_ci.sh` (the dispatcher), `scripts/quality/timing.sh`, `scripts/process.py`, `scripts/quality/ci_groups.py`,
`config/quality/ci_groups.json`, `.github/workflows/`, `.github/actions/`, `.pre-commit-config.yaml`, the requirements files and `pyproject.toml` select every group
in every scope, so each edit runs all the checks. Put a check command in the body file of its group (see Check group files) so that the edit selects only that group.

## Scratch worktrees in E2E runners

These runners add a scratch Git worktree of HEAD: `run_ci_groups_e2e` and `run_checks_ci_e2e.sh`. Each removes it on every exit path: a pass or a failed case, an exception, an interrupt or a deadline.
The Python runner adds the worktree through `scripts/quality/scratch_worktree.py`, which turns SIGTERM, SIGHUP and SIGALRM (the deadline wrapper's signal) into an exit that runs the removal.
The removal runs `git worktree remove --force` and `git worktree prune`. If the removal fails, the folder is deleted anyway and the run fails.
While the removal and its checks run, the helper and the shell runner hold SIGINT (Ctrl-C) and those three signals, also for the Git processes they start.
One that arrives then takes effect when the cleanup is done. The case `scratch_worktree_cleanup_holds_an_interrupt_until_it_is_done` of `run_ci_groups_e2e` checks it.
Only Git's own answers about the run's worktrees decide: `git worktree remove` failing for one of them, `git worktree prune` failing or the folder of one of
them still existing fails the run with the path named. The run resolves each path before adding the worktree, so a symbolic link in the path that
disappears later does not matter. It never reads Git's registry to decide, because several agents add and remove their own worktrees in the same
repository at once. The registered worktrees before and after are kept in the report only: a difference or a failed listing is a note.
`run_scratch_worktree_e2e` checks these rules for both runners with real worktrees of a throwaway repository, run by the repository checks group.
A SIGKILL cannot be handled. A worktree left by one shows in the next run's list as the starting state. Clear it with `git worktree remove --force` and `git worktree prune`.

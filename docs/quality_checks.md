---
title: Quality checks
description: Every repository check the pre-commit hooks and CI run, what each blocks and how to run it on its own.
last_updated: 2026-10-16
---

# Quality checks

- **Python lint and formatting:** Ruff checks every nonignored first-party Python source from the Git inventory,
  including `dags/`, `orchestration/` and service code. Both lint and formatting use the existing `pyproject.toml`
  settings. The `ruff` hook and the CI source gate use `.venv/bin/python -m scripts.quality.lint_sources ruff`.
  Paths remain separate arguments, including hidden files and names with spaces. Source symlinks are rejected.
- **Shell syntax:** the `shell-syntax` hook checks every nonignored shell source with its declared interpreter's
  syntax-only mode. Shell suffixes and declared shell scripts without a suffix are inventoried. Script bodies never run.
- **Language linters:** ShellCheck 0.11.0 checks tracked shell sources. actionlint 1.7.12 checks Actions workflows.
  It validates local `action.yml` metadata only through the workflow's `uses:` reference. Shell scripts and expressions
  inside composite steps are not linted. yamllint 1.38.0 checks tracked YAML, including `action.yml` and `.yamllint` itself and treats duplicate keys
  as errors. Hadolint 2.15.1 checks every tracked Dockerfile. gofmt and go vet use installed Go 1.27.1, with vet targeting
  Linux amd64 and arm64 for the Linux-only programs. No Go toolchain or module is downloaded. The native tool archives
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
- **Hadolint exceptions:** DL3022 is ignored only at the five `COPY --from=hai` instructions. The build-context check
  verifies their named context independently. DL3008 is ignored only for Spark's apt-install instruction: packages
  follow the Ubuntu archive on the build day. Dated snapshot pins are a lead-managed issue-register action for the next
  image build. Spark's final stage uses Bash with pipefail, keeping all three checksum pipelines. Its changed runtime
  behavior remains unverified until that image build. No Hadolint rule is ignored globally.
- **SQL style:** SQLFluff uses the pinned environment under `.tools/sqlfluff`. Active `dbt/models/`, `dbt/tests/`
  and `dbt/macros/` use DuckDB with the dbt templater. `services/analytics/resources.sql` uses raw DuckDB and the Polaris
  schema SQL uses raw PostgreSQL. `services/analytics/bronze.sql` is listed as excluded from SQLFluff: it is a DuckDB CLI
  init script whose LOAD, CREATE SECRET and ATTACH statements are outside the pinned grammar. Its source stays unchanged.
  The 6 SQL files of `tests/orchestration/fixtures/tiny` are listed as excluded too: they form the synthetic dbt project of
  the transform scheduler E2E, which builds and checks them.
  The 41 listed `features/` SQL files are deferred: they are saved for the feature phase
  and are not built by dbt, as described in `docs/feature_phase_saved.md`. They must be linted when that phase brings
  them into a dbt project. A tracked SQL file outside the active paths and explicit deferred or excluded lists fails.
  `.venv/bin/python -m scripts.quality.lint_sources sql` runs these profiles without executing SQL. dbt packages are
  a compilation prerequisite. Macro bodies are style-checked only through the models and tests that use them.
  A fresh local dbt parse supplies the macro call graph. Each project macro must have a caller recorded in the manifest:
  a model, singular or YAML generic test, project on-run-start or on-run-end hook or another macro. A recorded call does not
  prove that the caller executes. Hook-only and YAML-only macro bodies are not directly style-checked by this command.
  Unused macros and unreadable or inconsistent inventories fail. Invalid Jinja fails compilation.
  SQLFluff skips a file over its 20,000-byte limit with only a warning. The SQL lint fails when that happens to any
  actively linted model, test or service SQL file. A skipped macro file stays covered through its callers. The limit stays.
  No wrapper model or separate project is created.
  DuckDB dot-command lines in `resources.sql` are replaced with comments in temporary lint copies, preserving line numbers and source
  bytes. Only the two `getenv` SET lines in `services/analytics/resources.sql` ignore PRS because the pinned parser
  cannot parse that valid DuckDB syntax. A style error elsewhere still fails. Other parse errors remain failures.
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
  its review. Other tags do not change the inventory. The review reads local tags, so commit from a full clone
  that has the release tags. Draft the record with
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
The source lint cases also run through `scripts/quality/run_lint_coverage_e2e.py`, which retains complete positive and
negative hook logs under `data/e2e/lint_coverage/`. Its `--mutant` mode verifies that a dispatcher which does no checking
fails the same cases. The samples are synthetic. No script body, SQL statement or container runs in that suite.
The repository policy checks were run on the whole repository before they were enabled,
with no false positives, so they block from the start. The writing check blocked
only after the existing prose was corrected. Its dash and slash kinds block since the prose was corrected on
Oct 15 2026. Its semicolon kind warns until a whole-repository run has no semicolon finding. The unused code, dependency and file
checks blocked only after their findings were deleted or listed with a reason.

## Model metadata

`scripts/quality/model_metadata.py` reads the dbt YAML and SQL in every folder that `dbt/dbt_project.yml` lists for models, seeds, analyses, tests, macros and snapshots
(`.yml` and `.yaml`), the gold contract `data_contracts/lakehouse/gold_contract.json` and the bronze table map `config/lakehouse/bronze_tables.json`.
The installed `.tools/sqlfluff` environment compiles models through its dbt templater with a private in-memory `lint` profile. SQLFluff parses the compiled SQL.
Compilation uses the installed dbt packages, writes temporary artifacts and starts no build or container. It needs no catalog, credentials or network.
The `model-metadata` hook runs when a dbt YAML or SQL file, a macro, the contract, the table map or the check itself changes.

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
a Public source contributes no confidential origin, an explicit-list source protects only listed names and an all-column source protects every name.
An unknown model or CTE output retains the union of that relation's confidential output origins. With several candidate relations and no declared match,
an unknown name retains their combined confidential origins for that reference. Registering both a table name and its alias does not duplicate the relation.
A two-part reference without a local relation qualifier uses the first part as its struct-column name. Visible outer references remain unsupported.
Table functions in FROM, lateral sources, correlated subqueries, dotted function calls, positional column references and other unsupported SQL constructs require whole-model conservative handling.
A construct the check cannot trace propagates every named confidential column from its input models' outputs to every output.
That input set includes declared references and known relations found in parsed SQL. Compilation or SQL-parse failures retain all named confidential source columns.
A SQL analysis exception retains all named confidential source columns for the affected model and lets the worker continue checking other models.
Class-inheritance messages append the conservative reason when one applies. The lineage report also retains the type and text of caught analysis exceptions.
The lineage report names each such model and its reason. The compiler and parser worker has a 90-second deadline and reports an exceeded deadline with the wait duration. An unresolved compiled relation blocks the check.
Write the documented ingestion exception on every Confidential source, collection and table override.

`meta.additive_across` lists allowed aggregation axes, for example `[hospital]`. The check requires the list and does not restrict axis names.
`meta.additivity_note` carries a free-text restriction. Cost-report stocks require one selected report and period per hospital before summing across hospitals.
WONDER population sums hold one database fixed and exclude repeated-export and held rows.

The same module provides `scan-personal-data --manifest <manifest.json> --output <report.json> [--inventory <inventory.json>]`
for explicitly listed local CSV, JSON, JSONL, NDJSON or text exports.
The manifest has a `tables` list. Each entry names a `collection`, a `table` and a nonempty `files` list of export paths. Relative paths resolve from the manifest.
An optional `columns` list declares the table's schema names. Otherwise the first CSV file's header supplies them. JSON keys outside this list become `field_N`
in emitted paths, including lowercase keys that look like identifiers. Nested unknown keys are replaced too. Text exports use the fixed `line_text` column.
An inventory is a JSON object with a nonempty `tables` list of `collection` and `table` pairs. Duplicate pairs or a missing inventoried table make the scan incomplete.
Without `--inventory`, `complete` confirms only that the listed files were read. It does not prove that every table was listed.
Only listed files are scanned. Supply the full approved export inventory to assess a collection. Unsupported or unreadable files make the report incomplete.
The scanner cannot read Parquet, spreadsheets, PDFs, binary documents, archives or database table storage directly; they need a supported local text export.
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
runs. A change of passive paths that no group lists selects no conditional group. The `sql_style` group runs in every
full and range run (the run before a push and GitHub CI). On commits it runs when a listed SQL input changes:
`dbt/`, `.sqlfluff`, `features/` and `services/` SQL, the dbt package archive configuration and the SQLFluff pins and
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
quality evidence under `data/e2e/quality/`. E2E reports include source hashes,
expected and observed process outcomes, repeat commands and tested limits.
GitHub Actions retains these synthetic artifacts for 14 days. Local TFLint
needs permission to start its bundled plugin; a sandbox denial is a blocked
check, not a passing result.

Before every commit, stage the intended files by name, then draft, complete and
retain the ignored local documentation review record (see Docs match the code above).

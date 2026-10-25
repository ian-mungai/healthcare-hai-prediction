---
title: Quality checks
description: Every repository check the pre-commit hooks and CI run, what each blocks and how to run it on its own.
last_updated: 2026-10-25
---

# Quality checks

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
  `tests/fixtures/` for synthetic samples, in `dbt/seeds/` for the reviewed
  dbt seeds (file labels and checksums from the S3 manifests, no source values),
  and as the one named file
  `config/lakehouse/identity_classification.csv` (codes, classes and public
  source citations; a path that only starts with that name is blocked). Any file over 5 MB is blocked
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
run as child processes. They take about 20 minutes on this machine (the recorded profile estimate), so they run only
when acquisition code, configuration or their dependencies change: through the
`acquisition-checks` pre-commit hook, which is one unsharded run and in GitHub Actions, where the pytest run is two shard jobs
and a combining job (see Sharded acquisition tests below; LFS checkout, pinned Terraform). The CI jobs use the
hook's own path pattern through `scripts/acquisition/run_checks_ci.sh`, which also
matches the CI workflow; a manual run, a push with no earlier commit or rewritten
history runs every check. `bash scripts/acquisition/run_checks_ci_e2e.sh` tests
that gate. CI runs on every pull request, on every push to `main` and on a manual dispatch; a push to any other
branch runs no CI, because its pull request run checks the same commits. Each push to `main` gets its own run, so a newer
push never cancels the check of an earlier range. They need no AWS credentials and no
collected `data/`. Off macOS, the HUD workbook and WONDER suites substitute only the
macOS browser-download metadata (the "downloaded from" attribute and file creation
time); on a Mac the real reads run. When the private archive is absent (as in CI), the
registry-additions suite skips its single legacy-archive check and the code-version suite its archived-original
checks; both record the skips. Each gate run writes `run.json` beside the suite outcomes in `data/e2e/acquisition_checks/`
with the seconds of every suite and a hash of the acquisition code and configuration; a change during the run fails it. Every command of
the gate prints its start, end and seconds (see Command timings below).
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
For the two ONC sources, loader lineage `_object_key` and `_member_sha256` are exempt under A273. The exemption applies at the source boundary;
a publisher field renamed to either loader name retains its confidential origins.
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
scanning, repository checks, configuration, secret store and quality E2E, retained regression
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
GitHub CI runs the manual stage on every pull request and push to `main` as parallel
jobs after a quick plan job (see Main-run reuse below): the hooks without `local_ci` (file-based hooks check the changed files of
the range), then `scripts/run_ci.sh` as `CI_PART=core`, one job for each lakehouse
layer (`CI_PART=bronze`, `silver`, `gold` or `features`, see Lakehouse layers below), a job for the
gold model metadata E2E (`CI_PART=gold` with `CI_UNITS=model_metadata_e2e`), the launcher E2E in two shards
(`CI_PART=launcher`, `LAUNCHER_SHARD=K/2`) and the acquisition shards with their combining job, each for the groups the range
touches (`CI_SCOPE=range`). Every checkout of a pull request run is the head commit of the pull request, not GitHub's merge of the
head into the base, so the tree a pull request run checks is the tree that Main-run reuse compares. Without a usable range (a manual run, the first push
of a branch or rewritten history) every check runs. A job whose group the range does not reach
reports success and writes the reason to its job summary. Each job caches only what a
lock file or hash checks again before use (npm packages, Terraform providers,
the dbt package archive and the SQLFluff packages); the Python environment is
built fresh from `requirements.txt` without a pip cache. Each job's timeout is about
twice its measured time (see Measured durations below). Locally `scripts/run_ci.sh` runs every part the way the GitHub jobs do: the gates
first, then the launcher E2E, then the units that run alone (the gold writer set-up) and then the independent check groups and units, which run
concurrently: Local CI Core, the four lakehouse layers, the gold model metadata E2E with its workers and the four acquisition shards. After them the
combining step of the acquisition gate merges the four shard folders and enforces the 90% coverage gate. The full local CI is expected to take about 60 minutes. The launcher E2E takes about 13 minutes and the feature checks take about 40 minutes, with gates and setup added. This is an estimate until the first full local run on this layout measures it. A separate measured trial runs the
launcher E2E in the parallel set five times on this Mac (16 cores, 48 GiB). Only if all five runs pass does the launcher join the parallel set, which would remove the launcher time from the serial path. The exception to the run alone rule is recorded with the evidence. Until then the launcher stays alone, first. The commit hook
(`CI_SCOPE=staged`) always runs the groups the staged paths select and reuses no earlier result. The run prints each group's seconds and each command's
start, end and seconds (GitHub CI adds both to the job summary). In the full set gitleaks scans every outgoing source file instead of
the staged index. The acquisition checks
run in their own CI jobs and through their hook only when acquisition files
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
commit hook) or `range` with `RANGE_START` and `RANGE_END` (GitHub CI). `CI_LIST_ONLY=1` prints which groups would run
and why. It runs nothing. The groups and the paths they depend on are in `config/quality/ci_groups.json`. A path that
no group lists and that is not passive selects every applicable group. Markdown outside `dbt/`, anything under `docs/`,
`.gitignore` and `.gitattributes` are passive: they do not select every group, but a group whose list matches one still
runs. A change of passive paths that no group lists selects no conditional group. The `sql_style` group runs in every
full and range run (the run before a push and GitHub CI). On commits it runs when a listed SQL input changes:
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
quality evidence under `data/e2e/quality/`. The secret store E2E writes its report under `data/e2e/secret_store/` and the gold writer set-up
check under `data/e2e/gold_publish/`. E2E reports include source hashes,
expected and observed process outcomes, repeat commands and tested limits.
GitHub Actions uploads the configuration, quality, broker and feature evidence and retains it for 14 days. The feature job retains `data/e2e/feature_sources/` and `data/e2e/feature_columns/`. The secret store and gold set-up reports stay on the runner and are not uploaded. Local TFLint
needs permission to start its bundled plugin; a sandbox denial is a blocked
check, not a passing result.

Before every commit, stage the intended files by name, then draft, complete and
retain the ignored local documentation review record (see Docs match the code above).

## Check group files

`scripts/run_ci.sh` is the dispatcher: the gates, timing, parts, scopes, the parallel unit runner and the exit trap. The commands of each check group are in
`scripts/ci/groups/<group>.sh`, one file for each group of `config/quality/ci_groups.json` (the gold layer's three units are in `lakehouse_gold.sh` and the
launcher E2E is in `launcher.sh`). The groups of the acquisition gate have no file because their commands are the scripts in `scripts/acquisition/`.

- **Each group runs in its own shell process.** The dispatcher starts `bash --noprofile --norc` for every group, loads only `scripts/quality/timing.sh` (the
  `timed` wrapper) and that group's file and calls the entry function: the group name, `launcher_e2e` for the launcher and `lakehouse_gold_<unit>` for each
  gold unit. The process gets these variables beside the exported environment of the run and nothing else of the dispatcher: `python_bin`, `repo_root`,
  `scratch`, `quality_output`, `e2e_output` and `CI_TIMING_FILE` (`MODEL_METADATA_WORKERS`, `LAUNCHER_SHARD` and `TF_PLUGIN_CACHE_DIR` come from the exported
  environment). Give a group any other dispatcher value in `run_group` of `scripts/run_ci.sh`. A missing or unreadable file stops the run with
  the file and the group named.
- **What the separate shell isolates (owner decision, 23 October 2026).** The shell of a group isolates shell state only: its functions (a redefined `timed`,
  a Bash builtin or the name of another group's function), its variables, its shell options and traps, its working directory, its exported functions and its
  exported environment. A change of any of these exists only in that group's process, so it cannot reach another group or the dispatcher.
- **What it does not isolate (accepted by the owner).** The groups share the host. Group files are reviewed code. A group can write files in the checkout
  or in the scratch folders. That includes the files of other groups and the timing table of the run. It can also signal the process group of the run. The guard
  checks the shape of the files and does not defend against these effects. Operating-system isolation (separate users, containers or read-only mounts) would be
  needed for that and is not part of this change.
- A body file is listed in its own group's path list and in no other list. An edit to it selects that group, the lakehouse layers downstream of it (a bronze file
  selects all four layers, a silver file selects silver, gold and features and a gold file selects gold and features) and `repository_code_checks`, which matches every `.sh` file and runs the guard. It selects no
  other group. The launcher list leaves the folder out of its `scripts/` pattern for this reason.
- The dispatcher, the timing wrapper, the selector, `config/quality/ci_groups.json`, `.github/workflows/`, `.github/actions/`, `.pre-commit-config.yaml`, the
  requirements files and `pyproject.toml` still select every group. Other files under `.github/` (the pull request template) are documents and select no group.
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
  that name. The two exceptions are the launcher (`launcher_e2e`) and the gold layer (one function `lakehouse_gold_<unit>` for each unit in `gold_units` of the
  dispatcher). Helper functions are allowed. Add the group's expected selection to the body-file cases of `run_ci_groups_e2e` and its job to the workflow.
- `run_ci_groups_e2e` proves the selection of each body file, plants drift for every guard rule and runs the dispatcher with a `timed` that only prints the
  command. These dry runs prove that a missing body file stops a run, that the full local plan names the same groups, gold units and acquisition shards as the
  GitHub jobs and that each of 15 hostile group files leaves the shell state of the other groups and of the dispatcher unchanged. Before the move the plans (`CI_LIST_ONLY=1`) and command lists of every part
  and scope were captured and compared with the result.

## Lakehouse layers

The lakehouse checks are four groups in `config/quality/ci_groups.json`: `lakehouse_bronze`, `lakehouse_silver`, `lakehouse_gold` and `lakehouse_features`, listed in
`layers` from upstream to downstream. Each group has its own path list and its own GitHub job (`CI_PART=bronze`, `silver`, `gold` or `features`). The bronze list is the
broad lakehouse list. It holds the code every layer shares: the launcher, memory plan, provenance, lock and compose file, the dbt project, macros and tests.
The silver, gold and feature lists name only the files that belong to that layer. Each file is listed for the layer that reads it first. No later layer's pattern
may match it. A file that an upstream layer's checks read although a later layer's list also matches it is named in `upstream_inputs` for the
earliest layer that reads it and belongs to that layer. The bronze entries are the gold macros and dimension models, the bronze staging models, the
silver build marker model and the saved suites that the bronze storage and provenance runners read. The silver suite check reads the saved suites in `features/great_expectations`, the suites and baselines in `data_contracts/great_expectations`
(the gold suite folder included) and the gold contract, so those select silver, gold and features. A path that several lists match belongs to the most downstream
of them. A changed path selects its layer and every layer downstream of it. A bronze or shared change selects all four. A silver change selects silver, gold and features. A gold change selects gold and features. A change to a feature check module selects features. Feature models, tests and recipe seeds still select gold and features because the gold model metadata and bridge checks read them. A job whose layer the range does not reach exits 0 and writes `skipped <group>: <reason>` to the job summary. The
gates never skip. `lakehouse_bronze` runs the platform checks (resource limits, run lock, provenance, storage single copy and contracts), `lakehouse_silver` runs the silver
suite check and `lakehouse_gold` runs the gold reference seeds, identity and label checks, gold suites, the gold publish contract check, the gold and silver writer set-up check against a stand-in Polaris, the model metadata check and the model metadata E2E, as three units (see Gold units below).

`lakehouse_features` runs the feature-phase source check (`feature_sources --check` and its E2E `run_feature_sources_e2e`: a text pass, then a build and a bind of every feature model and test on a DuckDB file that holds only the gold tables of the contract (without the raw bridge), the feature seeds, the live seeds and the filter results, so a read of silver, bronze or a retired seed fails however it is spelled; the bridge is read in two stages: the check builds 15 filter tables itself in DuckDB, each `bridge_measure_control` filtered to one `source_seed` (the 15 filter models, pinned to one template, must compile to that query); every other feature model and test is built and bound on a database that holds the filter tables and no `bridge_measure_control`, so any other read of it fails; every selected node is bound or rejected by name; a hook, a macro that overrides dbt and a relation added or rewritten by the run fail (rows compared by digest); an enforced column contract on every feature model; before anything builds (the check stops at the first admission problem, with E2E cases that record the process starts to prove that rejected code never ran), a closed allow list of the Jinja constructs and calls, the dbt macros, the config keys and values, the YAML Jinja strings, the hooks, the `dbt_project.yml` keys that the feature build may use and the `ref()` targets that `dbt/models/features/_feature_inputs.json` pins for each model and test (a direct read is bound on the pinned relations only), plus the pinned SHA-256 of the two run-hook macros and the project macros they reach, so SQL outside the compiled query of a node fails by content: `run_query`, `statement`, `adapter.execute`, `sql_header`, `persist_docs`, a changed hook body), the feature column check (`feature_columns --check` and its E2E `run_feature_columns_e2e`: every output column of a feature model documented with its data type and description, compared on empty gold tables without the dbt contract).

The `orchestration_checks` group also lists `scripts/lakehouse/`, `dbt/`, `data_contracts/lakehouse/` and `data_contracts/great_expectations/`, because its publish contract and pipeline
checks read them, so a change to those paths runs it beside the layers. Put a new check in the most downstream layer whose files it reads. `.venv/bin/python -m scripts.quality.run_ci_groups_e2e` checks selection for each layer and shared changes
and the real inputs. It also requires `configuration_checks` to start `scripts.infrastructure.run_secret_store_e2e` and `lakehouse_gold` to start
`scripts.lakehouse.run_gold_publish_e2e --mode writer_setup`. The sources of those two offline checks must select their group. It reads the source of the modules each layer's function starts (and of the gold checks the silver suite check calls and of the storage
metadata comparison the bronze storage runner calls) and requires every tracked file they name to select that layer. A name is a module-level
constant, a path string or a `/` join inside a function or an f-string whose placeholders are strings of the module; a folder that a function
only creates is not a read. A path built from a variable the scan cannot resolve is not seen, so a new reader of another layer's file needs an
`upstream_inputs` entry written by hand. It requires every file the runner copies to select
`repository_code_checks`. A check whose inputs no path list can name
completely belongs in `always` in `config/quality/ci_groups.json`.

## Command timings

`scripts/quality/timing.sh` wraps every check command of `scripts/run_ci.sh` and the acquisition scripts in `timed <label> <command>`. The log gets
`[timing] start <label> at <UTC time>` and `[timing] end <label> at <UTC time>: <seconds> s, exit <code>` and the command keeps its own exit code. Each
command adds a row to a file of its own unit and when the run ends the rows of the gates, the launcher E2E and every unit are listed in the job
summary as a table of command, start, end, seconds and exit code. In a unit that runs in the background the log is shown when the unit has ended; the
times in it are those of the command. A wrapped function holds one command or one pipeline, because a shell function that runs under `||` ignores
`set -e`. `scripts/quality/run_ci_groups_e2e.py` checks the wrapper, the failing and the interrupted command and the rows of parallel units.

## Measured durations

The longest hosted time of each job over six hosted runs and the job's timeout, in minutes. A timeout is between 1.5
and 2.6 times the measured time (at least 5 minutes). A job marked "estimate" has no hosted measurement yet; resize it from its first complete hosted run. The
CI-groups E2E compares this table with `.github/workflows/ci.yml`. The seconds of every command are in the job summary of each run. The longest commands are the 258 cases of the model
metadata E2E (about 39 minutes one after another) and the 38 offline workflow tests of the acquisition gate (about 39 minutes in one process).

| Job | Measured (minutes) | Timeout (minutes) | Basis |
| --- | ---: | ---: | --- |
| CI Plan | 1 | 5 | estimate |
| Quick Gates of a Reused Result | 4 | 10 | estimate |
| Repository Hooks | 20.7 | 45 | hosted, with all hooks |
| Local CI Core | 14.1 | 30 | hosted |
| Lakehouse Bronze Checks | 8.9 | 20 | hosted |
| Lakehouse Silver Checks | 2.4 | 5 | hosted |
| Lakehouse Gold Checks | 30 | 60 | estimate |
| Lakehouse Features Checks | 130 | 260 | estimate, three times local command time plus setup and gates |
| Lakehouse Gold Model Metadata E2E | 14 | 30 | estimate |
| Launcher E2E (Shard 1 of 2) | 8.4 | 20 | hosted |
| Launcher E2E (Shard 2 of 2) | 9 | 20 | hosted |
| Acquisition Checks (Shard 1 of 4) | 13 | 30 | estimate |
| Acquisition Checks (Shard 2 of 4) | 13 | 30 | estimate |
| Acquisition Checks (Shard 3 of 4) | 13 | 30 | estimate |
| Acquisition Checks (Shard 4 of 4) | 13 | 30 | estimate |
| Acquisition Checks | 4 | 10 | estimate |

The four feature commands took 2,402 seconds on the Mac. The feature estimate assumes hosted execution is three times slower. It adds 10 minutes for checkout, setup and shared gates, giving about 130 minutes. The 260-minute limit is about twice that estimate. Resize it from the first complete hosted run. The gold job keeps its 60-minute limit.

## Gold units

The gold layer runs as three units that share no scratch folder, report folder, port or process (each writes `data/e2e/<its own name>/<UTC
second>` and the gold publish modes add the mode to the folder name). `CI_UNITS` selects units; the default is all three.

- `checks`: the reference seeds, identity splits and source labels checks, the gold suites
  check, the gold publish contract E2E and the model metadata check. The job summary of each run gives each command's time.
- `writer_setup`: the gold and silver writer set-up E2E against a stand-in secret store. It has timing assertions (a lock wait of 0.3 seconds), so it
  runs alone: in the gold job before the other unit of that job and in a local run after the launcher E2E and before the concurrent units.
- `model_metadata_e2e`: the 258 cases of the model metadata E2E, each with a scratch repository and a pre-commit hook run. Run one after another they take about 39 minutes,
  so the cases run at the same time: `--workers auto` is the CPU count, at most 4 and `--workers 1` is the serial mode.
  `--prove-parallel-equal` runs the cases both ways on the same code and fails unless the outcomes are equal.
  Each hook or checker run has a deadline of `--case-seconds` (180 by default) and the pool has a total deadline that names the unfinished cases.
  GitHub CI runs it as its own job ("Lakehouse Gold Model Metadata E2E").

The jobs together take as long as the longest unit instead of the sum. The CI-groups E2E requires the gold units to hold their ten commands and the feature group to hold its four commands once each, to start no
launcher mode, to be covered once by the gold jobs and to run the alone unit before the others.

## Sharded acquisition tests

In GitHub CI the acquisition pytest run is four jobs ("Acquisition Checks (Shard 1 of 4)" to "(Shard 4 of 4)") and a combining job ("Acquisition
Checks"). The local hook and `bash scripts/acquisition/run_checks.sh` stay one unsharded run with the same 90% gate.

- `scripts/acquisition/shards.py` gives every collected test to exactly one shard: tests in order of falling weight, each to the shard with the least
  weight so far. The weights are the seconds of the long tests in `config/acquisition/shard_weights.json`; a test without a weight counts 0.1 second. A wrong
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
when all of this holds: the push is to `main`; exactly one merged pull request has the pushed commit as its merge commit; the tree of the pushed commit (from the API and from the
checkout) is byte-identical to the tree of the head commit of that pull request; and the latest completed pull request run of `ci.yml` for that head commit succeeded,
with every job listed in `config/quality/ci_jobs.json` present once and successful and no other job failed, cancelled or skipped (except the reuse job). Any other result and any
error of the API, runs every job. A push to `main` without a pull request runs in full. Pull request runs check out the head commit, so the tree that their jobs checked is the tree
compared. When the run is reused, the jobs of the workflow are skipped; "Quick Gates of a Reused Result" runs the commit-message and removed-names checks of the pushed range and
`CI_PART=gates` (the privacy, secret, credential, data-file, writing, Markdown, documentation and source gates). The plan job's summary names the pull request, the head commit, the
tree, the reused run and the conclusion of each required job. `.venv/bin/python -m scripts.quality.run_ci_reuse_e2e` checks the reuse and every case that must run in full.

## Practice

Keep edits to CI-wide files rare: `scripts/run_ci.sh` (the dispatcher), `scripts/quality/timing.sh`, `scripts/process.py`, `scripts/quality/ci_groups.py`,
`config/quality/ci_groups.json`, `.github/workflows/`, `.github/actions/`, `.pre-commit-config.yaml`, the requirements files and `pyproject.toml` select every group
in every scope, so each edit runs all the checks. Put a check command in the body file of its group (see Check group files) so that the edit selects only that group.

## Scratch worktrees in E2E runners

These runners add a scratch Git worktree of HEAD: `run_ci_groups_e2e` and `run_checks_ci_e2e.sh`. Each removes it on every exit path: a pass or a failed case, an exception, an interrupt or a deadline.
The Python runner adds the worktree through `scripts/quality/scratch_worktree.py`, which turns SIGTERM, SIGHUP and SIGALRM (the deadline wrapper's signal) into an exit that runs the removal.
The removal runs `git worktree remove --force` and `git worktree prune`. If the removal fails, the folder is deleted instead. The report notes it.
While the removal and the check of the registered worktrees run, the helper and the shell runner hold SIGINT (Ctrl-C) and those three signals, also for the Git processes they start;
one that arrives then takes effect when the cleanup is done. The case `scratch_worktree_cleanup_holds_an_interrupt_until_it_is_done` of `run_ci_groups_e2e` checks it.
Each run lists the registered worktrees (`git worktree list --porcelain`) before and after. A difference fails the run with the paths named. The report keeps both lists.
A SIGKILL cannot be handled. A worktree left by one shows in the next run's list as the starting state. Clear it with `git worktree remove --force` and `git worktree prune`.

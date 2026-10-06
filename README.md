# healthcare-hai-prediction

Predicting hospital healthcare-associated infection rates from public CMS, Census, BLS and state data, built as a reproducible data engineering project on AWS.

Hospital-acquired infection prediction research for a data engineer. This fresh
repository builds on the capstone concept, not its implementation. Same-period
prediction is primary; forecasting is a later extension. Acquisition and S3
storage precede comprehensive schema review and tutorial drafting. Availability
does not establish clinical validity or model eligibility.

## Table of Contents

- [Security](#security)
- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Architecture](#architecture)
- [Data](#data)
- [Quality Checks](#quality-checks)
- [Deploy and Teardown](#deploy-and-teardown)
- [Repository Layout](#repository-layout)
- [Limitations](#limitations)
- [Contributing](#contributing)
- [License](#license)

## Security

Deployment configuration is Internal; credentials are Restricted and never
belong in tracked files. Settings come only from the ignored `.env`, described
by `.env.example`. API keys live in AWS Secrets Manager and are read only at run
time (see [Install](#install)). Every commit is scanned for secrets, credential
files and data files (see [Quality Checks](#quality-checks)). Report a suspected
secret or personal value in this repository to the repository owner privately.

## Background

The project asks whether a hospital's infection score can be predicted from
same-period public measures and why a simpler earlier model explained little
of the variation. Approved collection is complete and its code is committed
(`3af1abb`); acquisition closeout is pending the final publisher redownload (run 3)
and confirmed disposal of personal originals. Public sources are collected
through publisher APIs first, then download URLs, into private versioned S3
storage with a receipt, hash and version readback for every object. The bronze
layer of a local Apache Iceberg lakehouse then holds one copy of every stored data
file as published, with each row traced to its S3 object version and checksum. It
also lists every other stored copy. All 330 mapped tables are loaded and checked; a
table whose files were all removed for privacy left the map. A dbt staging layer in DuckDB
reads the HAI, cost report, IPPS, occupational-mix, Provider of Services, Hospital General
Information, ownership and Care Compare process and patient-experience tables. It keeps one row
per HAI and Care Compare measurement window, one row per hospital cost report, one Provider of
Services row per hospital and snapshot and one case-mix index per hospital and payment-rule year.
A hospital-year spine lines each hospital and calendar-year HAI window up with the Provider of
Services snapshot and the case-mix index from before the window. It also flags the model population.
Modeling and serving come later. The [Architecture](#architecture)
diagram shows what is built and what is planned.

## Install

Prerequisites: macOS on Apple silicon or Linux AMD64, Python 3.12, Git,
Terraform 1.16.1 and AWS CLI v2. Node.js 18 or later and Google Chrome render the
architecture diagram.
The lakehouse needs Docker Desktop with at least 24 GB of memory: the bronze,
dictionary and checksum jobs give Spark a 12 GB heap (`JOB_MEMORY` in
`scripts/lakehouse/session.py`).

Run from this repository's root with its existing Python 3.12 `.venv`.
Dependencies are shared across development and production in `requirements.txt`;
a requirement used only as a command carries `# tool: <use>` and one imported by
name carries `# dynamic: <where>`, so the dependency check reads them correctly.
Gitleaks, TFLint and trivy are installed only in ignored `.tools/`; versions and
publisher SHA-256 values are pinned in `config/quality_tools.json`. The markdownlint
hook's `markdownlint-cli2` is installed into `.tools/markdownlint-cli2` with `npm ci`
from the lockfile in `scripts/quality/markdownlint/`, which pins each package's
integrity hash; it needs Node.js 22 or later. The `sqlfluff` hook's SQLFluff and
its dbt templater are installed into `.tools/sqlfluff` from the hash-pinned
`scripts/quality/sqlfluff/requirements.txt`; the same command installs the dbt
packages from `dbt/package-lock.yml` into the ignored `data/analytics/dbt/dbt_packages`. The schema-review tools in
`scripts/review/` need the hash-pinned packages in `requirements-review.txt`,
installed into the ignored `.review_dependencies/` (Apple silicon only); the file's
header gives the command.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/quality/install_tools.py
.venv/bin/python -m scripts.quality.install_markdownlint
.venv/bin/python -m scripts.quality.install_sqlfluff
PRE_COMMIT_HOME="$PWD/.tools/pre_commit_cache" .venv/bin/python -m pre_commit install
cp .env.example .env
```

Fill every value in `.env`; the file stays out of Git. `AWS_PROFILE` is the
project profile, named `<PROJECT_NAME>_<ENVIRONMENT>` after its same-named IAM
user. IAM changes use a separate administrator profile that is never stored in
`.env`.

The lakehouse runs in Docker Compose (`docker-compose.yaml`):

- PostgreSQL and Apache Polaris 1.8.0 use digest-pinned images.
- The Spark 4.1.2 (Iceberg 1.11.0) and DuckDB 1.5.6 images are built from `services/`, with their Python packages hash-pinned.

The first catalog command pulls and builds them:

```sh
.venv/bin/python -m scripts.lakehouse.catalog up
```

`up` is idempotent. On first use it generates the catalog credentials into the ignored, owner-only
`data/lakehouse/secrets/` and never prints them. It creates the `hai_lakehouse` catalog, limited to `lakehouse/` in the
project bucket. `down` stops the containers and keeps the catalog database.

The native installer supports macOS ARM64 and Linux AMD64. Repeating a verified
installation preserves bytes and modification times; an unexpected existing
binary is rejected rather than overwritten. `--archive-directory` supports
offline installation from the exact pinned release filenames. Do not replace
the shared system tools to satisfy this project.

Named manual steps. These are the only setup steps no script performs:

1. **API keys.** Request the Census and BLS API keys from their publishers and
   store each in AWS Secrets Manager, in the configured region, as a secret named
   `census_api_key` or `bls_api_key` holding JSON with one field, `api_key`.
   The keys are shared by several projects, so they are created by hand outside
   Terraform. Store the HUD USPS crosswalk token separately as `hud_api_key`,
   in the same JSON shape with the token alone in `api_key` (no `Bearer`
   prefix; the collector adds the header). The IAM
   configuration grants read access to these three names and never reads a
   value; each permission update requires a reviewed saved plan before apply.
2. **Administrator profile.** Configure an AWS CLI profile with IAM permissions
   for the IAM stack. It is passed on the command line, never stored in `.env`.

## Usage

Run the local CI, which the pre-commit hooks and GitHub Actions also run, without AWS credentials:

```sh
env PYTHON_BIN="$PWD/.venv/bin/python" \
  TF_PLUGIN_CACHE_DIR="$PWD/infra/.terraform/providers" \
  /bin/bash scripts/run_ci.sh
```

Each CI run retains configuration evidence under `data/e2e/configuration/` and
quality evidence under `data/e2e/quality/`. E2E reports include source hashes,
expected and observed process outcomes, repeat commands and tested limits.
GitHub Actions retains these synthetic artifacts for 14 days. Local TFLint
needs permission to start its bundled plugin; a sandbox denial is a blocked
check, not a passing result.

The E2E runners can also run on their own; each needs a new output directory:

```sh
.venv/bin/python -m scripts.infrastructure.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_checks_e2e
.venv/bin/python -m scripts.quality.run_documentation_review_e2e
```

The lakehouse jobs run in the Spark container. The bronze E2E uses a throwaway local catalog and synthetic files, never
S3. Each load reads only the S3 manifests and the committed table map (`config/lakehouse/bronze_tables.json`), loads one
copy of each stored file (the smallest object key), lists every copy in `bronze.stored_copies`, replaces each object in
its own commit, prunes objects a table no longer selects, rebuilds the table's data dictionary and writes a counts-only
report. A corrected manifest replaces the manifest it names as superseded; tables taken out of the map are listed in
`config/lakehouse/removed_tables.json` and dropped with `--drop-removed`:

```sh
.venv/bin/python -m scripts.lakehouse.catalog job bronze_e2e        # report: data/e2e/bronze/
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --group hai           # or --tables a,b; report: data/e2e/bronze_load/
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --tables a,b --copies-only   # list copies, prune, verify
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --drop-removed       # drop tables in removed_tables.json
.venv/bin/python -m scripts.lakehouse.catalog job dictionary -- --tables a,b      # rebuild bronze_dictionary tables
.venv/bin/python -m scripts.lakehouse.care_compare_tables           # regenerate the care_compare group (read-only S3)
.venv/bin/python -m scripts.lakehouse.retired_objects --collection <publisher/collection>   # rebuild the retired list
scripts/lakehouse/query.sh                                          # DuckDB shell, bronze attached read-only
scripts/lakehouse/ui.sh                                             # DuckDB UI at http://localhost:4213
```

The dbt staging layer (`dbt/`, dbt-core with dbt-duckdb in the analytics image) reads bronze read-only. Its E2E builds
the models on synthetic fixtures, including cases that must fail one named test, then on the real bronze tables twice,
and reconciles each staging table with bronze. Where a text file and a workbook hold the same IPPS or occupational-mix
table, staging reads the text file and drops only the workbook sheets that a selected text file of the same release
matches row for row; every other sheet stays selected (`stg_bronze__sheet_selection`). `scripts.lakehouse.ipps_file_labels` regenerates the IPPS and
occupational-mix label and twin seeds from the S3 manifests; `--check` confirms the committed seeds. `int_pos_hospital_snapshots`
types the Provider of Services hospital rows; each file is dated by the catalog coverage in `dbt/seeds/pos_file_periods.csv`,
which `scripts.lakehouse.pos_file_periods` rebuilds from the S3 manifests and the local acquisition job plans. The case-mix index
models read the unadjusted CMI by exact header name (`dbt/seeds/cmi_layout_columns.csv`), keep the transfer-adjusted CMI in its
own column and choose one CMI per hospital and rule year from the year's best rule stage; disagreeing files are held in
`int_cmi_holds`. `int_hospital_spine` has one row per hospital and calendar-year HAI window, as of the window start: the
Provider of Services snapshot that ends in the 12 months before the window and the CMI of the fiscal year that ends before
it (`int_cmi_hospital_data_years`), with the primary and sensitivity population flags. The Care Compare timely and effective
care, maternal health and HCAHPS tables get the same window models as HAI (`int_cc_*_windows`, holds in
`int_cc_window_holds`); `int_registry_measure_windows` carries each registry measure of those families by the exact measure ID
in `dbt/seeds/registry_measure_sources.csv`. `int_hgi_hospital_releases` types Hospital General Information per file.
`int_cost_reports` has one row per hospital cost report, in the federal fiscal year in which its period starts, with the
number of reports of its CCN in that year; `int_cost_report_measures` computes each registry cost-report measure by the
columns and rule in `dbt/seeds/cost_report_measures.csv` and gives null for a zero or missing denominator. The dbt packages
(dbt-project-evaluator 1.4.0 and its dbt_utils 1.4.1) are declared in `dbt/packages.yml` and pinned by version in `dbt/package-lock.yml`; dbt Hub
publishes no content checksum. They install outside the read-only project: `scripts/lakehouse/dbt.sh deps` installs them in the
container. The staging E2E installs them before it builds. dbt-project-evaluator is off in every other run and runs
only on request, on the in-memory `lint` target with no catalog; its findings are warnings:

```sh
.venv/bin/python -m scripts.lakehouse.run_staging_e2e              # fixtures; --real adds the real tables; report: data/e2e/staging/
scripts/lakehouse/dbt.sh deps                                       # install the locked dbt packages (once)
scripts/lakehouse/dbt.sh build                                      # dbt on the real bronze tables
.venv/bin/python -m scripts.lakehouse.run_project_evaluator         # dbt-project-evaluator; report: data/e2e/dbt_evaluator/
.venv/bin/python -m scripts.lakehouse.ipps_file_labels --check      # seeds match the S3 manifests (read-only S3)
.venv/bin/python -m scripts.lakehouse.pos_file_periods --check      # POS periods match the manifests and job plans
```

Load large groups in batches of about 20 to 40 tables: each job stops after 1 hour (`COMPOSE_TIMEOUT` in
`scripts/lakehouse/catalog.py`). A rerun of the same tables ends in the same state.

Before every commit, stage the intended files by name, then draft, complete and
retain the ignored local documentation review record (see [Quality Checks](#quality-checks)).

## Architecture

![Architecture diagram](docs/architecture/architecture.png)

The diagram source is `docs/architecture/architecture.json`, drawn with
[Archify](https://github.com/tt-a1i/archify) v3.0.1. Each component cites the
lines of code or configuration it was checked against, at the commit named in
the JSON. Its `finalize` command validates the JSON, writes
`docs/architecture/architecture.html` and checks it in Chrome; the PNG is
rendered from the HTML with headless Chrome. Planned components are listed in
the "Planned (not built)" card. With Archify unpacked at `<archify>`:

```sh
ARCHIFY_UPDATE_CHECK_DISABLED=1 node <archify>/bin/archify.mjs finalize architecture \
  docs/architecture/architecture.json docs/architecture/architecture.html --repo-root . --quality showcase
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --hide-scrollbars \
  --window-size=1600,1120 --virtual-time-budget=5000 \
  --screenshot=docs/architecture/architecture.png "file://$PWD/docs/architecture/architecture.html"
```

`finalize` also writes receipt files beside the HTML; keep them out of Git.

## Data

Published hospital-level datasets are public reference data subject to their
source terms. Public availability does not remove personal-data safeguards.
Any specifically approved source containing personal names or contact details
is classified as Confidential and requires a documented ingestion exception.

For the approved HCAI exception, the untouched download stays local with
owner-only access. Only the privacy-filtered derivative and non-personal
references or provenance may enter the project's private S3 storage. The
derivative remains under privacy and modeling review; redaction does not
establish formal anonymization or clinical validity. Raw retention duration
remains unresolved. FileVault was verified at commit `a1b758a`; that host check
does not establish compliance or resolve retention requirements.

The data bucket carries its own `DataClassification` tag of Confidential
(project decision, commit `c143cbc`). Redacted derivatives are not formally
anonymized. Hospital chief executives in HCAI annual financial files and state
officials' work contacts in a CMS contacts table are treated as Confidential:
originals stay local and only redacted copies belong in S3. The initial 218
Excel/contact versions were removed before commit `f9ef312`. Redacted replacements for
75 further versions are stored; those originals were deleted and verified before
that commit. Retirement records cover all 293 removed versions, storage
reconciliation passed and the temporary deletion grant was removed.
Operational evidence stays in ignored local
folders; this status does not clear privacy or modeling holds. The project no
longer uses the HRSA Area Health Resources Files: their license limits sharing
the data with third parties, so the five measures built on them were dropped and
all 399 stored versions were deleted and verified before commit `f9ef312`.

Tests use generic synthetic data. Real personal values must never appear in
fixtures, logs, tutorials, published artifacts or commits. Acquisition inputs,
operational evidence and the detailed exception record remain untracked.

The explicitly approved live S3 verification tool is the sole opt-in exception
to synthetic-only verification. Normal CI does not run it or contact AWS.

**Bronze lakehouse.** Bronze loads one copy of every stored data file as published, every value as text:

- CSV files keep their columns.
- Text files keep one row per line.
- Excel files keep one row per sheet row.
- PDFs and Word documents keep their table rows and text.

Every row carries its source, snapshot, S3 key, version and SHA-256. `bronze.stored_copies` lists every stored copy of
each file with its lineage and whether it is loaded or retired. Each table's `bronze_dictionary` entry lists its
columns with counts and the publisher's description or published type where a stored dictionary gives one. CMS Care
Compare dictionaries give types only. Bronze does not load:

- objects removed from S3, which the manifests still list (`config/lakehouse/retired_objects.json`, keys, versions and
  checksums only): the privacy deletion and, on Oct 4 2026, 2,193 byte-identical duplicate versions, each with a kept
  twin verified first;
- other copies of a file already loaded, which `bronze.stored_copies` lists;
- a Care Compare contacts file that names state staff;
- audit copies, receipts and Office package parts.

Two HRSA detail files, stored under the dictionary role, have corrected manifests in S3 that give them the data role
(`config/acquisition/manifest_corrections.json`, written by `scripts.acquisition.correct_manifest_roles`); the original
manifests stay unchanged. Open gaps are tracked in the local issue register.

[docs/data_collection.md](docs/data_collection.md) explains how every source was
collected and how to recheck it, including the named manual data steps: terms
acceptance, API accounts and the browser downloads (CMS Mapping Medicare
Disparities exports, Census table exports, CDC WONDER county mortality exports
and the 2010–2020 HUD ZIP-to-county workbooks). Its commands need the acquisition code in `scripts/acquisition/`
and `config/acquisition/`, tracked in Git since commit `3af1abb`. Registry revision 2 preserves exact legacy fingerprints for historic replay;
its private legacy archive stays outside Git. The bounded publisher redownload (run 3) remains the final verification.

## Quality Checks

CI checks Ruff formatting and security rules, MyPy, outgoing-source secret
scanning, repository checks, configuration and quality E2E, retained regression
safeguards, Terraform mock tests, TFLint, Checkov and `trivy config` (its
embedded checks only, run offline). Pre-commit scans the actual staged index,
including changes hidden by a clean working copy, before local CI. The source
scan excludes ignored acquisition data; neither scan sends files to an external
service. Secret values and matching source fragments are omitted from scanner
diagnostics.

Pre-commit (`pre-commit` and `commit-msg` stages) and `scripts/run_ci.sh`
enforce these repository rules at the pinned versions. The full set
(`.venv/bin/pre-commit run --all-files --hook-stage manual`) runs before a
release or push. GitHub CI runs that same command on every push and pull
request. In the full set gitleaks scans every outgoing source file instead of
the staged index. The acquisition checks run in their own CI job and through
their hook only when acquisition files change; run them directly before a release
with `bash scripts/acquisition/run_checks.sh`.

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
  tests are listed there. Before publishing, also scan ignored and hidden files
  with `.venv/bin/python -m scripts.quality.repo_checks privacy-scan --all`
  (`--warn` reports without failing).
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
  agent/vendor list, so new forms need a failing sample before the check is
  extended. Review pull request text separately; a Git hook cannot read it.
- **Docs match the code:** every environment variable the code reads is listed
  in `.env.example` and every variable listed there is read by code, a script,
  a workflow or a template. Local commits require an ignored `.documentation_review.json`, a per-document
  review outcome bound to the staged snapshot. Draft it with
  `.venv/bin/python -m scripts.quality.documentation_review prepare --reviewer <name> --reviewed-at <UTC>`
  after staging, review every document and fill each outcome and note locally. Never stage
  this record. The local hook validates its freshness against the index and rejects
  tracked evidence. GitHub Actions checks that the record is untracked and runs
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
  file name, folder or import; tflint (`terraform_unused_declarations`, every
  module) reports unused Terraform declarations. vulture and deptry are pinned
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
  `--check-untyped-defs` and `--disallow-untyped-defs`. The only allowed
  suppression is Ruff `S603` in `scripts/process.py`, the one process launcher;
  other scripts start programs through it and run as modules
  (`.venv/bin/python -m scripts.quality.scan_secrets`). Tests use
  `tests/support.py` `check()` instead of `assert`.

**Acquisition checks** (`bash scripts/acquisition/run_checks.sh`) run Ruff, MyPy,
registry validation, the acquisition regression tests and every offline E2E
suite twice, with a 90% coverage gate that also counts collector command lines
run as child processes. They take 12–14 minutes (up to 29 in CI), so they run only
when acquisition code, configuration or their dependencies change: through the
`acquisition-checks` pre-commit hook and in a separate GitHub Actions "Acquisition
Checks" job (30-minute limit, LFS checkout, pinned Terraform). The CI job uses the
hook's own path pattern through `scripts/acquisition/run_checks_ci.sh`, which also
matches the CI workflow; a manual run, the first push of a branch or rewritten
history runs every check. `bash scripts/acquisition/run_checks_ci_e2e.sh` tests
that gate. Each push to `main` gets its own CI run, so a newer push never cancels
the check of an earlier range. They need no AWS credentials and no
collected `data/`. Off macOS, the HUD workbook and WONDER suites substitute only the
macOS browser-download metadata (the "downloaded from" attribute and file creation
time); on a Mac the real reads run. When the private archive is absent (as in CI), the
registry-additions suite skips its single legacy-archive check and records the skip. The coverage gate leaves out
the tools in `scripts/acquisition/one_off/`, which ran once and are evidenced by their run records; a
gate test loads every committed collector plan through its contract; it needs the collected `data/`, so CI
skips it.

Each check has a bad and a good sample run through the real `pre-commit` entry
point (`scripts/quality/run_checks_e2e.py` and
`scripts/quality/run_documentation_review_e2e.py`); both run in local CI.
Every detection check was run on the whole repository before it was enabled,
with no false positives, so they block from the start. The writing check blocked
only after the existing prose was corrected. The unused code, dependency and file
checks blocked only after their findings were deleted or listed with a reason. There are no bypasses:
never use `SKIP=` or `git commit --no-verify`. If a check blocks a valid change,
fix the check or ask the repository owner.

## Deploy and Teardown

`scripts/infrastructure/terraform.sh` renders Terraform inputs from `.env`,
verifies the project identity and runs Terraform with the named profile. Apply
only a reviewed saved plan:

```sh
bash scripts/infrastructure/terraform.sh plan -out=<plan_file>
bash scripts/infrastructure/terraform.sh apply <plan_file>
bash scripts/infrastructure/terraform.sh --iam --admin-profile <administrator_profile> plan -out=<plan_file>
bash scripts/infrastructure/terraform.sh --iam --admin-profile <administrator_profile> apply <plan_file>
```

Terraform state stays local in each stack folder and is never committed.
Remote state protection remains deferred until the end of the project as
requested.

**Teardown.** The data bucket is the project's persistent store of immutable
source data, not a demo stack, so it has no destructive teardown switch
(project exception, commit `c143cbc`). `force_destroy` is false and
`prevent_destroy` is set; removing the bucket would need a separately reviewed
change that lifts both protections first. The bucket keeps its established name
rather than the `<project>-<component>-<environment>` pattern, because renaming
it would mean moving all stored data (project exception, commit `c143cbc`).

**Environments.** Both stacks tag resources with `Environment = dev`. A `prod`
environment and an `infra/modules/` layout are deferred until the next
infrastructure component is added (project decision, commit `c143cbc`).

**Lifecycle rules.** The bucket-wide rule, applied and read back from S3 at
commit `a1b758a`, aborts incomplete multipart uploads seven days after
initiation, with no expiration or transition of completed objects, historical
versions or delete markers. One exception covers only `lakehouse/`, where
Iceberg replaces and deletes its own table files: replaced versions there expire
30 days after they are replaced and orphaned delete markers are removed; current
lakehouse files and everything outside `lakehouse/` never expire (owner decision). Local mock-plan tests, source-shape checks and live
configuration readback pass. The seven-day scheduler itself has not been
observed in a timed test and no deliberately incomplete upload was created for
verification.

Pre-deployment inspection confirmed no existing lifecycle configuration. The
separately approved IAM plan added only bucket-scoped
`s3:PutLifecycleConfiguration`; the approved storage plan created only the
cleanup rule. IAM administration was used only for the policy update, while
storage deployment used the project profile. This permission can change the
bucket's entire lifecycle configuration, so future changes still require full
plan review and preservation of existing rules. Private plans and live
verification evidence remain untracked under
`data/infrastructure/lifecycle_deployment/`.

**Deferred controls.** Apply the project's engineering requirements when
development reaches the relevant stage. Future requirements guide later work;
deferral does not mean a control is implemented. The following
acquisition-stage deferrals are approved for `aws_s3_bucket.data` only. Checkov
and trivy evidence retain each exception, resource and reason.

| Deferred control | Reason | Review trigger |
| --- | --- | --- |
| Event notifications (`CKV2_AWS_62`) | No event consumer exists. | Implementing an event consumer. |
| Cross-region replication (`CKV_AWS_144`) | Recovery objectives and a destination are not established. | Recovery design, before production. |
| KMS (`CKV_AWS_145`, trivy `AWS-0132`) | Acquisition uses SSE-S3/AES256. | Coordinated ingestion and IAM migration, before production. |
| Access logging (`CKV_AWS_18`, trivy `AWS-0089`) | Approved acquisition-stage deferral of a separate logging destination; request-audit gap accepted for this stage. | Before additional user or service access, production or newly approved sensitive-data use. |

Access logging was explicitly deferred at commit `a1b758a`. Acquisition
receipts establish pipeline provenance, not an independent record of every
bucket request. Review CloudTrail data events alongside server access logging
when a review trigger is reached. Server access logs are best-effort, not a
complete audit trail ([AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html)).

Existing AES256 encryption, private access, versioning and destruction
protection are preserved. Synthetic E2E checks verify that the four exceptions
do not suppress other resources or the versioning control. Passing static
checks with these exceptions does not establish that deferred controls exist.
No AWS changes implementing these four deferred controls have been applied.

## Repository Layout

- `infra/`: private, versioned S3 configuration; `infra/iam/`: scoped service policies.
- `scripts/process.py`: the one process launcher every script uses to start programs.
- `scripts/infrastructure/`: dotenv rendering, identity guards and configuration E2E.
- `scripts/quality/`: pinned tool installation, secret scanning, repository checks, documentation review and Terraform analysis.
- `scripts/run_ci.sh`: the local CI script; the `local_ci` hook runs it on every commit and in GitHub CI.
- `tests/`: retained pytest regression safeguards and their `check()` helper.
- `config/quality_tools.json`: pinned native tool versions and publisher hashes.
- `config/quality/dependency_runtimes.json`: each runtime's requirements files and code, for the dependency check.
- `docs/architecture/`: the architecture diagram source (`architecture.json`), the HTML that Archify renders from it and the PNG.
- `dbt/`, `.sqlfluff`: the dbt staging project (sources, staging and intermediate models, seeds, tests and the locked
  packages) and its SQL style.
- `scripts/review/`, `requirements-review.txt`: the schema-review tools and their hash-pinned packages.
- `docs/data_collection.md`: how the source data was collected and how to recheck it.
- `docs/issue_register.md`: the project's single issue register, kept local-only (Git-ignored).
- `docs/project_guide.md`: the project's target design with each section's build status, kept local-only (Git-ignored).
- `.github/`: the CI workflow and the pull request template.
- `scripts/lakehouse/`, `config/lakehouse/`: the catalog script, the bronze loader, file readers, dictionary and
  checksum jobs, the Care Compare, retired-object and IPPS label generators, the dbt runner, the dbt-project-evaluator
  runner and the bronze and staging E2E; the table map, the retired and removed lists and the reviewed label overrides.
- `services/`, `docker-compose.yaml`: the Polaris catalog, Spark job and DuckDB analytics containers.
- `scripts/acquisition/`, `config/acquisition/`: collectors, storage checks, E2E suites and their locked plans and
  registry. Committed at commit `3af1abb` with full documentation of the collection process
  at the end of the data collection stage; collected data and audit evidence stay in the ignored `data/` folder.
  `scripts/acquisition/one_off/` holds tools that ran once, such as queue builders and the privacy and duplicate
  deletions, moved there from `data/` on Oct 4 2026; their run records stay in `data/`.
  No tutorial files are added to this repository.

## Limitations

These checks are not deployment or clinical validation. The acquisition checks
run on synthetic inputs; a clean checkout cannot replay stored captures, which need
the ignored local originals and the private legacy registry archive.
Nothing in S3 is approved for modeling: definitions, geography and modeling
reviews remain open. Raw personal-data retention remains a separate unresolved
control. The deferred controls above are not implemented.

The lakehouse runs on one Mac against the project bucket; it is not a deployed service. Bronze holds raw text only and
loads one copy per stored file. Staging covers the HAI, cost report, IPPS, occupational-mix, Provider of Services, Hospital
General Information, ownership and Care Compare timely and effective care, maternal health and HCAHPS tables. It types the
HAI and Care Compare windows, the cost reports, the Provider of Services hospital snapshots, Hospital General Information and the case-mix
index. It also builds the hospital-year spine; the other tables pass through as published. No gold or model tables exist. The bronze and staging E2E runs use
Docker and are not part of the local CI script. The final publisher redownload (run 3) has its queue built and checked
offline. It has not run: its controls, independent review and the owner's approval are still to come.

## Contributing

Individual project; contributions are not accepted.

## License

All rights reserved.

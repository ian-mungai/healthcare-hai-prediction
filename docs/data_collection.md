---
title: Data Collection
description: How the source data was collected and stored, with the commands that repeat or recheck each step.
last_updated: 2026-10-10
---

# Data Collection

This guide explains how the project's source data was collected and how to repeat or check each step. It covers every collection route: publisher APIs, scripted file downloads and the few browser downloads a person had to do by hand.

**Status:** collection is complete. The collection code (`scripts/acquisition/`), its configuration (`config/acquisition/`) and the collection tests are tracked in Git; all evidence under `data/` stays out of Git. Acquisition closeout remains open for the final redownload verification and confirmed disposal of personal originals (see the [Closeout Checklist](#closeout-checklist)). The offline rechecks below need the ignored local originals under `data/`; a fresh clone runs the synthetic suites only.

## Contents

- [How Collection Works](#how-collection-works)
- [Prerequisites](#prerequisites)
- [API Collectors](#api-collectors)
- [Scripted File Downloads](#scripted-file-downloads)
- [Browser Downloads](#browser-downloads)
- [Named Manual Steps](#named-manual-steps)
- [Privacy Handling](#privacy-handling)
- [Checking Stored Data](#checking-stored-data)
- [Publisher Data Dictionaries](#publisher-data-dictionaries)
- [Storage Changes After Collection](#storage-changes-after-collection)
- [What Cannot Be Repeated Exactly](#what-cannot-be-repeated-exactly)
- [Open Items](#open-items)
- [Closeout Checklist](#closeout-checklist)

## How Collection Works

**Route order.** Each source is collected by the first route that can deliver the years it needs: the publisher's API, then a direct download link fetched by a script, then, only when neither works and the project owner chooses it, a download by hand in a browser.

**Every collector follows the same steps:**

1. **Locked plan.** The exact requests, files or quarters are written once to a plan file with a `.lock.json` beside it holding the plan's SHA-256. A changed plan stops the run.
2. **Capture.** The publisher's original bytes are saved unchanged, write-once. A derived CSV may be written beside them; it never replaces the original.
3. **Receipt.** A `receipt.json` records the source, request or download, period, file hashes, row counts, warnings and the terms that apply. Every receipt carries `model_eligible: false`.
4. **Storage.** Files go to the project's private, versioned S3 bucket under `<publisher>/<collection>/`. Each object is read back by its exact version ID and its SHA-256 and size are compared before the step counts as done.
5. **Completion record.** A `completed.json` ties the receipt to its storage evidence. A rerun that finds it rechecks everything offline and makes no requests or writes.

**Safe by default.** Collectors run offline unless told otherwise: `--fetch` allows publisher requests and `--execute` allows S3 writes. Each collector takes an OS lock so two runs cannot overlap.

**Reviewed code only.** Each collector refuses to create new captures unless the SHA-256 of the current acquisition code appears in its `config/acquisition/*_code_versions.json` list. A new version is added only after its end-to-end suite and the other collectors' suites pass twice. Earlier versions stay listed, so older captures remain verifiable.

**Collection is not approval.** Storing a file checks its integrity only. Definitions, geography, coverage and model eligibility are reviewed later and every source keeps its holds until then.

## Prerequisites

Run everything from the repository root with the project's Python 3.12 virtual environment.

1. **Python packages:** `requirements.txt`, plus the collection-only packages in `config/acquisition/runtime_dependencies.json` (`jsonschema[format]==4.26.0` and `referencing==0.37.0`, which `capture.py` imports directly).
2. **Configuration:** a `.env` built from `.env.example`, holding the region, the project's AWS profile, account ID, project name and bucket. Storage refuses to run under any other identity.
3. **Infrastructure outputs:** `terraform -chdir=infra output -json` must work locally; storage checks the bucket settings against it.
4. **API keys**, stored by the project owner in AWS Secrets Manager as JSON with one field (`api_key`) and read only at request time: `bls_api_key`, `census_api_key` and `hud_api_key`. They never appear in plans, receipts, logs or S3.
5. **macOS** for browser downloads: the storage scripts read the download origin that Safari and Chrome record on each saved file (`com.apple.metadata:kMDItemWhereFroms`) with `xattr`.

## API Collectors

Each API collector has a plan builder, a collector and a synthetic end-to-end suite. The suites use generated data, a fake HTTP boundary and a fake versioned S3; they never contact a publisher or AWS. Every run writes a new report file, so give each run a new `--output` path.

| Source | Scope collected | Plan | Collect | End-to-end suite |
| --- | --- | --- | --- | --- |
| BLS Local Area Unemployment Statistics | County unemployment, employment and labor force, monthly and annual, 1990–2025: 520 requests | `build_bls_api_plan` | `collect_bls_api --fetch --execute --limit 520` | `run_bls_api_e2e` |
| Census ACS 5-year profiles, 2005–2009 | DP02, DP02PR, DP03, DP04 and DP05, every county and Puerto Rico municipio | `build_census_acs_api_plan` | `collect_census_acs_api --fetch --execute --limit 5` | `run_census_acs_api_e2e` |
| HUD USPS ZIP crosswalk, 2021 Q1–2025 Q4 | ZIP-to-county, nationwide, one request per quarter | `build_hud_api_plan` | `collect_hud_api --fetch --execute` | `run_hud_api_e2e` |
| CMS Mapping Medicare Disparities | The condition-years in its queue | `run_mmd_api_collection` builds its queue | `run_mmd_api_collection --execute --output <report>` or `collect_mmd_api --measure-id <id> --year <year> --fetch --execute` | `run_mmd_api_e2e` |

Prefix every module name with `.venv/bin/python -m scripts.acquisition.`, for example:

```sh
.venv/bin/python -m scripts.acquisition.collect_hud_api                   # offline recheck of stored quarters
.venv/bin/python -m scripts.acquisition.collect_hud_api --fetch --execute # request and store missing quarters
.venv/bin/python -m scripts.acquisition.run_hud_api_e2e --output <new_report.json>
```

**Rate limits.** BLS keeps a local request ledger and stops at 450 requests in any rolling 24 hours, below the publisher's 500. HUD requests are at least two seconds apart, under HUD's 60 a minute. Census and MMD requests run one at a time.

**Rechecking one capture offline:** `collect_bls_api`, `collect_census_acs_api`, `collect_hud_api` and `store_hud_xlsx` accept `--validate-receipt <receipt.json>`, which rebuilds the derived CSV from the original bytes and compares every recorded hash, without network access.

## Scripted File Downloads

Most sources are public files on publisher websites. Their download links come from the source registry (`config/acquisition/source_registry.json`, checked against `source_registry_lock.json`) or from links found on the publishers' own index pages.

**Current releases.** One bounded batch, `data/datasets/acquisition_batches/<batch_id>/batch.json` (local record, not in the repository), holds 140 download jobs across 35 sources. Validate it offline, then run it:

```sh
.venv/bin/python -m scripts.acquisition.batch --plan data/datasets/acquisition_batches/<batch_id>/batch.json
.venv/bin/python -m scripts.acquisition.batch --plan data/datasets/acquisition_batches/<batch_id>/batch.json --execute
```

**Earlier releases.** History is collected in two steps:

1. **Find the files.** A discovery script saves the publisher's index pages and lists only the links they actually contain, as a candidate file: `discover_history`, `discover_publisher_history`, `discover_acs_history`, `discover_acs_sequences` or `discover_california_exports`. Most take `--state-root <folder>`; run each with `--help` for its options.
2. **Download them.** `history_routes` adds the candidates to a copy of the registry (the registry itself is never edited) and downloads each file within a byte budget:

   ```sh
   .venv/bin/python -m scripts.acquisition.history_routes --candidates <candidates.json> --state-root data/datasets/historical_acquisition/<folder>
   .venv/bin/python -m scripts.acquisition.history_routes --candidates <candidates.json> --state-root data/datasets/historical_acquisition/<folder> --execute
   ```

ZIP archives are opened only through reviewed, checksum-bound archive maps (`continue_history --archive-maps <maps.json> --plan-register <register.json> --state-root <folder>`); each member is stored separately and the ZIP itself stays local.

**Which candidate list built which folder.** The original candidate filenames were not recorded consistently. The pairs below were matched by comparing the download links each folder added to its registry with each candidate file. "Done" counts jobs with a `completed.json`; a job without one was blocked or superseded and keeps its failure evidence.

| History folder | Candidate list | Jobs done |
| --- | --- | --- |
| `acs_2009_history` | `acs_sequence_discovery/candidates_*.json` (two identical lists match) | 320 of 320 |
| `acs_history` | `acs_discovery/candidates.json` | 74 of 74 |
| `ahrf_history` | `ahrf_remaining_candidates.json` | 7 of 10 |
| `ahrf_history_v2` | `ahrf_corrected_candidates.json` | 37 of 37 |
| `california_csv_history` | `california_export_discovery/candidates.json` | 81 of 82 |
| `census_history` | `census_labor_discovery/candidates.json` | 41 of 41 |
| `census_older_history` | `census_older_candidates.json` | 10 of 10 |
| `cms_annual_review` | `cms_annual_candidates.json` | 1 of 8 |
| `cms_history` | `cms_candidates.json` | 29 of 29 |
| `community_history` | `community_extra_candidates.json` | 13 of 13 |
| `illinois_history` | `illinois_candidates.json` | 259 of 270 |
| `impact_older_history` | `impact_older_discovery/candidates.json` | 23 of 23 |
| `structure_history` | `structure_remaining_candidates.json` | 132 of 132 |
| `workforce_history` | `workforce_remaining_candidates.json` | 132 of 133 |
| `reference_history` | partial match with `references_candidates.json` | 3 of 18 |
| `california_history`, `california_originals_history`, `california_originals_v2`, `california_large_original`, `reference_originals_history` | not matched: they add no links beyond the base registry | 0 of 104, 65 of 103, 38 of 39, 1 of 1, 15 of 15 |

The 11 Illinois exports that did not complete belong to closed hospitals; the publisher redirects them to its home page.

The two AHRF folders, their three reviewed AHRF archives and the three AHRF candidate lists were deleted from this Mac for license reasons; AHRF is also removed from S3. Each deleted file's path, size and SHA-256 is kept in `data/license_removal/20260929/ahrf_local_retirement.json` (local record, not in the repository).

## Browser Downloads

Four collections could only be saved by hand in a browser. In each case the storage script proves the file came from the publisher by reading the download origin the browser recorded and binds it to its SHA-256 before storing it.

| Collection | Why by hand | What the person does | Storage command |
| --- | --- | --- | --- |
| CMS Mapping Medicare Disparities exports (52 stored) | Collected before the MMD API route was built | Export each listed condition, year and geography from the MMD tool as CSV | `store_mmd_export --download <file> --template <receipt> --condition <name> --measure-id <id> --year <year> --domain <domain> --geography <County or State/Territory> --execute` |
| Census ACS table exports (16 stored) | Some tables and years are only offered as data.census.gov downloads | Download the table for all counties with all fields, for the listed five-year vintages | `store_census_export --download <file.zip> --table <id> --years <years>` |
| CDC WONDER county mortality, 1999–2024 (6 stored) | WONDER's API serves national data only; county results come only from its query form | Run the Underlying Cause of Death form grouped by county and year, with crude rates, zero and suppressed values shown and totals off. Save the export unchanged | `build_wonder_plan`, then `store_wonder_export --execute` |
| HUD ZIP-to-county workbooks, 2010 Q1–2020 Q4 (44 stored) | The HUD API covers whole-country requests only from 2021 Q1. The crosswalk files site has no fixed file links and scripting its login is not assumed to be allowed | Log in to the [HUD crosswalk files site](https://www.huduser.gov/apps/public/uspscrosswalk/login), choose **ZIP-COUNTY** and each quarter, select **Download** and save the file unchanged to Downloads | `build_hud_xlsx_plan`, then `store_hud_xlsx --execute` |

**HUD workbook route in detail.** The 44 downloads are listed with their size, SHA-256 and origin in `data/acquisition_planning/hud_zip_county_2010_2020_downloads.json` (local record, not in the repository). `build_hud_xlsx_plan` turns that list into a locked plan (`config/acquisition/hud_xlsx_plan_2010q1_2020q4.json`) that records each quarter's hash and county geography: 2000 Census counties through 2011 Q4 and 2010 Census counties from 2012 Q1. `store_hud_xlsx` then checks each quarter on its own:

- the file in Downloads has HUD's origin and the planned hash;
- the workbook has one sheet, the exact six columns, text ZIP and county codes, numeric ratios, no formulas or shared strings, unique ZIP–county pairs and every state and DC;
- the original is copied write-once and a CSV in the same layout as the API quarters is written beside it.

A quarter that fails is held and named; the others continue. Once a quarter is copied, the Downloads file is no longer needed. The failure analysis written before the code is in [HUD Crosswalk Workbooks](acquisition_design.md#hud-crosswalk-workbooks), items 27 to 45.

**2014 Q2 repeats.** HUD's 2014 Q2 workbook lists 49,140 of its 100,082 rows twice, with identical values. The original is stored unchanged and only the CSV drops the exact repeats. The exception is a separate locked record, `config/acquisition/hud_xlsx_exact_repeats_2014q2.json`, bound to that quarter's download hash and the exact count. Every other quarter still rejects any repeated row and a repeated pair with different values is always rejected.

```sh
.venv/bin/python -m scripts.acquisition.build_hud_xlsx_plan        # once; refuses to change an existing plan
.venv/bin/python -m scripts.acquisition.build_hud_xlsx_plan --approve-exact-repeats 2014 2 49140  # once; records the 2014 Q2 exception
.venv/bin/python -m scripts.acquisition.store_hud_xlsx             # offline checks of all 44 quarters
.venv/bin/python -m scripts.acquisition.store_hud_xlsx --execute   # store passing quarters with S3 readback
.venv/bin/python -m scripts.acquisition.run_hud_xlsx_e2e --output <new_report.json>
```

**WONDER route in detail.** WONDER does not offer county age-adjusted rates, so the measure is deaths, population and crude rate per 100,000 with 95% limits and standard error; age is handled by other predictors. The 1999–2020 database (older race categories) and the 2018–2024 database (single race) overlap in 2018–2020. WONDER cannot export 1999–2020 in one file, so that database is exported as 1999–2005, 2006–2012 and 2013–2019. `store_wonder_export` checks each file's WONDER origin (with the session identifier removed), the exact query record in its notes, the header, the planned years, at least 3,100 counties a year, every state and DC, unique county-years and the published flags. It refuses any count of 1–9 deaths. The plan, findings and failure modes are in [CDC WONDER Exports](acquisition_design.md#cdc-wonder-exports).

```sh
.venv/bin/python -m scripts.acquisition.build_wonder_plan               # once, from data/acquisition_planning/wonder_downloads.json
.venv/bin/python -m scripts.acquisition.store_wonder_export             # offline checks
.venv/bin/python -m scripts.acquisition.store_wonder_export --execute   # store with S3 readback
.venv/bin/python -m scripts.acquisition.run_wonder_export_e2e --output <new_report.json>
```

## Named Manual Steps

These steps need a person. Everything else runs from the scripts above.

1. **Accept publisher terms.** The owner's acceptance of the terms for HUD, CDC WONDER and AHRQ Community-Level Health, with each dataset's restrictions, is in `config/acquisition/terms_acceptance_20260928.json`. HUD receipts carry these restrictions; storage refuses a held source whose receipt does not match this exact file.
2. **Create API accounts and keys.** Register with BLS, Census and HUD, then store each key in Secrets Manager as described in [Prerequisites](#prerequisites).
3. **Browser downloads.** The three collections in [Browser Downloads](#browser-downloads).
4. **Approve plans and changes.** The owner approves each collection plan, each change to what a collector accepts (for example, HUD's 2022 Q1 layout without city and state, four Pacific territory codes from 2024 Q2 and the 2014 Q2 exact repeats) and each infrastructure change, including temporary IAM grants.

## Privacy Handling

A few sources contain personal names or contact details. The README's [Data section](../README.md#data) sets the rules; in short:

- The unchanged download stays on the local machine with owner-only access.
- `abstract_public_business_csv --policy <policy> --source-url <url> --input <original> --output <derivative>` writes a privacy-filtered copy; only that copy goes to S3.
- Stored versions that turned out to contain personal data were deleted by version ID and recorded as retired; `verify_storage_records` accounts for every retired version.
- **CMS Hospital All Owners** (44 monthly releases, November 2022 to August 2026) has its own collector, `store_cms_owners`. It downloads each original into `data/datasets/historical_acquisition/cms_hospital_owners/private_original/` (owner-only; local record, not in the repository) and stores only organization owner rows, without name, title, street address, city or ZIP columns; company names containing an individual owner's whole name are replaced with `[REDACTED]`. Plan: `config/acquisition/cms_owners_plan.json`; failure modes: [CMS Hospital Ownership](acquisition_design.md#cms-hospital-ownership).

  ```sh
  .venv/bin/python -m scripts.acquisition.store_cms_owners                      # offline recheck of completed releases
  .venv/bin/python -m scripts.acquisition.store_cms_owners --fetch --execute    # download missing originals and store
  ```

  The offline recheck rebuilds each stored CSV from its local original, so it needs the originals until they are deleted at closeout.
- **HCAI hospital annual utilization 2018–2025** (eight annual workbooks, 2025 preliminary) has its own collector, `store_hcai_util`. It downloads each workbook through the reviewed signed redirect into `data/datasets/historical_acquisition/hcai_util_2018_2025/private_original/` (owner-only; local record, not in the repository) and stores one abstracted CSV per sheet: personal columns, individual-owner details, copied names and stray contact details are replaced with `[REDACTED]`. Plan: `config/acquisition/hcai_util_2018_2025_plan.json`; failure modes: [HCAI Utilization](acquisition_design.md#hcai-utilization). Same commands as the owners collector, with `store_hcai_util`.
- **ONC meaningful-use attestations 2011–2017** (`store_onc_mu`) downloads `MU_REPORT.csv` into `data/datasets/historical_acquisition/onc_mu_attestation/private_original/` (owner-only; local record, not in the repository) and stores hospital rows only, without the clinician-only `Specialty` column; clinician rows never leave this Mac. Plan: `config/acquisition/onc_mu_hospital_plan.json`.
- **ACS derived history** (`collect_census_acs_detailed`) collects B17001 (2010–2016), B16001 and B16004 (2009–2015) through the Census API with the runtime-only key, then derives poverty % for 2010–2011 and the limited-English total for 2009–2015. Derived values are stored only after exact checks: B17001 against the stored S1701 exports (2012–2016) and B16001 against B16004 in the same years. Plan: `config/acquisition/census_acs_detailed_plan.json`; method: [Census ACS Detailed Tables and Derived History](acquisition_design.md#census-acs-detailed-tables-and-derived-history). Run it like the other collectors (`--fetch`, `--execute` or neither for an offline recheck).
- **Held sources released by an access record, not by terms.** HCAI is on access hold in the locked registry. Storage accepts it only when the receipt binds, by SHA-256, the record `config/acquisition/access_releases_20260929.json`; the terms-acceptance record for HUD and WONDER is unchanged.

## Checking Stored Data

- **One collector:** rerun it without `--fetch` or `--execute`; it rechecks every completed capture offline.
- **Census ACS 2009:** `reconcile_census_acs_api_e2e --output <new_report.json>` rebuilds all five tables and compares them with the stored evidence.
- **MMD:** `reconcile_mmd_history --measure-id <id> --years <years> --output <new_report.json>`.
- **The whole bucket:** `verify_storage_records --output <new_folder>` lists every S3 object version and matches it against the local storage records, counting retired versions. It compares sizes, not contents; the per-object content checks happen at upload. Pass every retirement record with `--retirements`, including `data/license_removal/20260928/ahrf_retired_objects.json` (local record, not in the repository) for the AHRF versions deleted for license reasons and `data/lakehouse_planning/dedup_20261003/retired_objects_duplicates.json` for the removed duplicates; the default finds the privacy and duplicate records but not the AHRF one. Versions under the bucket's top-level `lakehouse/` folder are Iceberg table files, not acquisition objects: the check counts them apart instead of as unrecorded. It still checks any upload record that names such a key. The latest full run is described under [Clean-Checkout Recheck](#clean-checkout-recheck). The command also needs `--evidence-root "$PWD/data" --empty-capture-dispositions data/privacy_review/20260927/empty_capture_dispositions.json`.

## Publisher Data Dictionaries

Data dictionaries, record layouts and codebooks are stored as reference documents under each collection's `references/` prefix, never as data. They explain the published columns. The bronze data dictionaries in the lakehouse take their column descriptions and published types from them; the Care Compare dictionaries give names and types only. The HRSA shortage-area detail files `BCD_HPSA_FCT_DET_PC.csv` and `MUA_DET.csv` are the exception: they are data stored with the dictionary role; corrected manifests give them the data role. Bronze loads them as data tables (see [Storage Changes After Collection](#storage-changes-after-collection)).

**Stored with the data:** the 30 Care Compare dictionary editions inside the HAI archives; one edition each of the CMS cost-report, All Owners, Change of Ownership, Enrollments, Provider of Services (`P.QWB.POSQ.OTHER.LAYOUT.MAR23.pdf`) and 2013–2016 Medicare inpatient dictionaries; the CMS geographic variation, hospital service area and MMD documentation; some IPPS and occupational-mix layouts; the HCAI finance documentation; ACS table notes; the HRSA nursing survey codebook and the HPSA and MUA metadata workbooks; and the Massachusetts technical appendix.

**Collected with the history route:** 56 more files through `history_routes` with the reference role, each with a receipt and S3 readback. For CMS datasets on data.cms.gov, each release names its dictionary in the `describedBy` field of the public catalog, `https://data.cms.gov/data.json`, which is saved as the evidence page:

- CMS: the older All Owners (4), Change of Ownership (4) and Enrollments (3) editions and the clinical-laboratory Provider of Services layout.
- Census: SAIPE state and county layouts for every stored year (31) and SAHIE layouts for 2005–2007 and 2008–2024. The county adjacency pages for 2010 and 2023–2026 hold that file's record layout.
- USDA RUCA and RUCC documentation pages and the CDC PLACES data dictionary.
- The Medicare inpatient by provider (2017+) and by provider and service dictionaries, found through each release's `resourcesAPI` link in `data.json`. Also the HHS hospital capacity column definitions (`columns.json`).

Candidate lists and saved publisher pages are in `data/datasets/historical_acquisition/dictionary_discovery_20261002/` (local record, not in the repository); the run folders are `dictionary_history_20261002/`, `dictionary_history_other_20261002/` and `dictionary_history_group1_20261002/`.

```sh
.venv/bin/python -m scripts.acquisition.history_routes --candidates data/datasets/historical_acquisition/dictionary_discovery_20261002/cms_dictionary_candidates.json --state-root data/datasets/historical_acquisition/dictionary_history_20261002
.venv/bin/python -m scripts.acquisition.history_routes --candidates data/datasets/historical_acquisition/dictionary_discovery_20261002/other_dictionary_candidates.json --state-root data/datasets/historical_acquisition/dictionary_history_other_20261002
.venv/bin/python -m scripts.acquisition.history_routes --candidates data/datasets/historical_acquisition/dictionary_discovery_20261002/group1_dictionary_candidates.json --state-root data/datasets/historical_acquisition/dictionary_history_group1_20261002
```

Without `--execute` each command validates its list offline and reports no pending jobs once everything is stored.

**Also stored:** the ACS column metadata. Every data.census.gov export archive holds a `*-Column-Metadata.csv`, stored as a reference document. The ACS API collectors store each table's `groups/<table>.json` variable labels with the data.

**Held-source documents:** `history_routes` accepts a reference document from a source on access hold when its candidate names a release binding, `terms` or `access_release`. Data files from held sources are still refused. Storage rechecks the bound record. HUD answers every scripted request with an empty HTTP 202, so its crosswalk page and Cityscape methodology article (`hud_dictionary_candidates.json`, run folder `dictionary_history_hud_20261002/`) use the browser route below.

**HCAI documentation:** the access release also covers HCAI's publisher documentation: the reporting form and instructions for each year from 2012 to 2025, 28 PDFs, stored through the held-source history route. The extension is a separate record, `config/acquisition/access_releases_20261002.json`. The first record stays unchanged, because the stored HCAI receipts bind it by SHA-256. Storage accepts a receipt that binds either record. New receipts bind the newest. Candidates: `hcai_dictionary_candidates.json`, after `history_redirects` reviewed each redirect; run folder: `dictionary_history_hcai_20261002/`.

**Browser downloads.** `store_reference_download` stores reference documents a person saves in a browser, for publishers that refuse scripted requests. A reviewed request list names each file's source, role, title and exact origin URL. `--build-plan` checks each saved file's browser-recorded origin and writes the plan with every file's SHA-256 and size, plus its lock. Storage rechecks the origin, hash and plan. The receipt records `manual_download` with no HTTP status. The proof keeps only the planned URL, not the referring page. Each file is held or stored on its own, exactly once. A held source needs a release binding, as in the history route. Its code version is recorded on the passing acquisition gate, without a separate review. Failure modes: [Reference Downloads](acquisition_design.md#reference-downloads).

```sh
.venv/bin/python -m scripts.acquisition.store_reference_download --build-plan <requests.json>
.venv/bin/python -m scripts.acquisition.store_reference_download
.venv/bin/python -m scripts.acquisition.store_reference_download --execute
```

**SVI documentation:** the SVI documentation tool serves its PDFs from `https://svi2.cdc.gov/webapi/documents/publication?filename=<name>`, which answers scripted requests. The history route stored the editions for 2000, 2010, 2014, 2016, 2018 and 2020 from there. The 2022 county and tract edition came from its ATSDR address, because the tool offers only the ZCTA edition for 2022. Each of the six tool-served files matches a browser copy byte for byte. Candidates: `svi_dictionary_candidates.json`; run folder: `dictionary_history_svi_20261002/`.

**Stored with the browser route:** BLS `la.txt`, the two CDC WONDER help pages (`ucd.html` and `ucd-expanded.html`) and the HUD crosswalk page with its Cityscape 20(2) article. The HUD files are bound to the HUD terms acceptance. Requests: `config/acquisition/reference_download_requests.json`; plan and lock: `config/acquisition/reference_downloads_plan.json`; state folder: `data/datasets/historical_acquisition/reference_downloads/` (local record, not in the repository). The plan accepts only names without spaces, so saved pages are renamed in Downloads before planning; the origin and creation time move with the file. The HUD page names one HUD staff contact, as published.

**Still missing:**

- **Mapped at staging:** the IPPS layouts.
- **Not published:** no dictionary was found for ONC Promoting Interoperability or the Illinois hospital report card.

The full list is in `plans/publisher_dictionaries_20261002/inventory.md` (local record, not in the repository).

## Storage Changes After Collection

Each change keeps every stored original that holds unique bytes.

- **Local folder layout.** The dataset folders `historical_acquisition`, `acquisition_batches`, `acquisition_recoveries` and `snapshots` are under `data/datasets/` (local record, not in the repository), each moved in one rename with file counts and bytes compared. Receipts and records keep the paths they were written with. `config/data_paths.json` maps each old folder to its new one. The collectors, checks and redownload read recorded paths through `scripts/acquisition/data_paths.py`.
- **Duplicate S3 versions removed.** 2,193 versions (3.72 GB) that were byte-identical to a kept copy are deleted; no deletion permission remains. Each kept twin was verified live by size and S3 SHA-256 before its duplicate was deleted. Each deletion was read back. The retirement record `data/lakehouse_planning/dedup_20261003/retired_objects_duplicates.json` (local record, not in the repository) lets the storage check and bronze account for them.
- **HRSA manifests corrected.** The HPSA and MUA detail files hold the designations themselves but were stored under the dictionary role. `scripts.acquisition.correct_manifest_roles` wrote a corrected manifest for each, from the committed specification `config/acquisition/manifest_corrections.json`: it changes only those two roles to `data` and names the original manifest it supersedes. The originals and the data files are unchanged. A dry run is the default; `--store` needs the specification's SHA-256. Its record is in `data/datasets/manifest_corrections/` (local record, not in the repository).
- **One-off tools.** Tools that ran once, such as the queue builders and the privacy and duplicate deletions, are in `scripts/acquisition/one_off/`; their records stay under `data/` (local record, not in the repository). `data/acquisition_planning/code_moved_20261004.json` maps each old path to its new one.
- **Redownload run 3 queue.** Run 3 covers only the files bronze uses plus the stored dictionaries and reference documents. The queue is `data/redownload_checks/20261004/queue.json` (local record, not in the repository), built by `scripts/acquisition/one_off/redownload/build_queue_run3.py`: 2,888 active downloads. Its offline preflight resolves every unit with no requests. The frozen controls and the independent review sit beside the queue; manual units wait until a person saves their pilot file in a browser.

## What Cannot Be Repeated Exactly

- **Publishers revise their data.** Most APIs and download links serve only the current version, so a new download may not match the stored bytes. The repeatable record is the stored original plus its hash, receipt and plan, not a fresh download.
- **Browser downloads** depend on a person and the publisher's website at the time. Their hashes, origins and file creation times are recorded; HTTP status and headers do not exist for them.
- **Blocked routes.** Some files returned errors (HTTP 403, timeouts or redirects). Their failed attempts are kept as evidence and the files are not stored.

## Open Items

- **Closed as gaps:** NY reports 2012 and 2016, AHRQ Compendium and HFMD, Massachusetts and Minnesota staffing; HUD ends at 2025 Q4, ZIP-to-county only. **Not collected:** the sources held for access, licenses or privacy decisions listed in the source registry. AHRQ Community-Level Health is not collected; the ACS and SVI originals are used instead.
- **Checks outside collection:** coverage accounting, definitions by year, geography changes and model eligibility.
- **Publisher data dictionaries:** the sources listed as still missing in [Publisher Data Dictionaries](#publisher-data-dictionaries).

## Closeout Checklist

At the end of the collection stage:

1. Done: `scripts/acquisition/`, `config/acquisition/` and the collection tests are tracked and run in CI. `source_registry.json` and `planning_v1/acquisition_plans_v1.json` are over the 5 MB data-file limit, so they are tracked through Git LFS; `.gitattributes` lists them, the repository's Git LFS hooks are installed and CI checks out with `lfs: true`.
2. Done (see [Clean-Checkout Recheck](#clean-checkout-recheck)): rerun the collection checks and one offline replay per collector from a clean checkout, then compare the results with the stored evidence.
3. Done (see [Recovered History Provenance](#recovered-history-provenance)): confirm the unmatched history folders above and record which command built each.
4. Done with registry revision 2: update the source registry to the API-first route order and retire the temporary route overlay.
5. Update this guide with the final counts.
6. Delete the local originals that name individuals (the CMS owners originals, the unredacted HCAI 2012–2017 and 2018–2025 originals and the ONC meaningful-use original) after the user confirms the exact list, then record their retirement so the offline rechecks accept their absence.

### Clean-Checkout Recheck

Closeout checklist item 2 runs with `scripts/acquisition/one_off/closeout/clean_checkout_replay.sh`. Evidence is in `data/e2e/closeout_clean_20261001/` (local record, not in the repository).

- **Stage A:** a copy holding only the files Git tracks, with no `data/` folder, passes the acquisition gate: 419 tests and 90.94% coverage.
- **Stage B:** the same copy, linked to the real `data/` folder, passes every offline recheck: BLS, Census ACS profiles and their reconciliation, ACS derived history (22 snapshots), HUD API, HUD workbooks (44), WONDER (6), CMS owners (44), HCAI utilization (8), ONC meaningful use (70,492 rows) and the data-dependent MMD suite (28 scenarios). The redownload preflight resolves every queued unit offline.
- **No stored evidence changed:** a size and modification-time manifest of `data/` showed no file changed or removed by the recheck.
- **Checklist items 3 and 4** are met: the history provenance record identifies each history folder's collector; registry revision 2 retires the temporary route overlay.
- **Storage check:** `verify_storage_records` skips `data/schema_review/` (local record, not in the repository) as derived review evidence, like `data/e2e/` and `data/conformance/` (failure modes in [Storage Records Check](acquisition_design.md#storage-records-check), independently reviewed). The read-only check passes: 22,162 live versions, 21,918 recorded and verified, 692 retired versions confirmed absent, 150 replacements verified, 720 known only from saved inventories and no missing, unrecorded or wrong-size versions. Report: `data/storage_checks/closeout_20261001/20261001T165556Z/report.json`. The schema-review check's report is in `data/e2e/storage_schema_review_20261001/`, because its synthetic example is shaped like an S3 listing.

## Closeout Records

The closeout sequence is code and privacy cleanup, registry versioning, then coverage records and documentation. The bounded publisher redownload (run 3, see [Storage Changes After Collection](#storage-changes-after-collection)) is the final substantive verification. Personal originals remain local until that verification passes and the user confirms the exact deletion list.

Locked plans and records work the same way. The tracked plans, the terms and access-release records, the HUD 2014 Q2 repeat record, the manifest corrections and the code-version lists state the current rule only. Their exact earlier bytes sit, with the four plans as committed before the dataset folders moved under `data/datasets/`, in the private archive `data/acquisition_planning/acquisition_legacy_20261009/` (local record, not in the repository). `config/acquisition/legacy_versions.json` and its lock bind each archived file by SHA-256 and canonical hash. `scripts/acquisition/legacy_versions.py` accepts a capture that recorded an archived plan or record only when those bytes still match. `scripts.acquisition.one_off.successor.build_successor` rebuilds the tracked files from the archive; a rerun changes nothing. Every collector's offline recheck passes with them.

The successor registry is revision 2. Existing receipts retain their original registry fingerprints. `registry_versions.json` and its lock bind the exact legacy registry, lock and additions files in a private, Git-ignored archive. A legacy replay needs that archive; a public clean checkout must not contain it. Unknown fingerprints, substituted bytes and missing legacy files fail closed. New plan builders use the sanitized successor. The original source and modeling gates remain uncleared; the registry records the current decisions, including the C259 crude-rate definition and the source and measure exclusions, apart from the transport records.

The five configuration documents contain neutral external-note labels and masked phone examples. Masks use `(XXX) XXX-XXXX`, so they cannot be mistaken for actual numbers. The synthetic AWS test account evaluates to twelve ones, retaining a distinct all-zero negative case.

The local evidence index contains 2,990 capture candidates across 47 source IDs. These include superseded, audit and derived records; they are not 2,990 publisher requests or model-eligible observations. `config/acquisition/collection_closeout.json` records per-source candidate and unique snapshot counts. All 2,990 receipt-file hashes match the inventory taken before the cleanup; this checks receipt immutability, not a fresh S3 readback or a rebuild of every object.

Stored collection scope remains: HUD ZIP-to-county for all 64 quarters of 2010–2025; 520 BLS requests; six WONDER exports covering 1999–2024; 62 CMS ownership-change/enrollment releases and 44 company-owner releases; HCAI annual files for 2018–2025; the separate ONC meaningful-use hospital series for 2011–2017; and the ACS poverty 2010–2011 and limited-English 2009–2015 derivations. The 21 detailed ACS source tables plus one derived snapshot retain their 12 exact published-table comparisons. These are acquisition coverage statements; definitions, suppression, revision, geography and model eligibility still require downstream review.

### Recovered History Provenance

`data/acquisition_planning/closeout_20260929/history_provenance.json` (local record, not in the repository) identifies `scripts.acquisition.history_routes` as the collector for the six histories the filename table above does not fully match. Every saved job identifier matches the canonical candidate-derived identifier and its route URL matches the saved registry. Exact candidate lists were recovered from each job manifest; the original shell invocation was not logged and is not claimed.

| History folder | Preserved jobs | Completed jobs | Recovered candidate list |
| --- | ---: | ---: | --- |
| `california_history` | 104 | 0 | `data/acquisition_planning/closeout_20260929/recovered_candidates/california_history.json` (local record, not in the repository) |
| `california_originals_history` | 103 | 65 | `data/acquisition_planning/closeout_20260929/recovered_candidates/california_originals_history.json` |
| `california_originals_v2` | 39 | 38 | `data/acquisition_planning/closeout_20260929/recovered_candidates/california_originals_v2.json` |
| `california_large_original` | 1 | 1 | `data/acquisition_planning/closeout_20260929/recovered_candidates/california_large_original.json` |
| `reference_originals_history` | 15 | 15 | `data/acquisition_planning/closeout_20260929/recovered_candidates/reference_originals_history.json` |
| `reference_history` | 18 | 3 | `data/acquisition_planning/closeout_20260929/recovered_candidates/reference_history.json` |

The filename table above is the filename-matching audit. The recovered manifests resolve collector and candidate provenance without inventing shell history or authorizing any rerun. Reproducing a fresh publisher download remains part of the bounded final-verification plan.

### Current Decisions and Reviewed Fingerprints

Registry revision 2 distinguishes retained historical reviews from effective current decisions. Dropped controls show a current `dropped` decision; C259 states the crude rate definition. Earlier control fields remain under `historical_preserved_controls`. ACS derivation completion, the separate ONC hospital-only series and completed AHRF removal are recorded explicitly. The current exclusion policy rejects AHRF, AHA, LEAP, HASC, NDNQI, APIC and CLH in capture, historical planning, batch/archive execution and storage, including attempts using legacy plans. Passing that exclusion check does not grant a route, privacy, terms or model approval. The retired temporary route overlay stays as provenance; revision 2 carries the API-first order and current decisions.

Nine reviewed code-version documents use typed `file`/`sha256` pairs instead of API-named dictionary keys. A strict reader accepts old and new forms without adding approvals. The migration record pins the normalized preconversion documents and repeatable E2E checks prove all historical approval maps unchanged. The original documents are retained privately outside Git. Complete secret scans of acquisition code and configuration report no leaks without scanner exemptions.

The existing regression tests pass from a disposable copy containing code, tests and configuration, with no `data/` or environment file. Historical MMD replay and registry migration separately require retained originals and the private legacy archive. This is a clean-copy regression check, not a full publisher redownload, fresh S3 readback or proof of model eligibility.

The acquisition test entry point runs the synthetic integrated suites twice, with durable per-suite artifacts. The regression-only route fixture is overridden with the real collection layout. All 16 retained Census browser exports regenerate identical registry, lock and plan files against their exact legacy parents. This is an offline metadata check; rebuilding every source object and fresh S3 reconciliation remain final-stage work.

The synthetic CMS/HCAI/ONC E2E drivers use minimal public metadata fixtures under `config/acquisition/e2e_inputs`; those contain URLs, periods, schema headers and file/sheet metadata, with publisher contacts removed. They contain no source rows. ACS scope checking reads its locked config plan rather than requiring ignored dictionary files. Cleanup implementation and verification boundaries are recorded in `data/acquisition_planning/closeout_20260929/cleanup_result.md` (local record, not in the repository).

### Coverage Gate and Code Versions

The coverage target is met with end-to-end scenarios, not unit tests or a lower target.

- **History suite:** `scripts.acquisition.run_history_e2e` drives the history collectors (`history_redirects` and `history_routes`) through fake publisher responses, fake versioned S3 and a fake Terraform output. Its 14 scenarios cover:
  - exact unsigned redirects into the reviewed bucket only
  - HTTP errors and network failures that are never marked verified
  - evidence that is never overwritten
  - held, privacy-review, large-file and user-excluded sources rejected
  - repeated URLs planned once and byte budgets enforced
  - a dry run with no side effects
  - capture and storage exactly once, with no repeat work on rerun
  - a failed download recorded but not completed
  - a wrong-bucket redirect that fails its job
- **Registry additions:** the data-free `run_registry_additions_e2e` suite is part of the gate. Coverage follows its child processes (`patch = subprocess` in `config/acquisition/coverage.ini`). The gate keeps only its `artifact.json`, because each run copies the 17 MB registry into every case (about 300 MB).
- **Result:** `bash scripts/acquisition/run_checks.sh` passes: Ruff, MyPy, registry validation and the tests, including every offline E2E suite twice. Coverage is above the 90% target (90.94% in the [Clean-Checkout Recheck](#clean-checkout-recheck)); the omit list is unchanged.
- **Local caveat:** on this Mac, Python skips `.pth` files that macOS marks hidden and every file in `.venv/lib/python3.12/site-packages` carried the hidden flag. Child-process coverage needs `chflags nohidden .venv/lib/python3.12/site-packages/a1_coverage.pth`. If the flag returns, the child-process lines go unmeasured and the gate fails visibly rather than passing falsely.
- **Code versions:** all nine collectors (BLS, HUD API, Census ACS, ACS detailed, WONDER, CMS owners, HCAI, ONC, MMD) have a reviewed version for the closeout code; prior versions are kept. The basis is recorded in `data/e2e/closeout_20260929/code_version_review.json` (local record, not in the repository): the hashed production modules match the cleanup review's final file hashes and the gate passed.

Not verified here: live publisher downloads (the final bounded redownload, run 3) and a fresh S3 readback.

### Acquisition Checks in CI

- **GitHub Actions:** a separate "Acquisition Checks" job runs `bash scripts/acquisition/run_checks.sh` on Linux, with a 30-minute limit, LFS checkout, the pinned Terraform 1.16.1 (collectors resolve the binary; the checks never run it against state) and the retained `data/e2e/acquisition_checks/` (local record, not in the repository) artifact. It runs through `scripts/acquisition/run_checks_ci.sh` only when the pushed or pull request range changes a path the `acquisition-checks` hook watches; a manual run, the first push of a branch or rewritten history runs every check. `bash scripts/acquisition/run_checks_ci_e2e.sh` tests that gate.
- **Pre-commit:** the `acquisition-checks` hook runs only when `scripts/acquisition/`, `config/acquisition/`, `tests/test_source_registry.py`, `scripts/process.py`, `scripts/infrastructure/render_project_config.py`, `requirements.txt` or `.github/workflows/ci.yml` change, so a change to the CI job is checked.
- **Git:** `scripts/acquisition/`, `config/acquisition/` and `tests/test_source_registry.py` are tracked; `data/` stays ignored.
- **Linux differences:** the HUD workbook and WONDER storage steps read two pieces of macOS browser-download metadata, the `com.apple.metadata:kMDItemWhereFroms` attribute and the file birth time (`hud_xlsx_contract.download_created_at`). Off macOS, the two suites answer only those two reads from a synthetic record, so the real plist parsing and origin checks still run; on a Mac they use the real attribute and birth time.
- **Private archive:** the registry-additions suite records its legacy-registry check as skipped when the private archive folder is absent (as in any clean checkout) and runs it wherever the archive exists.

### Bounded Redownload Controls

The full redownload queue in `data/redownload_checks/20260929/` (local record, not in the repository) holds 3,002 entries and 2,992 active acquisition units. The harness requires a passing separate independent review bound to `data/redownload_checks/20260929/controls.json` before execution state or requests. That snapshot binds runtime code, pinned dependencies, the queue and operational plans/locks/terms/privacy/job inputs, the code-version lists the collectors read, the subprocess launcher and the hash of local runtime settings, without copying any credential value. New code versions are therefore listed before the controls are frozen, so the review covers them.

Persistent request and byte reservations prevent restart from resetting attempts or captured-byte budgets. BLS original and fresh roots share a quota lock; 429 stops before retry or body capture on the real HTTP boundaries. The run root is owner-only, every staging path is checked for symlinks/overlap and private retention records whole original directories plus file hashes, including failed attempts. Byte accounting conservatively includes received bytes and additional persisted copies; interrupted allocations remain charged. Only control ledgers, inventories and outcomes are excluded. This can stop before 128 GiB of physical source payload and never increases the ceiling.

ZIP comparisons reject duplicate/unsafe names and bound decompression before streaming member hashes. Collector comparisons bind hashes to logical artifact identities, including an explicit mapping for the 52 MMD browser-to-API exports. Manual origin/time/content checks remain local evidence: they do not independently prove a fresh publisher request, so live manual exports still need their download evidence.

**Content, not download date.** The same content downloaded on any date matches only where the format names exactly which value carries the build time: an Office file's core properties. Files named `.zip`, `.xlsx` or `.docx` that begin with a zip header compare entry by entry, directories included; every archive, member and directory comment compares by bytes. Census table notes and WONDER exports compare by bytes, so a new access or query date is flagged for review. Any file that starts with a byte-order mark compares by bytes. An Office file's member `docProps/core.xml` compares as its raw text with only its own created and modified times masked (`document_build_time`). This applies only when the root is the Office core-properties element, the `cp`, `dcterms` and `xsi` prefixes are bound to their Office namespaces wherever they are declared and each time is typed `dcterms:W3CDTF` and is a real UTC time. A safe parse only confirms which elements carry them. Every other file, saved web pages included, compares by bytes. A stored copy is compared only after its SHA-256 matches its receipt; otherwise the result is flagged for review. Any other difference stays `changed_needs_review`. Each result lists its `excluded_parts` and the report counts matches by part. A manual file counts only when its recorded origin is the unit's own address, with only the fragment and a session identifier removed; a changed file from that address is kept and flagged for review instead of left waiting. A WONDER export without the planned bytes runs the collector's origin and export checks inside the harness and is then compared with the stored export by bytes. [Redownload Checks](acquisition_design.md#redownload-checks).

The controls bind one state root, so selecting another root invalidates the review rather than resetting budgets. Report and resume commands also check root ownership. The harness E2E evidence is `data/acquisition_planning/full_redownload_20260929/fixes_e2e_release_run1.json` (local record, not in the repository) and `fixes_e2e_release_run2.json` (54 of 54 scenarios each). Full gate results belong to the remediation result record, not a claim of live publisher or model validation.

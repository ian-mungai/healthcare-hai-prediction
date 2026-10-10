---
title: Acquisition Design
description: What each collector and shared acquisition mechanism checks and which failure each check prevents.
last_updated: 2026-10-10
---

# Acquisition Design

This note explains why the collection code in `scripts/acquisition/` checks what it checks. It has one section per shared mechanism and one per collector: what the code collects or does, then each check with the failure it prevents. The runbook, [Data Collection](data_collection.md), gives the commands. Where this note and the code differ, the code is the reference.

Checks fail closed: a failed check stops the item with a fixed reason and no completion record is written. That is not the same as having no side effects. Storage uploads and verifies objects one at a time, so a check that fails partway can leave earlier verified objects in the bucket without a completion record; a rerun finds them by key and verifies them instead of writing again. Batch runs also store failed or partial downloads on purpose as audit evidence, never as data.

Collection checks prove integrity and provenance only. A successful capture stays `acquired_unvalidated` with verification `pending`. A failed capture is `evidence_only_partial` with verification `evidence_only` when part of the response arrived, otherwise `rejected` with verification `rejected`. Storage manifests carry `model_eligible: false`, as do the lineage and completion records of the plan-based collectors. Definitions, geography, coverage and model eligibility are reviewed later.

## Contents

- [Shared Mechanisms](#shared-mechanisms)
  - [Download Transport](#download-transport)
  - [Capture Receipts](#capture-receipts)
  - [Storage in the Project Bucket](#storage-in-the-project-bucket)
  - [Locked Plans, Completion Records and Reruns](#locked-plans-completion-records-and-reruns)
  - [Source Registry, Terms and Access Releases](#source-registry-terms-and-access-releases)
  - [Reviewed Code Versions and Earlier Plan Versions](#reviewed-code-versions-and-earlier-plan-versions)
  - [API Keys](#api-keys)
  - [Browser Download Origins](#browser-download-origins)
  - [Personal Data in Publisher Files](#personal-data-in-publisher-files)
- [API Collectors](#api-collectors)
  - [BLS API](#bls-api)
  - [Census ACS Profiles API](#census-acs-profiles-api)
  - [Census ACS Detailed Tables and Derived History](#census-acs-detailed-tables-and-derived-history)
  - [HUD Crosswalk API](#hud-crosswalk-api)
  - [CMS Mapping Medicare Disparities](#cms-mapping-medicare-disparities)
  - [BLS API Fallback](#bls-api-fallback)
  - [Illinois Hospital Directory API](#illinois-hospital-directory-api)
- [Scripted File Downloads](#scripted-file-downloads)
  - [Download Batches](#download-batches)
  - [History Routes](#history-routes)
  - [ZIP Archives](#zip-archives)
- [Browser Downloads](#browser-downloads)
  - [Census Table Exports](#census-table-exports)
  - [HUD Crosswalk Workbooks](#hud-crosswalk-workbooks)
  - [CDC WONDER Exports](#cdc-wonder-exports)
  - [Reference Downloads](#reference-downloads)
- [Sources With Personal Data](#sources-with-personal-data)
  - [CMS Hospital Ownership](#cms-hospital-ownership)
  - [HCAI Utilization](#hcai-utilization)
  - [ONC Meaningful Use](#onc-meaningful-use)
  - [Public-Business CSV Abstraction](#public-business-csv-abstraction)
- [Checks After Storage](#checks-after-storage)
  - [Storage Records Check](#storage-records-check)
  - [Manifest Corrections](#manifest-corrections)
  - [Redownload Checks](#redownload-checks)

## Shared Mechanisms

### Download Transport

`transport.py` downloads every scripted file. Each attempt gets its own folder and `transport.json` record, written once.

| Check | Failure it prevents |
| --- | --- |
| The URL must be public HTTPS with no user name, password, fragment or key-like query parameter. Local and metadata-service addresses are refused. | A credential is saved in a record. A request reaches a machine that is not a publisher. |
| Redirects must stay on the requested host and use public HTTPS. | A download moves to another site. |
| One exact signed redirect into a reviewed publisher bucket is allowed for each reviewed resource. The method must be GET, the status a redirect code and the target path exact. Every signature parameter must be present and well formed, with a bounded expiry. | An unreviewed or forged signed address is followed. |
| After a signed redirect no further redirect is allowed and the request carries no publisher headers or cookies. The signature query is removed from every record. | Publisher headers reach another host. A signed address leaks through a record. |
| The request asks for unencoded bytes. Encoded, partial (`206` or `Content-Range`), short, oversized or incomplete responses are failures, never complete files. | A compressed or truncated file is stored as complete. |
| An empty body or HTML in place of data is refused. | An error or login page is stored as data. |
| Each payload gets a light format check. ZIP, XLSX and DOCX must open with a non-empty member directory; XLSX and DOCX also need their required package entries. PDF needs its header and an end marker in the last 4 KiB. JSON up to 32 MiB must parse without duplicate keys or non-finite numbers. XLS and gzip need their file signatures. The first 8 KiB of CSV or text must not look like JSON, XML or binary. | A wrong file type is stored under the expected name. |
| Member CRCs and workbook contents are not checked here. The archive parser or collector that reads them does that. | The transport check is mistaken for full validation. |
| Only timeouts, `408`, `429`, server errors and network failures are retried, with growing waits, a `Retry-After` cap and validated size and time limits. | A run hangs, retries without end or sends too many requests. |
| Failures are recorded as fixed codes from a table. The body of a failed signed request is not kept. | Provider error text or echoed credentials enter evidence. |

### Capture Receipts

`capture.py` turns a plan into a local snapshot whose `receipt.json` follows `config/acquisition/snapshot_contract.schema.json`.

| Check | Failure it prevents |
| --- | --- |
| A plan may hold only known fields and must state its scope. Its route must be exactly one registry file route that is not a discovery index or a recorded failed route. | A plan with unchecked options runs or a route never meant to deliver data is used. |
| Governance must say the data is public, needs no credentials and holds no protected health or personal data. A held source is refused, except a reference document with a release binding. Reference-only sources never supply data. | A sensitive or held source enters the general downloader. |
| The receipt is schema-validated before and after the download. Artifact paths must stay inside the snapshot and every hash and size must match the file on disk. | A receipt describes missing, altered or misplaced files. |
| A capture can never mark itself validated. A successful capture stays `acquired_unvalidated` with verification `pending`; a failed one is `evidence_only_partial` (`evidence_only`) or `rejected` (`rejected`). The status pair must match. | Collection is mistaken for approval of the data. |
| A plan may pin the expected SHA-256. A different download is recorded as incomplete. | A changed publisher file replaces the planned one unnoticed. |
| The lineage records the registry and schema hashes. The snapshot folder name holds a UTC time and a random part and is created exclusively. | A capture cannot be traced to its rules or two captures collide. |

### Storage in the Project Bucket

`s3_store.py` stores verified captures in the project's private, versioned bucket. `storage_controls.py` sends each receipt to the check of the collector that wrote it.

| Check | Failure it prevents |
| --- | --- |
| Before any write, the caller must be the IAM user named by the project profile in the project account. | Data is written under the wrong identity. |
| Bucket, region and collection prefixes must match both the local settings and the Terraform outputs. | Data is written to the wrong bucket or prefix. |
| Versioning, all four public-access blocks, SSE-S3 default encryption and disabled object ACLs must be on. | Data lands in an unversioned, public or unencrypted bucket. |
| AWS calls ignore AWS environment variables and endpoint overrides. Every bucket call names the expected account. Errors keep only the AWS error code. | A stray setting redirects the call. Provider text leaks into logs. |
| The receipt is revalidated against the locked registry and schema (or a catalogued earlier registry). A held source needs its release record. The collector's own `verify_capture` rebuilds its derived files. | A capture that no longer matches its rules is stored. |
| Only complete captures are stored as data. Failed captures can be stored only as audit evidence. | A broken download becomes a data object. |
| Object keys hold the file's SHA-256. Uploads use "write only if absent" with a SHA-256 checksum. An existing key is verified, never replaced. | A stored version is overwritten or two runs race. |
| Every object is read back by its version ID. Version, length, local hash, S3 checksum and encryption must match. | A failed write is recorded as stored. |
| Objects are stored and verified one at a time. The reconciliation file and any completion record are written only after the last object verifies. | A partly stored capture is recorded as complete. |
| ZIP containers are never uploaded, only their verified members. Objects are capped at 512 MiB. | Unreviewed archive contents reach the bucket. |
| Manifests and the reconciliation file are written once. Local evidence is published atomically and an existing file with different bytes stops the run. | Storage evidence is lost, half-written or edited later. |

### Locked Plans, Completion Records and Reruns

These rules hold for the BLS, Census profile, Census detailed, HUD API, HUD workbook, WONDER, CMS ownership, HCAI, ONC and reference-download collectors. MMD and the browser-export scripts for MMD and Census differ; their sections say how.

| Check | Failure it prevents |
| --- | --- |
| The plan sits beside a `.lock.json` holding its canonical hash. Plan, registry hash and pinned reference files must all match. | A plan is edited between review and use. |
| Collectors run offline by default: `--fetch` allows requests and `--execute` allows bucket writes. An OS lock stops two runs of one collector from overlapping. | An accidental run sends requests or writes data. Two processes write one capture. |
| A successful response is cached write-once with its hash. A rerun reuses it after rechecking the hash and exact request. | Retries create duplicate requests or snapshots. |
| A `completed.json` binds the receipt, reconciliation and plan hashes and the status, plus the row count for every collector except reference downloads. A rerun that finds it rechecks the receipt and every stored object's key, bucket, hash, size and version record offline and makes no requests or writes. | A completed item is downloaded or stored twice. Local evidence drifts from storage. |
| `--validate-receipt` works without network access. For a collector that writes a derived CSV, it rebuilds the CSV from the original bytes and compares every hash. For reference downloads, which store only the original document and its download proof, it checks the document, the proof and the plan binding. | A derived file no longer follows from its original. |
| Collectors of many separate files check and store each item on its own. A failing item is held with a fixed reason and the run exits non-zero naming it. | One bad file blocks the rest or is skipped silently. |

### Source Registry, Terms and Access Releases

`source_registry.py` checks `config/acquisition/source_registry.json` against its lock. `registry_additions.py` handles added measure controls. `s3_store.py` checks releases of held sources.

| Check | Failure it prevents |
| --- | --- |
| The registry must match its lock: source and measure-control IDs, input hashes, counts and canonical hash. Duplicate JSON keys and non-finite numbers are refused. | A source record changes without review. |
| Each approval block must cover transport only, leave model eligibility undecided and mark no gate as run and no acquisition as done. | The registry is used to claim data was validated or cleared for modeling. |
| Exclusions are rechecked from the exact registry bytes at capture, history planning, batch execution and storage. | An excluded source is collected through another route or an older plan. |
| A receipt naming an earlier registry is checked against a private archived copy that `registry_versions.json` and its lock bind by hash. | Older captures stop verifying or a substituted registry is accepted. |
| Added measure controls sit in a locked companion file bound to the base registry hash. Each names registered sources and a parent and starts on a hold. | Additions rewrite the registry or start with a cleared gate. |
| A held source is stored only when its receipt binds, by SHA-256, the terms-acceptance record or an access-release record naming that source as released. Records are never edited: an extension is a separate record. A receipt is checked against the record it binds and later receipts bind the most recent one. | A held source is stored without its release. Editing a record breaks the receipts bound to it. |
| Receipts of held sources carry the terms record's restrictions and attribution. | Downstream use loses the publisher's conditions. |

### Reviewed Code Versions and Earlier Plan Versions

`code_versions.py`, `legacy_versions.py` and `data_paths.py` keep stored captures verifiable as code and configuration change.

| Check | Failure it prevents |
| --- | --- |
| Each plan-based collector (BLS, Census profile and detailed, HUD API, WONDER, CMS ownership, HCAI, ONC and reference downloads) fingerprints the top-level Python modules in `scripts/acquisition/` (file names containing `e2e` excluded) and the configuration loader. The MMD API collector fingerprints its own fixed module list. General captures (file downloads and the BLS and Illinois API fallbacks), the HUD workbook collector and the Census and MMD browser-export scripts have no fingerprint gate; their checks are in their own sections. | A change to shared code goes unnoticed. |
| A plan-based collector creates captures only when its exact fingerprint set appears in its `config/acquisition/*_code_versions.json` list. The list reader accepts only repository-relative `scripts/` paths with 64-character hashes. A stored capture verifies only while the fingerprint it recorded is still listed, so earlier versions stay listed. | Unlisted code produces evidence. A code change breaks older captures. |
| Tracked plans and records state the current rule. A capture that recorded an earlier version verifies only if `config/acquisition/legacy_versions.json` (with its lock) lists it and the archived bytes still match. A supplementary plan names its parent's hash. | Older captures stop verifying, an altered predecessor is accepted or plan versions mix. |
| Records keep the paths they were written with; code maps them through `config/data_paths.json`. | Moving the dataset folders breaks rechecks. |

### API Keys

The BLS, Census and HUD collectors read keys from AWS Secrets Manager.

| Check | Failure it prevents |
| --- | --- |
| The key is read only at request time, after the caller identity matches the project IAM user. Its shape is checked before use. | The key is read under the wrong identity. |
| The key never appears in plans, receipts, logs, caches or the bucket. BLS sends it in the request body, Census as an unrecorded query parameter and HUD in the `Authorization` header. | A key leaks through a saved URL or record. |
| A response containing the key is refused before anything is written. Only HTTP 200 JSON within a size cap is accepted, redirects are not followed and errors withhold response details. | A publisher echo or error page writes the key or bad data into evidence. |

### Browser Download Origins

Some files can only be saved by a person in a browser. On macOS, Safari and Chrome record the download address on each saved file (`com.apple.metadata:kMDItemWhereFroms`) and the file system records its creation time. The rules below apply to the HUD workbook, WONDER and reference-download collectors. MMD and Census browser exports have their own origin checks, described in their sections.

| Check | Failure it prevents |
| --- | --- |
| The origin is read with `xattr` and must match the plan: exactly HUD's address, WONDER's planned query page or a list that includes the planned reference address. A missing origin holds the file. | A file of another or unknown origin is stored as the publisher's. |
| The file must be a regular file whose SHA-256 and size equal the locked plan. It is copied write-once with an origin proof and reruns use the copy. | A different, edited or re-saved file is stored or the Downloads copy changes later. |
| The receipt records a manual download with no HTTP status or headers and uses the file creation time as retrieval time. Only the planned address is kept, never session identifiers or referring pages. | A browser download claims transport evidence it lacks or session tokens and search terms are stored. |

Off macOS, the end-to-end suites read these two values from a synthetic record, so origin parsing and checks still run.

### Personal Data in Publisher Files

Three sources publish names or contact details next to the organizational data the project needs. Their collectors share these rules.

| Check | Failure it prevents |
| --- | --- |
| The original is downloaded only into a `private_original/` folder (files `0600`, folders `0700`) outside every snapshot. | An original that names people is uploaded or shared. |
| A snapshot may hold only derived files, a download proof and a scope file; `raw/` or `audit/` folders are refused. | The original reaches the bucket through the standard layout. |
| Names found in the original stay in memory, used only to find copies in other cells. Only counts are written. | Personal details are recorded while removing them. |
| Derived files are rebuilt from the local original on every recheck and must match byte for byte. A verified original is reused; a failed download stays private and the item is held. | Derived files drift, downloads repeat or a bad file is stored. |

## API Collectors

### BLS API

`collect_bls_api.py` collects Local Area Unemployment Statistics county series (unemployment, employment and labor force, monthly and annual) from version 2 of the BLS API.

| Check | Failure it prevents |
| --- | --- |
| Each batch holds at most 50 sorted, unique county series of the form `LAUCN<county>000000000<measure>`, spans under 20 years and has an ID equal to the hash of its request. The county list is pinned by hash. | Requests for other series, years or window sizes. |
| A local ledger reserves each request before sending, failed attempts included, under a shared lock. The plan allows at most 450 requests in any rolling 24 hours, below the publisher's 500. A `429` or quota message pauses the run. | The shared key exceeds its daily quota. |
| The response must succeed and return exactly the requested series. Rows must have the expected fields and a requested year and period, with no repeats. A value is a number or `-` with a footnote. | Partial, extra or duplicated data is stored. Unknown tokens are read as numbers. |
| An empty series or a missing year is accepted only with BLS's exact "does not exist" or "No Data Available" message and recorded as an availability hold. A partly missing year stops the batch. Every message must be recognized. | Gaps are hidden, filled in or explained by an unknown warning. |
| The raw JSON is stored unchanged. The CSV keeps published tokens and footnotes. | The derived file alters source values. |

### Census ACS Profiles API

`collect_census_acs_api.py` collects the ACS 5-year data profiles DP02 to DP05 for the 2005 to 2009 period, with DP02PR for Puerto Rico in a supplementary plan, for every county and municipio.

| Check | Failure it prevents |
| --- | --- |
| The plans pin endpoint, period, tables and secret name. The Puerto Rico plan names the main plan's hash. | Another product, period or table is collected. |
| Headers must equal the pinned variable dictionary plus `state` and `county`. Each estimate needs its margin and annotation variables. The stored dictionary must match its hash. | Columns change without notice. |
| Rows must have the header width with string or null cells. State and county codes must match `GEO_ID`, no county may repeat and the county set must equal the pinned list. | Missing, duplicate or mismatched geographies. |
| The CSV writes nulls as `\N` and refuses a cell that already holds it. | Nulls and published text become indistinguishable. |

### Census ACS Detailed Tables and Derived History

`collect_census_acs_detailed.py` collects tables B17001, B16001 and B16004 through the Census API, then derives poverty percentages and limited-English totals for years with no published county table.

| Check | Failure it prevents |
| --- | --- |
| Each batch is bound to its year's endpoint and table and the header to that year's pinned dictionary. Counties must be unique five-digit codes matching `GEO_ID`, at least 3,100 of them. | Another table, year or geography is stored. |
| The "speak English less than very well" lines are chosen by label from each year's own dictionary and pinned in the plan. The loader re-derives them and stops on any difference. | The wrong lines are summed when labels or positions change. |
| Each compared pair (B17001 against the stored S1701 export; B16001 against B16004 in the same year) must cover exactly the same county set. The compared files are pinned by hash. | Comparisons pass on different or changed inputs. |
| Within that set, counties with a non-numeric value on either side are counted as skipped. Every other county is compared: B17001's population and below-poverty counts must equal S1701's and the percentage must match to one decimal place; the B16001 total and universe must equal B16004's. A check passes only with zero mismatches and at least one compared county. Otherwise nothing derived is stored. | A derivation that does not reproduce the published tables is stored. A check passes with nothing compared. |
| A county gets no derived value when any estimate is a negative sentinel, null or text. It also gets none when the denominator is zero. A missing or non-numeric margin keeps the estimate and leaves its approximate margin blank. | Special values are treated as numbers. A valid estimate is dropped for a missing margin. |
| The derived snapshot is built only when every input capture and check passed. Derived CSVs carry numerator, denominator, percentage, the approximate margin of error and source lines. Receipts call the values derived and attach the check report. | A failed input is used. Derived values pass as published ones. |

### HUD Crosswalk API

`collect_hud_api.py` collects the USPS ZIP-to-county crosswalk, nationwide, one request per quarter.

| Check | Failure it prevents |
| --- | --- |
| The plans pin endpoint, crosswalk type, `query=All`, quarters and the terms record by hash. The quarter plan names the first plan's hash. Requests are at least two seconds apart. | Another type, quarter or query runs or HUD's rate limit is exceeded. |
| The raw response is cached before the envelope checks. | A schema change forces repeated requests to examine it. |
| Envelope fields must equal the pinned set (no pagination marker) and echo the requested year, quarter, input and type. | A truncated, paginated or mismatched response is stored. |
| Result fields must equal the pinned set or that set without both `city` and `state`, the same in every row. Omitted fields are never filled in; the receipt names them. | A field change slips through or values are invented. |
| ZIP and county codes stay five-digit text. Two-digit codes are accepted only for the Pacific territories 60, 64, 68 and 70, kept as published and not counted as counties. | Leading zeros are lost or an unknown code passes. |
| Ratios are decimals from 0 to 1 inclusive, kept as published. No ZIP-county pair may repeat and every state and DC must appear. | Invalid weights pass, rows collapse or states are missing. |
| ZIPs whose ratio sums miss 1 by more than 0.01 or equal 0 are counted in the statistics. They are neither rejected nor renormalized. | Business-only ZIPs and small sum gaps are hidden. Valid quarters are refused. |
| Each quarter records its county geography: 2010 Census counties before 2023 Q1 and 2020 Census counties after. Receipts carry HUD's attribution and restrictions. | Boundary eras mix silently or HUD's conditions are dropped. |

### CMS Mapping Medicare Disparities

`collect_mmd_api.py` collects condition-years from the CMS MMD API; `run_mmd_api_collection.py` works through its queue one item at a time. `store_mmd_export.py` stores the earlier browser exports.

MMD differs from the [shared plan rules](#locked-plans-completion-records-and-reruns). Its plans are pinned by fixed SHA-256 values in `mmd_api_contract.py`, not by lock files. The collector takes no OS lock. A finished year is reused through its `capture_ready.json` after the receipt and contract checks pass again. A `completed.json` is accepted when it names that same receipt.

| Check | Failure it prevents |
| --- | --- |
| Only the MMD endpoint on `data.cms.gov` is requested. The collection plans, their registry hash, references and the API-to-browser comparison report are pinned by hash. Each condition must be in exactly one plan, with the year offered and its review hold in place. | Unplanned endpoints, conditions or years are collected. |
| Rows must have exactly the expected fields and echo every selection. FIPS codes must have the expected width (CMS drops some state codes' leading zero; kept as published), be unique and in range. Denominator codes must be known and rates plain non-negative decimals in range. | Unexpected fields, subgroups, geographies or rate tokens. |
| Three probe requests at the start, at an offset and past the end must return the matching rows and an empty end. An empty, tiny or page-size-limited response is refused. | A truncated or reordered response is taken as complete. |
| The derived CSV follows CMS's export format and its selections; an unknown geography stops the item. A saved response without its metadata record stops the run. | The CSV invents names or unrecorded bytes become evidence. |
| Browser exports: every recorded origin must be HTTPS on `data.cms.gov` and all of them are kept in the lineage. The file must be a regular file of at most 16 MiB with the reviewed header, all 13 selections in every row and unique, non-blank FIPS codes. | An export from another site or with mixed selections is stored. |
| No locked plan pins a browser export's hash. The observed hash is recorded and the stored copy must keep it. The retrieval time is the file's modification time, stated as such. | Bytes change during capture. The export claims a transport time it lacks. |

### BLS API Fallback

The general capture path (`capture.py` with `api_fallbacks.py`, run through [Download Batches](#download-batches)) has a separate `bls_api` mode for version 1 of the BLS API. It is distinct from the version 2 collector above.

| Check | Failure it prevents |
| --- | --- |
| The source's registry route must allow an API fallback, the plan must state why the file route is not enough and the version 1 endpoint must be in the registry. | An API is used where the registry does not allow it. |
| A request holds 1 to 25 distinct county series (`LAUCN` plus 15 digits), at most ten years and an explicit list of period codes (`M01` to `M13`). | Requests for other series or oversized windows. |
| The response must succeed with no message and hold one result set with exactly the requested series, each once. Every requested year and period must appear exactly once. No year outside the request may appear. Extra periods inside a requested year are allowed if each appears once. | Warnings, missing periods, extra series or extra years pass as complete. |
| Each batch run reserves its attempts against a local ledger of at most 25 requests in any rolling 24 hours. | The unregistered daily quota is exceeded. |
| Storage rebuilds the allowed request from the plan and rechecks that the stored response covers it. | A receipt whose request drifted is stored. |

### Illinois Hospital Directory API

The general capture path also has an `il_directory` mode for the Illinois hospital report card directory, fetched as numbered pages of 100.

| Check | Failure it prevents |
| --- | --- |
| The endpoint must be in the registry. Every page must report integer counts with the expected page number, a declared page size of 100 and a last page that matches the total. Each page must hold its calculated row count: 100 rows, the final page only the remainder. | A different endpoint or inconsistent paging. |
| The last page and total must stay the same on every page. | The directory changes during capture and pages mix. |
| Each page must hold exactly its expected number of rows. Every hospital ID must be present and unique across pages. | Rows are lost or duplicated. |
| Each next-page link must be the same endpoint with the next page number. The last page must have no next link and the IDs seen must equal the total. Reaching the page limit first is a failure. | Pages are skipped or the capture ends before the directory does. |
| Storage replays the page sequence from the stored pages. | A receipt with a broken page chain is stored. |

## Scripted File Downloads

### Download Batches

`batch.py` runs bounded batches of file downloads built from the registry.

| Check | Failure it prevents |
| --- | --- |
| A batch names the registry hash. Job IDs are unique and every plan passes the capture checks offline before any request. | A malformed or unchecked batch runs. |
| Files over 512 MiB are refused. Each run has a job limit and a byte budget; a job that would exceed it stays pending. A state-root lock prevents parallel batches. | A run grows without bound, truncates a file or collides with another run. |
| A failed or partial capture is stored with the audit-only option as evidence (never as data) and gets no completion record. | Failure evidence is lost. A broken file is promoted. |
| A completed job must match its job and layout hashes. A recovered capture must match the job's source, registry, request, scope, release, periods and expected hash. | A rerun uses a capture taken for another job. |
| A capture of the same pinned file is reused only through an explicit, checksum-bound link. | A file is downloaded twice or reuse crosses scopes. |

### History Routes

Discovery scripts (`discover_history.py`, `discover_publisher_history.py`, `discover_acs_history.py`, `discover_acs_sequences.py` and `discover_california_exports.py`) find earlier releases; `history_routes.py` downloads them.

| Check | Failure it prevents |
| --- | --- |
| Discovery saves each publisher index page and lists only links it contains, with the page's hash as evidence. | Guessed download links. |
| Candidates are added to a validated copy of the registry; the locked registry is never edited. | Discovery changes the reviewed registry. |
| Data from held, privacy-review or large-file-review sources is refused. A held source may supply a reference document only with a `terms` or `access_release` binding, which storage rechecks. | History downloads bypass a hold. |
| Repeated URLs are planned once. Each job reserves twice its size limit against a run budget, with 512 MiB per file. | Duplicate downloads or an unbounded run. |
| A redirect is followed only after `history_redirects.py` confirms it lands on the same resource and file name in the reviewed publisher bucket. | An unreviewed redirect is followed. |
| Without `--execute` every job is validated offline. Each job has a write-once record and a `completed.json` once stored. | Dry runs have side effects. Work is repeated. |

### ZIP Archives

`archive_review.py`, `mapped_archives.py`, `continue_history.py` and `plan_archive_members.py` open ZIP downloads. Storage expands an archive in one of two ways.

| Check | Failure it prevents |
| --- | --- |
| Before expansion the archive must match its captured hash. Member count and expanded size are bounded. Absolute or parent paths, backslashes, duplicates, symbolic links and encrypted members are refused. Written lengths must match the ZIP directory and an extracted member is never overwritten with different bytes. | Path traversal, archive bombs, corrupt members, a swapped archive or changed evidence. |
| Default expansion (no map) opens one level only. A PDF or Word member is stored as a reference, as is a member whose name suggests a dictionary, codebook, layout, readme, method, footnote or manifest; every other member is data. macOS packaging files stay inside the original. A nested ZIP stops storage. | A nested archive is uploaded as table data. |
| Explicit archive references name the parent artifact, member, stored name, role and format. Roles are limited to dictionary, methodology, layout and manifest and each member is at most 20 MiB. | A document is stored under the wrong role or an oversized member is pulled out. |
| Mapped expansion uses an explicit map that names every leaf once, nested archives included to a bounded depth, with its hash, length and role. | Members are dropped, doubled or given the wrong role. |
| In both cases the container stays local; only members are stored. | Unreviewed content reaches the bucket. |

## Browser Downloads

### Census Table Exports

`store_census_export.py` stores ACS table exports from data.census.gov. It does not follow the shared offline-by-default rule: every run reads the Terraform outputs and contacts AWS and a run without a stored capture requests the export address.

| Check | Failure it prevents |
| --- | --- |
| The first recorded browser origin must be the data.census.gov table-download address with only a `download_id` parameter. The table ID must be well formed. | An export from another site or route is stored. |
| The ZIP is bounded (64 MiB, 1 GiB expanded, 10,000 members), CRC-checked and must hold unique members named by the ACS export pattern. Its vintages must equal the selected years, each with data, column metadata and table notes. | Corrupt content, missing years or missing companion files. |
| The script downloads that same export address through the shared transport. The captured archive's SHA-256 and size must equal the browser file. | A browser file that the publisher did not serve is stored. |
| Members are mapped explicitly: each `-Data.csv` as data and the metadata and notes as references. | Companion files are stored as data. |
| Each data file starts with `GEO_ID` and `NAME`. Rows have the header width and unique county IDs. There are more than 3,000 counties. | A partial or non-county export is stored. |
| When a completion record exists, the script rechecks the reconciliation, manifest and mapped members and reads every stored version back live. | A completed export is accepted although its stored objects changed. |

### HUD Crosswalk Workbooks

`store_hud_xlsx.py` stores the ZIP-to-county workbooks for quarters the API does not serve nationwide. The [browser origin checks](#browser-download-origins) apply.

| Check | Failure it prevents |
| --- | --- |
| The origin must be exactly HUD's address and each quarter's hash and size must match the plan. The quarter comes from HUD's file name (`ZIP-COUNTY_<month><year>.xlsx`) and must match the plan. | A different file is stored or a file lands under the wrong quarter. |
| The package must have the exact expected member set, pass CRC checks, stay within 16 MiB (64 MiB for the sheet) and hold no XML declarations. It must have one worksheet and no shared strings, with inline-string text and plain-number ratios in columns A to F only. | Hidden content, macros, formulas or extra columns are read as data. |
| The header must be `zip, geoid, res_ratio, bus_ratio, oth_ratio, tot_ratio` in order. Codes stay five-digit text and ratio text is kept as published, scientific notation included. | Moved columns, lost leading zeros or lost precision. |
| Each ratio, read as a decimal, must be from 0 to 1 inclusive. | An invalid weight passes as a number. |
| Ratio sums equal to 0 or more than 0.01 from 1 are counted in the statistics, as for the API. They are neither rejected nor renormalized. | Imperfect sums are hidden. Valid quarters are refused. |
| No ZIP-county pair may repeat and every state and DC must appear. | Duplicated or incomplete quarters. |
| One quarter has exact repeated rows. A separate locked record, bound to the plan hash and that quarter's download hash, allows dropping exactly the recorded count of rows identical in all six values, from the CSV only. A pair repeated with different values still stops the quarter and every other quarter still refuses repeats. | Repeats are dropped elsewhere, conflicts are collapsed or the exception drifts. |
| Each quarter records its county geography: 2000 Census counties through 2011 Q4 and 2010 Census counties from 2012 Q1. The CSV matches the API layout and receipts bind the HUD terms record. | Boundary eras mix or the two HUD routes diverge. |

### CDC WONDER Exports

`store_wonder_export.py` stores county mortality exports saved from WONDER's Underlying Cause of Death forms. WONDER's API serves national data only. The [browser origin checks](#browser-download-origins) apply.

| Check | Failure it prevents |
| --- | --- |
| Each origin is parsed. Scheme and host must be WONDER's exactly, with no user information or port. Only a `;jsessionid=` segment is removed and the remaining path must be the planned database's query page. | A file from another site or database is stored or a session identifier is saved. |
| The hash and size must match the locked plan, which pins databases, header, query settings, county floor and terms record. | A different or edited export is stored. |
| The notes after the first `"---"` line must name the planned database and list exactly the planned settings: grouped by county and year, totals off, zero and suppressed values shown and rates per 100,000. An optional year line must equal the planned years. | A query with another filter or setting is stored. |
| The header must be the exact 11 published columns. Rows need 11 cells, empty notes, a five-digit county code and a planned year matching its code, with no repeated county-year. Each planned year needs at least 3,100 counties and every state and DC. | Added columns such as an age-adjusted rate, totals rows or truncated exports. |
| Values are numbers or a flag allowed for that column (`Suppressed`, `Unreliable`, `Not Available` or `Missing`), kept as published. A death count from 1 to 9 stops the export. | Flags are lost or recalculated. An export breaks WONDER's suppression terms. |
| The export is stored unchanged with its notes and a county-year CSV beside it. Receipts carry WONDER's data-use restrictions. | The query record is lost or small counts are published downstream. |

### Reference Downloads

`store_reference_download.py` stores data dictionaries and methodology documents that publishers serve only to browsers. The [browser origin checks](#browser-download-origins) apply.

| Check | Failure it prevents |
| --- | --- |
| A request list names each file's source, role (dictionary, methodology or layout), title, publisher, exact origin address and space-free file name. | A file is stored as data or for an unnamed source. |
| `--build-plan` checks that each file's recorded origin includes the planned address, then writes the plan with hashes and sizes and its lock. Storage rechecks origin, hash, size and plan. | A file from elsewhere enters the plan or a file changes after planning. |
| A held source needs a release binding and other sources must not carry one. | A held source's document is stored without its release. |
| Each file has its own completion record (with no row count) and is stored once; a failing file is held while the others continue. | Files are stored twice. One file blocks the rest. |

## Sources With Personal Data

### CMS Hospital Ownership

`store_cms_owners.py` collects the monthly CMS Hospital All Owners releases and stores organization rows only. The [personal data rules](#personal-data-in-publisher-files) apply.

| Check | Failure it prevents |
| --- | --- |
| Each release is bound to its registry route, period and layout. Exactly two published headers are accepted, each pinned to its releases. | A changed file or layout goes unnoticed. |
| Only type `O` (organization) rows are kept; a type other than `I` or `O` stops the release, as does a kept row with a personal name or title. Name, title, street address, city and ZIP columns are dropped. | An individual's row or address reaches the bucket. |
| Every kept organization row must have a non-blank enrollment ID and the release must keep at least one organization row. | Rows that cannot link to a hospital are stored. An empty result passes. |
| An organization or doing-business-as name that contains the whole name (two or more words) of an individual listed in the same release is replaced with `[REDACTED]`. | An individual is named through a company name. |
| Private-equity and REIT flags exist only in the later layout and are never added to earlier releases; values must be `Y`, `N` or blank. Kept cells are written as published, with no trimming or zero-filling. | Flags are invented or values change during filtering. |
| UTF-8 is tried first, then Windows-1252; the encoding is recorded. Rows read must equal rows dropped plus rows kept. | Names are garbled or rows are lost silently. |

### HCAI Utilization

`store_hcai_util.py` collects California hospital annual utilization workbooks and stores one abstracted CSV per sheet. HCAI is on access hold; storage accepts it only with the access-release record. The [personal data rules](#personal-data-in-publisher-files) apply.

| Check | Failure it prevents |
| --- | --- |
| Each year is bound to its address, reviewed signed-redirect target, hash, size, sheet names and data header. | A revised or different file is used. |
| The package is read with bounded sizes, CRC checks and an event parser that refuses XML declarations. Formulas and unknown cell types stop the year. Cells beyond the header must be empty in the header row and in every hospital data row; only the four publisher label rows may extend beyond it. | A crafted workbook is parsed unsafely or formulas become values. |
| Every column whose name looks personal (NAME, PHONE, FAX, MAIL, ADDR, PREP, CONTACT, TITLE or SIGN) must be on the redaction list or the reviewed not-personal list. | An unreviewed personal column slips through. |
| Facility street address and phone, administrator and preparer names and the parent company's business address are redacted. Rows whose licensee is an individual investor also lose the parent name, city and ZIP code. | A person is named directly or as an individual investor. |
| Any other cell containing a collected name (two or more words), an email address or a phone number is redacted and counted. The four label rows are not read as names. | Names or contact details typed into other fields survive. |
| The data sheet needs at least 400 hospitals. Values stay as stored. The preliminary year is labeled and fails its binding if HCAI replaces the file. | A truncated workbook is stored or preliminary data passes as final. |

### ONC Meaningful Use

`store_onc_mu.py` collects the Medicare meaningful-use attestation file and stores hospital rows only. The [personal data rules](#personal-data-in-publisher-files) apply.

| Check | Failure it prevents |
| --- | --- |
| The plan binds the download's address, hash and size and raises the size and time limits for this one large file, which is read as a stream. The header must be exact and rows the same width. | A different file is used, memory runs out or the layout changes. |
| Provider types must be `Hospital` or `EP`; only hospital rows are kept and each must have an empty `Specialty` and a CCN. `Specialty` is dropped. | Clinician rows or columns reach the bucket. |
| Rows read must equal rows dropped plus rows kept and every full program year must have hospital rows. Receipts keep this series separate from the Promoting Interoperability file. | Rows are lost or the two ONC series are joined. |

### Public-Business CSV Abstraction

`abstract_public_business_csv.py` writes a privacy-filtered local copy of a publisher CSV under a policy file. It uploads nothing. The policy's retention value (`abstracted_only` or `raw_private_and_abstracted`) records what later storage may keep.

| Check | Failure it prevents |
| --- | --- |
| The policy must be complete and accepted. It pins source address, input hash, size limit, exact headers, row count, `[REDACTED]` as the replacement and the fields to redact and to scrub. Both field lists must be non-empty, inside the header and disjoint. | An unreviewed policy, a different file or an ambiguous field scope is used. |
| The header must match the policy, rows must have its width and the row count must match. The input hash is checked before and after reading. | Schema drift hides a personal column or the file changes mid-read. |
| Every non-blank cell in a redact field is replaced with `[REDACTED]`. | A listed personal field survives. |
| The values from redact fields (except placeholders such as `n/a`) are collected in memory and normalized for case, width and spacing. They are searched as whole words in the scrub fields and matches are replaced. Each collected value is at most 4,096 characters and at most 100,000 values are held. | Names or contact details copied into text fields survive. A crafted file exhausts memory. |
| The manifest marks free-text privacy review as still pending. | The abstraction is mistaken for a full privacy review of free text. |
| Output is staged and published only when complete; a conflicting existing output stops the run. | A partial or conflicting copy is used. |

## Checks After Storage

### Storage Records Check

`verify_storage_records.py` compares every object version in the project bucket with the local storage records, read-only. It compares sizes; content is verified at upload by the version readback.

| Check | Failure it prevents |
| --- | --- |
| The version listing must be complete and well formed, with no duplicate identities. | A truncated listing passes as complete. |
| Records that name the same key and version with different sizes fail the check. | Conflicting evidence goes unnoticed. |
| Each recorded version must be live with its size unless a retirement record lists it. | Missing or wrong-size objects go unnoticed. |
| Every retirement entry must have status `deleted_verified` or `already_absent` and retirement records must agree on sizes. Retired versions must be absent, with no delete marker either. Replacements must be live with their size. | A deletion that was never verified counts as retired. A retired version is still present. |
| A version known only from a saved S3 inventory must still be present, as a live object version or as a delete marker, unless it is retired. A retired version must be neither. | Objects listed in an inventory disappear unnoticed. |
| Live versions no record or inventory names are unrecorded. Versions under the top-level `lakehouse/` folder are Iceberg table files, counted apart; upload records naming such keys are still checked. | Untracked objects go unnoticed or table files are reported as gaps. |
| The `e2e`, `conformance` and `schema_review` folders are skipped only at the top of the evidence root and reported with reasons and counts; deeper folders with those names are still read. Folders named `private_original`, `storage_checks` and `redacted_copies` are skipped at any depth. | Synthetic files block the check, private originals are read or the exclusion hides evidence. |
| JSON over 50 MB or unparseable stops the check unless it has a recorded disposition. | Bad evidence is skipped silently. |

### Manifest Corrections

`correct_manifest_roles.py` writes corrected storage manifests when an object was stored under the wrong role.

| Check | Failure it prevents |
| --- | --- |
| A committed specification (`config/acquisition/manifest_corrections.json`) names each manifest by key, version and SHA-256 and each change by object, version, old role and target role. The stored manifest must match. | The wrong manifest is corrected. |
| Only the named `role` fields change; `supersedes` and `correction` are added and every other field is kept. | A correction changes more than the role. |
| The corrected manifest is a separate object keyed by its own hash, with no time in its bytes, so a rerun verifies instead of writing. The original stays. | The capture record is lost or reruns write duplicates. |
| A dry run is the default. Writing needs `--store` with the specification's SHA-256; each write is read back and recorded locally for the storage check. | Unreviewed, unverified or unrecorded writes. |

### Redownload Checks

`redownload.py` and `redownload_controls.py` download every queued source again into a separate folder and compare the result with the stored capture. They never write to the bucket and never replace a stored capture.

**Controls and budgets.**

| Check | Failure it prevents |
| --- | --- |
| The queue must match its lock and every queued receipt must be unchanged. | The run compares against edited receipts. |
| A controls snapshot binds the hashes of every acquisition module, the process and configuration modules, `requirements.txt`, every `config/acquisition/*.json` (code-version lists included), the path map `config/data_paths.json` (as `path_map_sha256`), the queue inputs, job files, the local settings file (hash only), the source scope, state root and limits. A passing independent review must bind the snapshot's hash; any change invalidates it. | Unreviewed code, plans or settings run. |
| The path map is checked against its reviewed hash again before each collector or plan dispatch and each receipt read. A change during the run stops it with "Path map changed" before any request. | A moved data folder redirects the run after review. |
| The state root must be inside the redownload folder, outside every stored collection and free of symbolic links. Its `run.json` binds one queue and one controls snapshot; another root invalidates the review. | The run writes into stored evidence or resets budgets by switching folders. |
| The first unit per source, route and mode is a pilot. Non-pilot units run only after every pilot in the selected reviewed scope has an exact match or a recorded review, so one pending pilot holds non-pilot work for all sources in that scope. | One systematic problem repeats across many units. |
| A non-pilot unit also needs its own pilot matched or reviewed. Any outcome other than an exact match stops that source until a resume names the reviewed outcomes. | A source keeps running after a mismatch. |
| Attempts (at most two per address) and bytes received or persisted are reserved durably against a 128 GiB cap; interrupted reservations stay charged and free disk is checked. BLS requests share one rolling 450 limit across stored and fresh ledgers. HTTP `429` pauses the source with its `Retry-After` delay. AWS calls are limited to identity and key reads. | Restarts reset budgets, the disk fills, quotas are exceeded or the bucket changes. |

**Units.** File units use the shared transport. Paged API units are fetched page by page and compared by hash. Collector units run the collector's capture path into the fresh folder with storage off, then compare data artifacts by role and name. Manual units are read from a separate downloads folder. A WONDER export with the planned bytes runs the full collector; any other runs the collector's origin and export checks and is compared with the stored export by bytes. MMD browser exports that moved to the API match through an explicit file-name map. Fresh originals with personal data stay private and are listed with hashes for deletion. Failures keep only fixed categories and reviewed code locations.

**Content comparison.** When fresh bytes differ from the stored file, `compare_files` decides whether the content still matches. Two kinds of difference can match. A recognized archive may differ in packaging (entry timestamps, compression or order) when every member's name and content and every comment match. Inside a member or file, the only difference that can match is an Office build time. Any other difference needs review. The steps run in this order:

1. The stored file's SHA-256 must equal its receipt's artifact hash. Otherwise the result is `changed_needs_review` with the entry "(stored copy differs from its receipt)" and no content comparison, date masking or archive expansion takes place.
2. Identical fresh and stored bytes match, with no excluded parts.
3. The cases below apply.

| Case | How it compares |
| --- | --- |
| An archive: the stored file name ends in `.zip`, `.xlsx` or `.docx` and both files begin with a ZIP local-file header (the fresh download's own name is not checked) | Entry by entry, by name and SHA-256, directories included, within safe bounds. A differing member is compared by the rules below using its path. The archive comment and every entry comment compare by bytes. |
| An Office member at exactly `docProps/core.xml` (up to 1 MiB) | Compared as raw text. A safe parse (defusedxml) only confirms that the root has exactly one `dcterms:created` and one `dcterms:modified` child holding a real UTC time in the form `YYYY-MM-DDThh:mm:ssZ` and carrying exactly one attribute, `xsi:type="dcterms:W3CDTF"`. The root must be the Office `coreProperties` element. The `cp`, `dcterms` and `xsi` prefixes must all be declared. Every declaration of them at any depth must bind its Office namespace. Each element's exact text must occur once in the file; only those two timestamps are masked (`document_build_time`). Comments, processing instructions, namespace declarations, attributes and whitespace still count. If any condition fails or the member starts with a UTF-8 byte-order mark, the member compares by bytes. |
| Every other file, Census table notes, WONDER exports and saved web pages included | By bytes. A new access or query date in Census notes or a WONDER export is `changed_needs_review`. |

Masking applies only to strict UTF-8 text. Any other difference is `changed_needs_review`. File, manual and collector comparisons record `excluded_parts`; the report counts matches by excluded part and treats outcomes without that field (paged API units, transport failures and blocked collector runs) as having no exclusions.

A manual file counts only when it was created after the run started and one of its recorded origins is the unit's exact address: scheme, host, port, path and query must match after removing only the fragment and a `;jsessionid=` segment. An absent port means the scheme default; an explicit port compares as written. A matching file wins. Otherwise the first changed file from that address is kept and flagged `changed_needs_review`. Files from other addresses, even on the same host, are ignored.

| Comparison rule | Failure it prevents |
| --- | --- |
| Inside `compare_files`, the stored copy must match its receipt before any content or archive comparison. A fresh file or collector artifact whose hash equals the receipt's hash matches without reading the stored copy. Paged API units compare page hashes with their receipt. | An altered stored copy is matched against a fresh download. |
| Masking covers one format only: the two confirmed build times in `docProps/core.xml`, each a valid UTC time. Census notes and WONDER exports compare by bytes. | A date in a data field, footnote or caveat is masked. A non-date value is read as a date. Changed data matches. |
| `docProps/core.xml` compares as raw text, not a normalized form. | A change in comments, declarations or attributes is hidden. |
| Archive entries and comments compare by bytes, directories included. Only true named archives are expanded. | A changed entry or comment is ignored or a page with an appended archive is read as one. |
| Saved web pages compare by bytes. | Normalization hides changed text, links or scripts. |
| Comparison results name the parts they left out. | Exclusions are applied silently. |
| Manual files must come from the unit's exact address. | A file from a neighboring address is attributed to the unit. |

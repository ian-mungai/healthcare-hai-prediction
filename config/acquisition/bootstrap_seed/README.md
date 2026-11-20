# Bootstrap seed

The files a bootstrap run in an empty environment cannot fetch from a publisher or from S3: public captures that only a person could download. Git LFS stores them (`.gitattributes`). `scripts/quality/repo_checks.py` declares this folder as the one data folder besides the test fixtures and the dbt seeds. It admits data files there, the 4 AHRQ technical documents (PDF) and no other kind of file: the writing, front matter and link checks still read the Markdown here. The 16 plan input files (the 12 MMD browser-history captures, the 3 MMD reference files and `county_adjacency2010.txt`) are in the folder, so a host build reads them from the code archive. The 50 HUD and WONDER manual-unit originals are not added yet. The 10 AHRQ files are added after the build. The check passes without them unless `--require-files` is set. The bootstrap's manual download and upload steps are the fallback for the 50 HUD and WONDER originals only. An AHRQ unit has no fallback: the host reads its file from this seed alone.

`manifest.json` lists every expected file, the queue it belongs to and a SHA-256 where a tracked source holds one:

| Folder | Files | Digest source |
| --- | --- | --- |
| `mmd_browser_history/` | the 12 MMD browser-history CSV captures (plan inputs) | the comparison report in the bundle |
| `mmd_references/` | `codebook_crosswalk.csv`, `countynames.tsv`, `urban.tsv`, which the MMD collector reads to build its requests and derived CSV files | the MMD plan |
| `structure_history/` | `county_adjacency2010.txt`, a Latin-1 text file | the geography file period seed (`dbt/seeds/geography_file_periods.csv`), whose row for the capture names the file's SHA-256 |
| `manual_units/<snapshot ID>/` | the original of each of the 44 HUD units (its ZIP-COUNTY workbook) and of each of the 6 WONDER units (its text export), as the publisher served it | the HUD plan for the 44 workbooks, the WONDER plan for the 6 originals |
| `manual_units/<snapshot ID>/` (AHRQ) | the file of each of the 10 AHRQ Compendium units: the 2016 `.xls`, five `.xlsx` workbooks and four `.pdf` technical documents, saved by the owner in a browser on Nov 8 2026. The 2018 workbook is a copy without its workstation path element. Three further technical documents are left out | the AHRQ plan (SHA-256, size, origin URL) |

An AHRQ unit (mode `ahrq_compendium`) is a seed-only unit. It has no derived file and no earlier capture. The run never requests its URL, so `www.ahrq.gov` is not on the approved hosts file. The host binds the seeded file to the AHRQ plan by SHA-256 and size and captures it as it is. Three cases make only their unit stale: a file absent from the seed, a link instead of a file and a file the plan does not bind. The owner's notice lists the unit. Each unit's specification records when and from where the owner saved the file. The 2018 unit also records the SHA-256 and size of the workbook as saved and the change that made the copy. The plan binds the copy. AHRQ publishes no terms text for these files: a push of the seed publishes them. The terms stay an open item. Three further technical documents that the owner saved (`chsp_linkage_file_tech_doc.pdf`, `2020-hospital-linkage-techdoc.pdf` and `2023-hospital-linkage-techdoc.pdf`) are not seeded: a text stream of each holds a user directory path and the repository is public. They stay in the owner's download folder and have no unit and no plan entry. The release record `config/acquisition/access_releases_20261108.json` is append-only and counts the owner's 13 downloads (6 workbooks and 7 technical documents), so it stays as written: the plan binds 10 of them. The privacy scan refuses a PDF that holds such a path.

A unit's derived file is not seeded: neither a HUD `crosswalk.csv` nor a WONDER `county_year.csv`. The bootstrap rebuilds it on the host from the original with the unit's contract (the HUD workbook contract or the WONDER export contract) and captures the unit only when the rebuilt file's SHA-256 and size equal the ones the unit's last capture records in `capture_specs.json`, so the unit's completion record matches. A mismatch makes the unit stale and stores nothing. The check recomputes this for every HUD workbook and WONDER original present.

An entry without a digest is pending: add the digest when the file is added. No entry is pending now. The check refuses a present file without a digest, a file with another digest and a file the manifest does not list. The five reference documents and the 16 ACS units are not part of the seed. The ACS units move to the Census API collector. No seed file is pinned in S3.

```sh
python -m scripts.acquisition.check_baseline_run                 # manifest, queue binding, digests of present files, LFS rule
python -m scripts.acquisition.check_baseline_run --require-files  # also demands every file without a hold
python -m scripts.acquisition.build_seed_manifest                 # rebuild the manifest from the bundle after a reviewed queue change
```

Every file must pass the privacy scan before it is committed. A CI job that does not need the seed does not fetch it through LFS.

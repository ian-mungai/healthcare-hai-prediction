# Bootstrap seed

The files a bootstrap run in an empty environment cannot fetch from a publisher or from S3: public captures that only a person could download. Git LFS
stores them (`.gitattributes`), and `scripts/quality/repo_checks.py` declares this folder as the one data folder besides the test fixtures and the dbt seeds
(owner decision, Nov 12 2026). No file of the folder is added yet. They are added after the owner's read-only download of the stored copies from S3.

`manifest.json` lists every expected file, the queue it belongs to and a SHA-256 where a tracked source holds one:

| Folder | Files | Digest source |
| --- | --- | --- |
| `mmd_browser_history/` | the 12 MMD browser-history CSV captures (plan inputs) | the comparison report in the bundle |
| `mmd_references/` | `codebook_crosswalk.csv`, `countynames.tsv`, `urban.tsv`. Held: pending owner decision, because the MMD collector reads them | the MMD plan |
| `structure_history/` | `county_adjacency2010.txt`. Held until the removed-names hook reads Latin-1 files | none yet |
| `manual_units/<snapshot ID>/` | the stored data files of the 66 manual units (44 HUD, 16 ACS, 6 WONDER) | the HUD plan for the 44 workbooks, none for the other files |

An entry without a digest is pending: add the digest when the file is added. The check refuses a present file without a digest, a file with another digest and a file
the manifest does not list. The five reference documents are not part of the seed, and no seed file is pinned in S3.

```sh
python -m scripts.acquisition.check_baseline_run                 # manifest, queue binding, digests of present files, LFS rule
python -m scripts.acquisition.check_baseline_run --require-files  # also demands every file without a hold
python -m scripts.acquisition.build_seed_manifest                 # rebuild the manifest from the bundle after a reviewed queue change
```

Every file must pass the privacy scan before it is committed. A CI job that does not need the seed does not fetch it through LFS.

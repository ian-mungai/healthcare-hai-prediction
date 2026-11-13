# Bootstrap seed

The files a bootstrap run in an empty environment cannot fetch from a publisher or from S3: public captures that only a person could download. Git LFS stores them (`.gitattributes`). `scripts/quality/repo_checks.py` declares this folder as the one data folder besides the test fixtures and the dbt seeds. It admits data files there and no other kind of file: the writing, front matter and link checks still read the Markdown here. No file of the folder is added yet. They are added after the owner's read-only download of the stored copies from S3.

`manifest.json` lists every expected file, the queue it belongs to and a SHA-256 where a tracked source holds one:

| Folder | Files | Digest source |
| --- | --- | --- |
| `mmd_browser_history/` | the 12 MMD browser-history CSV captures (plan inputs) | the comparison report in the bundle |
| `mmd_references/` | `codebook_crosswalk.csv`, `countynames.tsv`, `urban.tsv`, which the MMD collector reads to build its requests and derived CSV files | the MMD plan |
| `structure_history/` | `county_adjacency2010.txt`. Held until the removed-names hook reads Latin-1 files | none yet |
| `manual_units/<snapshot ID>/` | the stored data files of the 44 HUD units plus, for each of the 6 WONDER units, its original text export only, as the publisher served it | the HUD plan for the 44 workbooks, the WONDER plan for the 6 originals, none for the other files |

A WONDER unit's `county_year.csv` is not seeded. The bootstrap rebuilds it on the host from the original with the WONDER contract and captures the unit only when the rebuilt file's SHA-256 and size equal the ones the unit's last capture records in `capture_specs.json`, so the unit's completion record matches. A mismatch makes the unit stale and stores nothing. The check recomputes this for every WONDER original present.

An entry without a digest is pending: add the digest when the file is added. The check refuses a present file without a digest, a file with another digest and a file the manifest does not list. The five reference documents and the 16 ACS units are not part of the seed. The ACS units move to the Census API collector. No seed file is pinned in S3.

```sh
python -m scripts.acquisition.check_baseline_run                 # manifest, queue binding, digests of present files, LFS rule
python -m scripts.acquisition.check_baseline_run --require-files  # also demands every file without a hold
python -m scripts.acquisition.build_seed_manifest                 # rebuild the manifest from the bundle after a reviewed queue change
```

Every file must pass the privacy scan before it is committed. A CI job that does not need the seed does not fetch it through LFS.

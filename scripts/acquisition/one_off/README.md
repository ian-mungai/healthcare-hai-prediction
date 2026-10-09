# One-Off Acquisition Tools

Tools that ran once (or once per run) during the acquisition stage. They read and write records under `data/`, which stays out of Git; the records are the evidence of each run. Run them from the repository root as the usage line in each file says, usually `.venv/bin/python -m scripts.acquisition.one_off.<folder>.<tool>`.

The acquisition stage's source code and dependencies are tracked in the repository; run outputs and datasets stay out. The tools came here from their `data/` folders. Each file keeps its run-time content except path anchors, usage lines and the fixes the repository checks require, such as type hints and reading the owner's address from local Git settings. `data/acquisition_planning/code_moved_20261004.json` maps each old path to its new one.

| Folder | Tools | Records |
| --- | --- | --- |
| `redownload` | Queue builders for runs 1 and 3 plus the run 3 scope match | `data/redownload_checks/`, `data/acquisition_planning/run3_scope_20261004/` |
| `privacy_review` | Pattern and document scans, redaction, the listed-version deletion and their E2E checks | `data/privacy_review/20260927/` |
| `storage_dedup` | S3 manifest inventory, duplicate deletion list and the checked deletion (`delete_duplicate_versions.py`) | `data/lakehouse_planning/dedup_20261003/` |
| `datasets_move` | The move of the local dataset folders into `data/datasets/` (`move.py`) | `data/lakehouse_planning/datasets_move_20261004/` |
| `cms_admin` | CMS administrative batch build, local capture and screen | `data/acquisition_planning/cms_admin_20260929/` |
| `closeout` | Clean-checkout gate and replay checks | `data/acquisition_planning/closeout_*/` |

The acquisition gate lints and type-checks these tools but leaves them out of its coverage target (`config/acquisition/coverage.ini`). Their run records show what they did.

"""Where the locked AHRQ Compendium plan lives and how a planned file is identified.

AHRQ refuses scripted downloads (the registry holds AHRQ_SYSTEM for that reason), so the owner saved each file by hand. A locked plan binds
every file to its exact origin URL, SHA-256 and size: the six linkage workbooks (2016, 2018, 2020, 2021, 2022 and 2023 vintages) and the four technical
documents (three more that the owner saved are left out: each holds a user directory path). The files are seed-only manual units of the mode
ahrq_compendium (``manual_content``): the bootstrap seed holds the files and the run binds each one to this plan.
The 2018 workbook is seeded as a copy without its workstation path element (see ``ahrq_seed_copy``), so the plan binds the copy and not the file as saved.
The owner lifted the access hold for these files on 8 Nov 2026 by one append-only release record, and the base registry stays unchanged.
The seed README holds the terms evidence and the use restrictions.
"""

from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash

PLAN_PATH = REPO_ROOT / "config/acquisition/ahrq_compendium_plan.json"


def file_id(entry: dict) -> str:
    """Identify a planned file by everything bound to it, including its bytes."""
    return canonical_hash({k: v for k, v in entry.items() if k != "id"})

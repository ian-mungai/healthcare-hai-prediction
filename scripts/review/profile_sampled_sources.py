"""Full-row member review of the 15 sources whose non-key columns were only sampled (first 5,000 records).

Runs the existing member review (``inspect_members``) unchanged, with candidate grain keys added for these sources.
Every tabular member is read in full; the output holds header names that pass the contact-pattern filter, category
counts, identifier shapes and duplicate-key counts, never cell values.

Usage (the interpreter needs openpyxl, pypdf and xlrd)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.profile_sampled_sources --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output sampled_run1
"""

from __future__ import annotations

import sys
from typing import Any, cast

from scripts.review import inspect_members

SOURCES = (
    "ADJ",
    "CMS-MUP-DRG",
    "CMS_CHOW",
    "CMS_GV",
    "CMS_MEDICARE_PROVIDER",
    "CMS_OWNERS",
    "CMS_POS",
    "ENROLL",
    "HCAI_UTIL",
    "HHS",
    "HSA",
    "IL",
    "ONC_PI",
    "PLACES",
    "SVI",
)
GRAIN_KEYS: dict[str, tuple[tuple[str, ...], ...]] = {
    "ADJ": (("county_geoid", "neighbor_geoid"),),
    "CMS-MUP-DRG": (("rndrng_prvdr_ccn", "drg_cd"),),
    "CMS_CHOW": (("enrollment_id_buyer", "enrollment_id_seller"),),
    "CMS_GV": (("year", "bene_geo_lvl", "bene_geo_cd", "bene_age_lvl"),),
    "CMS_MEDICARE_PROVIDER": (("rndrng_prvdr_ccn",),),
    "CMS_OWNERS": (("enrollment_id", "associate_id_owner"),),
    "CMS_POS": (("prvdr_num",),),
    "ENROLL": (("enrollment_id",),),
    "HCAI_UTIL": (("fac_no",),),
    "HHS": (("hospital_pk", "collection_week"),),
    "HSA": (("medicare_prov_num", "zip_cd_of_residence"),),
    "IL": (("entity_id", "measure_id"),),
    "ONC_PI": (("facility_id",), ("ccn", "program_year")),
    "PLACES": (("year", "locationid", "measureid", "datavaluetypeid"),),
    "SVI": (("fips",),),
}


def main() -> int:
    """Add the grain keys, then run the member review CLI for these sources only."""
    # KEYS is inferred narrowly from its literal; any tuple of normalized column names is a valid key.
    cast(dict[str, Any], inspect_members.KEYS).update(GRAIN_KEYS)
    if "--sources" not in sys.argv:
        sys.argv.extend(["--sources", *SOURCES])
    return inspect_members.main()


if __name__ == "__main__":
    sys.exit(main())

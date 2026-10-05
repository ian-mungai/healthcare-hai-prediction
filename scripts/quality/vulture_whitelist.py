"""Names the unused-code check (vulture) cannot see being used. Each entry is used, or kept, for the reason beside it.

vulture reads this file with every other tracked Python file, so a name used here counts as used everywhere. Add an
entry only after tracing the use; a name that is truly dead is deleted instead. An entry for a removed name fails Ruff,
MyPy and the removed-names check. This file is never imported.
"""

from __future__ import annotations

import tarfile

from scripts.acquisition import wonder_export_contract
from scripts.acquisition.discover_history import Links
from scripts.acquisition.hcai_util_contract import _Markup
from scripts.acquisition.tests.conftest import synthetic_collection_routes
from scripts.acquisition.transport import Download


def uses(download: Download, member: tarfile.TarInfo) -> tuple[object, ...]:
    """Name each entry once; vulture counts these names as used."""
    return (
        Links.handle_data,  # HTMLParser calls it for the text inside each anchor.
        _Markup.handle_data,  # HTMLParser calls it for workbook XML text.
        _Markup.handle_startendtag,  # HTMLParser calls it for self-closing workbook XML tags.
        synthetic_collection_routes,  # pytest runs autouse fixtures without a reference.
        download.requested_url,  # transport.download writes it to the transport metadata through dataclasses.asdict.
        member.linkname,  # tarfile reads it when the quality E2E builds its symbolic-link archive.
        # Pending: dead, but the file is in 9 collectors' approved code hashes; delete it at the next WONDER code version.
        wonder_export_contract.RATE_COLUMNS,
    )


_ = uses

"""The bronze cutover's name for the container probe, which now lives in the permanent ``scripts.lakehouse.route_probe``.

The temporary cutover imports the probe from here; the permanent route check (``scripts.lakehouse.cutover_route``) pipes ``route_probe.py`` itself.
"""

from __future__ import annotations

import sys

from scripts.lakehouse.route_probe import HOST_NAME, host_targets, main, report

__all__ = ["HOST_NAME", "host_targets", "main", "report"]

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

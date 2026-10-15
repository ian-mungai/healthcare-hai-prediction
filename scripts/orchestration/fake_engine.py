"""A fake Docker Engine for the orchestration runners: the lakehouse fake daemon plus an image pull, the dropped capabilities and security options
that a create request sets, and a service that never turns healthy (failure modes 933 and 934). It serves only test registries."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from scripts.lakehouse.fake_docker import FakeDocker, Handler


class StackDocker(FakeDocker):
    """The fake daemon with the routes and reports the orchestration stack needs."""

    def __init__(self, path: Path, mem_total: int = 32 * 1024**3, cpus: int = 8) -> None:
        super().__init__(path, mem_total, cpus)
        # A message makes every pull fail with it, as a daemon refuses an unknown manifest.
        self.pull_error = ""
        # The orchestration service whose health check never passes.
        self.unhealthy_service = ""
        # A pulled reference (name@digest) also makes these references present, as the real daemon resolves a tagged reference by its digest.
        self.pull_aliases: dict[str, list[str]] = {}

    def dispatch(self, handler: Handler, method: str, parts: list[str], query: dict[str, str], body: Any) -> None:
        """``POST /images/create`` pulls an image by digest; every other route is the base daemon's."""
        if parts[:2] == ["images", "create"] and method == "POST":
            reference = f"{query.get('fromImage', '')}@{query.get('tag', '')}"
            self.checkpoint("pull", "before", {"reference": reference})
            if self.pull_error:
                handler.reply(200, {"error": self.pull_error})
                return
            with self.lock:
                image = {"Id": "sha256:" + os.urandom(8).hex(), "RepoDigests": [reference], "Config": {"Labels": {}, "Env": [], "Cmd": []}}
                for name in [reference, *self.pull_aliases.get(reference, [])]:
                    self.images.setdefault(name, image)
            handler.reply(200, {"status": f"Digest: {query.get('tag', '')}"})
            return
        super().dispatch(handler, method, parts, query, body)

    @staticmethod
    def inspect_form(container: dict[str, Any]) -> dict[str, Any]:
        """The daemon reports the dropped capabilities and security options the create request set."""
        form = FakeDocker.inspect_form(container)
        host = (container.get("_body") or {}).get("HostConfig", {})
        form["HostConfig"] = {**form["HostConfig"], "CapDrop": host.get("CapDrop"), "SecurityOpt": host.get("SecurityOpt")}
        return form

    def set_health(self, container: dict[str, Any]) -> None:
        """Mark a running container healthy, except the service the test chose."""
        if self.unhealthy_service and container["Config"]["Labels"].get("hai.orch_service") == self.unhealthy_service:
            return
        super().set_health(container)

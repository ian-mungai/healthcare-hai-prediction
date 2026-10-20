"""Check lineage service create specifications without starting containers."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.lakehouse.run_lock import Registry

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    output = ROOT / "data/e2e/lineage_service_render" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    result: dict[str, Any] = {"boundary": "render-only, no launcher daemon or container", "pass": False}
    try:
        if "lineage_service" not in launcher.TEMPLATES:
            raise AssertionError("lineage service template is not registered")
        if not {"lineage_up", "lineage_down"} <= set(launcher.METHODS):
            raise AssertionError("lineage service lifecycle methods are not registered")
        template = launcher.TEMPLATES["lineage_service"]
        if template.finite or template.owners != ("stack",):
            raise AssertionError("lineage service has the wrong lifecycle or owner")
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for name in ("postgres", "private", "config"):
                (root / name).mkdir()
            config = launcher.Config(Registry(root / "registry", "test"), ROOT, "/unused", root / "tokens", root / "runs", allowed_roots=(root,))
            renderer = launcher.Renderer(config, launcher.ComposeModel(ROOT / "services/lineage/docker-compose.lineage.yaml"), ROOT, ROOT, b"synthetic")
            values = {"HAI_LINEAGE_STATE": str(root), "HAI_LINEAGE_API_PORT": "15000", "HAI_LINEAGE_WEB_PORT": "13000"}
            values.update({f"HAI_LINEAGE_MEM_{name}": str(1024**3) for name in ("POSTGRES", "API", "WEB")})
            bodies = {}
            for service in ("postgres", "api", "web"):
                inp = launcher.RenderInput("lineage_service", "lineage", service, 1, 1, "op-synthetic", values, 1, [], {"service": service}, {})
                rendered = renderer.render_lineage_service(inp, service, "12345678-synthetic")
                body = rendered.body
                if rendered.finite or body["HostConfig"]["Memory"] != int(values[f"HAI_LINEAGE_MEM_{service.upper()}"]):
                    raise AssertionError("service lifecycle or memory plan was lost")
                ports = body["HostConfig"].get("PortBindings", {})
                if service == "postgres" and ports:
                    raise AssertionError("database port was published")
                if any(binding["HostIp"] != "127.0.0.1" for bindings in ports.values() for binding in bindings):
                    raise AssertionError("service port escaped loopback")
                if "@sha256:" not in body["Image"]:
                    raise AssertionError("service image is not pinned")
                bodies[service] = body
            invalid = launcher.RenderInput("lineage_service", "lineage", "invalid", 1, 1, "op-synthetic", values, 1, [], {"service": "invalid"}, {})
            try:
                renderer.render_lineage_service(invalid, "invalid", "12345678-synthetic")
            except launcher.LauncherError:
                pass
            else:
                raise AssertionError("unknown service was accepted")
            result.update({"pass": True, "specifications": bodies})
    except (AssertionError, ValueError, OSError, launcher.LauncherError) as error:
        result["error"] = str(error)
    result["launcher_sha256"] = hashlib.sha256((ROOT / "orchestration/launcher.py").read_bytes()).hexdigest()
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(json.dumps({"pass": result["pass"], "report": str(output / "report.json"), "error": result.get("error")}) + "\n")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

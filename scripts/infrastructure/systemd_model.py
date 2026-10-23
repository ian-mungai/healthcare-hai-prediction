# A small model of the systemd behavior the host boot script depends on. Like systemd, it reads each directive only in the
# section that defines it and ignores a misplaced one with a warning, which is why a dependency in the wrong section
# silently stops being a dependency. Its dependency rules follow systemd.unit(5): stopping or restarting a unit also
# stops or restarts the units that require it (Requires= and BindsTo=) and the units that are PartOf= it, dependents
# first; a reload reaches the units that list the reloaded unit in ReloadPropagatedFrom=. It does not model failure
# restarts, timers, sockets, job queues or parallel starts.
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path

Sections = dict[str, dict[str, list[str]]]
Execute = Callable[[list[str]], int]

KNOWN = {
    "Unit": {
        "Description",
        "Documentation",
        "Requires",
        "Requisite",
        "Wants",
        "BindsTo",
        "PartOf",
        "Conflicts",
        "Before",
        "After",
        "ReloadPropagatedFrom",
        "PropagatesReloadTo",
        "OnFailure",
        "ConditionPathExists",
    },
    "Service": {
        "Type",
        "RemainAfterExit",
        "TimeoutStartSec",
        "TimeoutStopSec",
        "ExecStartPre",
        "ExecStart",
        "ExecStartPost",
        "ExecReload",
        "ExecStop",
        "Restart",
        "User",
        "Environment",
        "EnvironmentFile",
    },
    "Install": {"WantedBy", "RequiredBy", "Also", "Alias"},
    "Timer": {"OnActiveSec", "OnUnitActiveSec", "OnBootSec", "OnCalendar", "AccuracySec", "Unit", "Persistent"},
}
LISTS = {"Requires", "Requisite", "Wants", "BindsTo", "PartOf", "Conflicts", "Before", "After", "ReloadPropagatedFrom", "PropagatesReloadTo"}


def read_unit(units: Path, unit: str, builtin: dict[str, str], warnings: list[str]) -> Sections:
    """Return {section: {key: [values]}} for a unit file plus its drop-ins, after the image's own unit when it has one."""
    chunks = []
    if unit in builtin:
        chunks.append((f"{unit} (image)", builtin[unit]))
    path = units / unit
    if path.is_file():
        chunks.append((unit, path.read_text(encoding="utf-8")))
    for dropin in sorted((units / f"{unit}.d").glob("*.conf")):
        chunks.append((f"{unit}.d/{dropin.name}", dropin.read_text(encoding="utf-8")))
    sections: Sections = {}
    for name, text in chunks:
        section = None
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith(("#", ";")):
                continue
            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1] if line[1:-1] in KNOWN else None
                if section is None:
                    warnings.append(f"{name}: Unknown section '{line[1:-1]}'. Ignoring.")
                continue
            key, separator, value = line.partition("=")
            if not separator or section is None:
                warnings.append(f"{name}: Assignment outside of a known section or without '='. Ignoring.")
            elif key not in KNOWN[section]:
                warnings.append(f"{name}: Unknown key name '{key}' in section '{section}', ignoring.")
            elif not value:
                sections.setdefault(section, {})[key] = []
            else:
                sections.setdefault(section, {}).setdefault(key, []).extend(value.split() if key in LISTS else [value])
    return sections


def unit_names(units: Path, builtin: dict[str, str]) -> list[str]:
    names = set(builtin)
    names.update(path.name for path in units.iterdir() if path.is_file() and "." in path.name)
    names.update(path.name.removesuffix(".d") for path in units.iterdir() if path.is_dir() and path.name.endswith(".d"))
    return sorted(names)


class Manager:
    """Unit states and the dependency rules, with every transition appended to the event log."""

    def __init__(self, units: str | Path, state: str | Path, builtin: dict[str, str], execute: Execute) -> None:
        self.units, self.state, self.builtin, self.execute = Path(units), Path(state), builtin, execute
        self.warnings: list[str] = []
        self.sections = {name: read_unit(self.units, name, builtin, self.warnings) for name in unit_names(self.units, builtin)}
        self.active_file = self.state / "active.json"
        self.active = set(json.loads(self.active_file.read_text(encoding="utf-8"))) if self.active_file.exists() else set()

    def save(self) -> None:
        self.active_file.write_text(json.dumps(sorted(self.active)), encoding="utf-8")

    def event(self, text: str) -> None:
        with (self.state / "events.log").open("a", encoding="utf-8") as log:
            log.write(text + "\n")

    def values(self, unit: str, section: str, key: str) -> list[str]:
        return self.sections.get(unit, {}).get(section, {}).get(key, [])

    def dependents(self, unit: str) -> list[str]:
        """Units that a stop or restart of this unit also stops or restarts."""
        return [
            other
            for other in self.sections
            if other != unit
            and (
                unit in self.values(other, "Unit", "Requires") or unit in self.values(other, "Unit", "BindsTo") or unit in self.values(other, "Unit", "PartOf")
            )
        ]

    def run(self, unit: str, key: str) -> None:
        for command in self.values(unit, "Service", key):
            command = command.removeprefix("-")
            self.event(f"run {unit} {key} {command}")
            status = self.execute(command.split())
            if status:
                sys.exit(f"{unit} failed: {command} exited with status {status}")

    def start(self, unit: str) -> None:
        if unit in self.active:
            return
        for required in self.values(unit, "Unit", "Requires") + self.values(unit, "Unit", "BindsTo"):
            if required in self.sections:
                self.start(required)
        self.run(unit, "ExecStartPre")
        self.run(unit, "ExecStart")
        self.run(unit, "ExecStartPost")
        if self.values(unit, "Service", "Type") != ["oneshot"] or self.values(unit, "Service", "RemainAfterExit") == ["yes"]:
            self.active.add(unit)
        self.event(f"start {unit}")
        self.save()

    def stop(self, unit: str) -> None:
        if unit not in self.active:
            return
        for dependent in self.dependents(unit):
            self.stop(dependent)
        self.run(unit, "ExecStop")
        self.active.discard(unit)
        self.event(f"stop {unit}")
        self.save()

    def restart(self, unit: str) -> None:
        affected = [unit]
        for name in affected:
            affected.extend(other for other in self.dependents(name) if other in self.active and other not in affected)
        self.stop(unit)
        self.start(unit)
        for other in affected[1:]:
            self.start(other)

    def reload(self, unit: str) -> None:
        self.run(unit, "ExecReload")
        self.event(f"reload {unit}")
        for other in self.sections:
            if unit in self.values(other, "Unit", "ReloadPropagatedFrom") and other in self.active:
                self.reload(other)


def main(arguments: list[str], builtin: dict[str, str], execute: Execute) -> None:
    """Run one systemctl command line against the model; unknown commands fail."""
    manager = Manager(os.environ["BOOT_SYSTEMD"], os.environ["BOOT_STATE"], builtin, execute)
    if os.environ.get("BOOT_STATE_INITIAL") and not manager.active_file.exists():
        manager.active = set(os.environ["BOOT_STATE_INITIAL"].split())
        manager.save()
    command, names = (arguments[0], arguments[1:]) if arguments else ("", [])
    if arguments[:2] == ["enable", "--now"]:
        command, names = "start", arguments[2:]
    if command == "daemon-reload":
        with (manager.state / "systemd_warnings.log").open("a", encoding="utf-8") as log:
            log.writelines(line + "\n" for line in manager.warnings)
    elif command == "enable":
        pass
    elif command in ("start", "stop", "restart", "reload"):
        for name in names:
            getattr(manager, command)(name)
    else:
        sys.exit(f"systemctl stand-in: unsupported arguments {arguments}")

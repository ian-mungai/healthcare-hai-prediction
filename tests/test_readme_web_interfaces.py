"""The web interfaces the host offers agree across the Terraform, ``open_ui``, the project policy and the README.

The README no longer holds a port table. Its "Web interfaces" section names three scripts: ``bash scripts/open_ui.sh --list`` (the Service, Port and Health path
of each web interface), ``bash scripts/host.sh start`` and ``bash scripts/open_ui.sh <name>``. The ports and health paths come from one source, the Terraform,
through ``scripts/infrastructure/web_ui_terraform.py``, which ``open_ui`` reads too. A new web page is one entry in the ``web_ui_forwards`` map of
``infra/iam/web_ui_forwards.tf`` and one entry in ``WEB_INTERFACES`` below (the reviewed statement of which service containers are web pages), and ``open_ui``
needs no change. This check compares:

- The documents (the map and the OpenMetadata document) are exactly the reviewed web interfaces, and each document's fixed port is a port that the reviewed
  service container declares in a port mapping (or the OpenSearch setting).
- The health path ``open_ui --list`` shows for a port is the path the container health check calls on that port, or "None" when it calls none.
- No document forwards a port of ``infra/services`` that belongs to another container, so a port without a web page cannot be forwarded.
- Each optional service is gated in ``main.tf`` by a switch with a literal ``true`` or ``false`` default. The default is the Terraform's to set (the live stack
  settings are the Terraform defaults), so the check does not fix its value. Only the services in ``ALWAYS_ON`` have no switch.
- The project policy allows exactly the documents plus each older document (the on-demand catalog task's), the Terraform defines each older document, and its
  fixed port is the port of a document. The generic port forward document is never allowed.
- The README states no default of a switch that differs from the Terraform's (it states none today).
- The README section shows the three commands, names only web interfaces that exist, holds no table, no port number and no ``aws`` line, and the scripts it
  names exist. The scripts themselves are tested in ``tests/test_open_ui.py`` and ``tests/test_script_entry_points.py``.

It does not read a bind address from the Terraform. Ports opened by a process inside a container without a port mapping, except the OpenSearch one, are not
visible to it.
"""

from __future__ import annotations

import re
from pathlib import Path

from scripts.infrastructure.open_ui import interface_rows
from scripts.infrastructure.web_ui_terraform import forward_documents, health_path, terraform_listeners, terraform_text
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
POLICIES = ROOT / "infra" / "iam" / "policies"
SECTION = "## Web interfaces"
GENERIC_DOCUMENT = "AWS-StartPortForwardingSession"
CATALOG_FORWARD = ROOT / "infra" / "iam" / "openmetadata.tf"
# The reviewed web pages: the name that opens each one and the service of main.tf and the container that run it.
WEB_INTERFACES = {
    "airflow": ("airflow_apiserver", "airflow_apiserver"),
    "openmetadata": ("openmetadata", "openmetadata_server"),
    "mlflow": ("mlflow", "mlflow"),
    "streamlit": ("streamlit", "streamlit"),
    "grafana": ("monitoring", "grafana"),
    "prometheus": ("monitoring", "prometheus"),
}
ALWAYS_ON = {"airflow_apiserver"}
# A README sentence that gives the default of a service switch: "`monitoring_enabled` defaults to false" or "monitoring_enabled is off by default".
SWITCH_CLAIM = re.compile(r"`?\b(\w+_enabled)\b`?\s+(?:defaults to|is)\s+(true|false|on|off)\b")
OPEN_COMMAND = re.compile(r"^\s*bash scripts/open_ui\.sh (\S+)$", re.MULTILINE)
HOST_COMMAND = "bash scripts/host.sh start"
LIST_COMMAND = "bash scripts/open_ui.sh --list"
SCRIPTS = ("scripts/open_ui.sh", "scripts/host.sh", "scripts/infrastructure/open_ui.py", "scripts/infrastructure/web_ui_terraform.py")


def readme_section(readme: str) -> str:
    start = readme.find(SECTION + "\n")
    if start < 0:
        return ""
    rest = readme[start + len(SECTION) :]
    end = re.search(r"^## ", rest, re.MULTILINE)
    return rest[: end.start()] if end else rest


def older_documents() -> dict[str, int]:
    """The key and fixed host port of each older forward document that is not in the ``web_ui_forwards`` map: the on-demand catalog task's."""
    text = CATALOG_FORWARD.read_text()
    found: dict[str, int] = {}
    for name, body in re.findall(
        r'resource "aws_ssm_document" "\w+" \{(?=.*?name\s*=\s*format\(local\.platform_name, "(\w+)_forward"\))(.*?)\n\}\n', text, re.DOTALL
    ):
        port = re.search(r'properties = \{.*?portNumber\s*=\s*"(\d+)"', body, re.DOTALL)
        check(port is not None, f"the older forward document {name} has no fixed port")
        if port:
            found[name] = int(port.group(1))
    return found


def policy_documents() -> set[str]:
    """The keys of the forward documents the project policy allows, and the generic document if it names it."""
    text = (POLICIES / "healthcare_hai_prediction_ssmforward_policy.json").read_text()
    found = set(re.findall(r"document/\$\{PROJECT_NAME\}_(\w+)_forward_\$\{ENVIRONMENT\}", text))
    return found | ({GENERIC_DOCUMENT} if GENERIC_DOCUMENT in text else set())


def switch_gates(main_text: str, texts: dict[str, str]) -> dict[str, str]:
    """The variable that gates each optional service key in main.tf."""
    gates = dict(re.findall(r"for key, service in \{\s+(\w+) = \{.*?\} : key => service if (\S+)\n", main_text, re.DOTALL))
    for key, gate in gates.items():
        if gate.startswith("local."):
            alias = next(
                (found for text in texts.values() if (found := re.search(rf"^\s*{gate.removeprefix('local.')}\s*=\s*(var\.\w+)", text, re.MULTILINE))), None
            )
            gates[key] = alias.group(1) if alias else gate
    return {key: gate.removeprefix("var.") for key, gate in gates.items()}


def variable_default(name: str, texts: dict[str, str]) -> str | None:
    for text in texts.values():
        block = re.search(rf'variable "{name}" \{{(.*?)\n\}}', text, re.DOTALL)
        if block:
            default = re.search(r"\bdefault\s*=\s*(\S+)", block.group(1))
            return default.group(1) if default else None
    return None


class Sources:
    """Everything the source checks read, so a case can plant a defect in one part."""

    def __init__(self) -> None:
        self.texts = terraform_text()
        self.listeners, self.owners = terraform_listeners(self.texts)
        self.documents = forward_documents()
        self.gates = switch_gates(self.texts["main.tf"], self.texts)
        self.defaults = {gate: variable_default(gate, self.texts) for gate in self.gates.values()}
        self.older = older_documents()
        self.allowed = policy_documents()
        self.rows = interface_rows(self.documents, self.listeners)


def source_problems(sources: Sources) -> list[str]:
    """Every difference between the Terraform, the list ``open_ui`` prints and the project policy, as messages."""
    found: list[str] = []
    documents, listeners = sources.documents, sources.listeners
    if set(documents) != set(WEB_INTERFACES):
        found.append(f"the documents {sorted(documents)} differ from the reviewed web interfaces {sorted(WEB_INTERFACES)}")
    for name, port in sorted(documents.items()):
        if name not in WEB_INTERFACES:
            continue
        service, container = WEB_INTERFACES[name]
        key = (service, container, port, "tcp")
        if key not in listeners:
            found.append(f"the document {name} forwards port {port} but the Terraform declares no such port for {service} {container}")
            continue
        try:
            by_port = health_path(port, listeners)
        except ValueError as error:
            found.append(f"{name}: {error}")
            continue
        if by_port != listeners[key].get(port):
            found.append(f"{name}: the health path by port {by_port} differs from the container's health check {listeners[key].get(port)}")
        shown = next((row for row in sources.rows if row[0] == name), None)
        expected = (name, str(port), listeners[key].get(port) or "None")
        if shown != expected:
            found.append(f"{name}: the list shows {shown} but the Terraform gives {expected}: health path")
        gate = sources.gates.get(service)
        if gate is None:
            if service not in ALWAYS_ON:
                found.append(f"{name}: the service {service} has no gate in main.tf, and only {sorted(ALWAYS_ON)} may have none")
        elif variable_default(gate, sources.texts) not in ("true", "false"):
            found.append(f"{name}: the switch {gate} must have a literal true or false default")
    web_ports = {documents.get(name) for name in WEB_INTERFACES}
    internal = {key[2] for key in listeners} - web_ports
    for name, port in sorted(documents.items()):
        if port in internal:
            found.append(f"the document {name} forwards port {port}, which is a service port without a web page")
    for name, port in sorted(sources.older.items()):
        if port not in documents.values():
            found.append(f"the older document {name} forwards port {port}, which is the port of no web interface document")
    expected_policy = set(documents) | set(sources.older)
    if sources.allowed != expected_policy:
        found.append(f"the project policy allows the documents {sorted(sources.allowed)} but the Terraform defines {sorted(expected_policy)}")
    return found


def readme_problems(readme: str, documents: dict[str, int], defaults: dict[str, str | None]) -> list[str]:
    """Every difference between the README and the scripts, web interfaces and switch defaults of the Terraform, as messages."""
    found: list[str] = []
    for switch, claimed in SWITCH_CLAIM.findall(readme):
        shown = {"on": "true", "off": "false"}.get(claimed, claimed)
        if switch in defaults and defaults[switch] != shown:
            found.append(f"the README says the switch {switch} defaults to {claimed} but the Terraform default is {defaults[switch]}")
    section = readme_section(readme)
    if not section:
        return [*found, f"README.md has no {SECTION!r} section"]
    for command in (HOST_COMMAND, LIST_COMMAND):
        if command not in section:
            found.append(f"the section must show {command!r}")
    opened = [name for name in OPEN_COMMAND.findall(section) if name != "--list"]
    if not opened:
        found.append("the section must show one 'bash scripts/open_ui.sh <name>' command")
    found += [f"the command opens {name!r}, which is not a web interface of the Terraform" for name in opened if name not in documents]
    if any(line.lstrip().startswith("|") for line in section.splitlines()):
        found.append("the section holds a table: the ports and health paths come from 'bash scripts/open_ui.sh --list'")
    found += [
        f"the section names the port {port} of {name}: the port comes from the list"
        for name, port in sorted(documents.items())
        if re.search(rf"\b{port}\b", section)
    ]
    found += [f"the command {line!r} must be a script command: bash scripts/open_ui.sh <name>" for line in re.findall(r"^\s*(aws\b.*)$", section, re.MULTILINE)]
    if re.search(r"<[A-Za-z_]+>", section):
        found.append("the section holds a placeholder")
    if GENERIC_DOCUMENT in section:
        found.append(f"the README names the generic document {GENERIC_DOCUMENT}, which the project user may not start")
    found += [f"the script {script} does not exist" for script in SCRIPTS if not (ROOT / script).is_file()]
    return found


def test_the_terraform_the_list_and_the_policy_agree() -> None:
    differences = source_problems(Sources())
    check(not differences, "the web interface sources differ:\n" + "\n".join(differences))


def test_the_readme_names_the_scripts_and_holds_no_port_table() -> None:
    differences = readme_problems((ROOT / "README.md").read_text(), forward_documents(), Sources().defaults)
    check(not differences, "README.md differs from the web interface scripts:\n" + "\n".join(differences))


def planted(sources: Sources, change: str) -> Sources:
    """The real sources with one planted defect."""
    web_services = {service for service, _ in WEB_INTERFACES.values()}
    if change == "a wrong port":
        sources.documents["grafana"] = 3002
    elif change == "a wrong container":
        sources.documents["airflow"] = next(port for service, _, port, _ in sorted(sources.listeners) if service == "mlflow")
    elif change == "a missing document":
        del sources.documents["prometheus"]
    elif change == "a document for an internal port":
        sources.documents["internal"] = next(key[2] for key in sorted(sources.listeners) if key[0] not in web_services)
    elif change == "a wrong health path":
        sources.rows = [(name, port, "/api/ready" if name == "grafana" else path) for name, port, path in sources.rows]
    elif change == "a switch without a default":
        sources.texts = {
            name: re.sub(r'(variable "monitoring_enabled" \{.*?)\n\s*default\s*=\s*\w+', r"\1", text, count=1, flags=re.DOTALL)
            for name, text in sources.texts.items()
        }
    elif change == "a gate that names no variable":
        sources.gates["monitoring"] = "monitoring_wrong"
    elif change == "an optional service without a gate":
        del sources.gates["mlflow"]
    elif change == "an older document with another port":
        sources.older["catalog"] = 9999
    elif change == "a document the policy lacks":
        sources.allowed.discard("grafana")
    elif change == "the generic document in the policy":
        sources.allowed.add(GENERIC_DOCUMENT)
    elif change == "the catalog document missing from the policy":
        sources.allowed -= set(sources.older)
    else:
        raise AssertionError(f"unknown planted defect {change!r}")
    return sources


def test_a_source_defect_is_caught() -> None:
    cases = {
        "a wrong port": "declares no such port",
        "a wrong container": "declares no such port",
        "a missing document": "differ from the reviewed web interfaces",
        "a document for an internal port": "service port without a web page",
        "a wrong health path": "health path",
        "a switch without a default": "literal true or false default",
        "a gate that names no variable": "literal true or false default",
        "an optional service without a gate": "has no gate",
        "an older document with another port": "port of no web interface document",
        "a document the policy lacks": "project policy allows",
        "the generic document in the policy": "project policy allows",
        "the catalog document missing from the policy": "project policy allows",
    }
    for change, expected in cases.items():
        found = source_problems(planted(Sources(), change))
        check(any(expected in message for message in found), f"{change} was not reported as {expected!r}: {found}")


def replaced(readme: str, old: str, new: str) -> str:
    check(old in readme, f"the planted defect needs {old!r} in the README")
    return readme.replace(old, new, 1)


def test_a_readme_defect_is_caught() -> None:
    readme = (ROOT / "README.md").read_text()
    documents = forward_documents()
    defaults = Sources().defaults
    wrong = "false" if defaults["monitoring_enabled"] == "true" else "true"
    airflow = "bash scripts/open_ui.sh airflow"
    cases = {
        "a missing host start": (replaced(readme, HOST_COMMAND, "bash scripts/host.sh status"), "must show"),
        "a missing list command": (replaced(readme, LIST_COMMAND, airflow), "must show"),
        "a command for a name that is not a web interface": (replaced(readme, airflow, "bash scripts/open_ui.sh postgres"), "not a web interface"),
        "a command for the generic document": (replaced(readme, airflow, f"bash scripts/open_ui.sh {GENERIC_DOCUMENT}"), "not a web interface"),
        "no open command": (replaced(readme, airflow, ""), "one 'bash scripts/open_ui.sh <name>' command"),
        "a port table": (
            replaced(readme, SECTION + "\n", SECTION + "\n\n| Service | Port | Health path |\n| --- | --- | --- |\n| Airflow | 8080 | `/api/v2/version` |\n"),
            "holds a table",
        ),
        "a port number in the text": (replaced(readme, "Every other port of the host is internal.", "Airflow uses port 8080."), "names the port 8080"),
        "an aws command": (replaced(readme, airflow, "aws ssm start-session --target i-0123"), "must be a script command"),
        "a placeholder": (replaced(readme, airflow, "bash scripts/open_ui.sh <UI>"), "placeholder"),
        "the generic document in the text": (
            replaced(readme, "Every other port of the host is internal.", f"Every other port of the host is internal. {GENERIC_DOCUMENT}"),
            "generic document",
        ),
        "a wrong switch default": (readme + f"\nThe switch `monitoring_enabled` defaults to {wrong}.\n", "the Terraform default is"),
    }
    for name, (text, expected) in cases.items():
        found = readme_problems(text, documents, defaults)
        check(any(expected in message for message in found), f"{name} was not reported as {expected!r}: {found}")

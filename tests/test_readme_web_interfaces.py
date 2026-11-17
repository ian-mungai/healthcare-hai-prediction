"""The README web interface table and forward commands match the Terraform, the Session Manager documents and the project policy.

The README section "Web interfaces" lists only the services that have a web page, with the port, the health path and one forward command for each. A new
web page is one entry in the ``web_ui_forwards`` map of ``infra/iam/web_ui_forwards.tf``, one row in the table and one line in the commands. This check reads
the Terraform as text and compares:

- Every row names a service, container and port that ``infra/services`` declares in a port mapping (or the OpenSearch setting). The health path of the row is
  the path the container health check calls on that port, or "None" when it calls none.
- The ports of the rows, of the forward documents and of the commands are the same set, and each command names the document that fixes its port.
- No document forwards a port of ``infra/services`` that has no row, so a port without a web page cannot be forwarded.
- Each switch is a declared variable that gates its service in ``main.tf`` and defaults to off.
- The project policy names exactly the documents of the Terraform and the older catalog document, and not the generic AWS port forward document.

It does not read a bind address from the Terraform. Ports opened by a process inside a container without a port mapping, except the OpenSearch one, are not
visible to it.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ROOT / "infra" / "services"
FORWARDS = ROOT / "infra" / "iam" / "web_ui_forwards.tf"
OPENMETADATA_FORWARD = ROOT / "infra" / "iam" / "openmetadata_persistent.tf"
POLICIES = ROOT / "infra" / "iam" / "policies"
SECTION = "## Web interfaces"
UI_HEADER = ("Service", "Container", "Port", "Web interface", "Health path", "Terraform switch")
GENERIC_DOCUMENT = "AWS-StartPortForwardingSession"
# The document of the on-demand catalog task is older than the web page documents and stays in the policy beside them.
OLDER_DOCUMENTS = {"catalog"}
COMMAND = re.compile(
    r"^\s*aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_(\w+)_forward_<ENVIRONMENT> --parameters localPortNumber=(\d+)$",
    re.MULTILINE,
)
LOCAL_NUMBER = re.compile(r"^\s*(\w+)\s*=\s*(\d+)\s*$", re.MULTILINE)
CONTAINER_START = re.compile(r"merge\(local\.(?:airflow_)?container_defaults, \{")
ASSIGNMENT = re.compile(r"^\s*(\w+_containers)\s*=", re.MULTILINE)


def terraform_text() -> dict[str, str]:
    return {path.name: path.read_text() for path in sorted(SERVICES.glob("*.tf"))}


def local_numbers(texts: dict[str, str]) -> dict[str, int]:
    return {name: int(value) for text in texts.values() for name, value in LOCAL_NUMBER.findall(text)}


def number(token: str, numbers: dict[str, int]) -> int:
    token = token.strip()
    return int(token) if token.isdigit() else numbers[token.removeprefix("local.").removeprefix("${local.").removesuffix("}")]


def service_containers(main_text: str) -> dict[str, str]:
    """Map each ``<name>_containers`` local to the service key that runs it."""
    keys = re.findall(r"^\s+(\w+) = \{\n\s+task_role_arn\b.*?\n\s+containers\s*=\s*local\.(\w+)", main_text, re.MULTILINE | re.DOTALL)
    return {containers: key for key, containers in keys}


def health_paths(block: str, numbers: dict[str, int]) -> dict[int, str]:
    """The HTTP health path of each port the container's health check calls, without its query."""
    found: dict[int, str] = {}
    for port, path in re.findall(r"http://127\.0\.0\.1:(\d+|\$\{local\.\w+\})(/[^\s'\"\\)]*)", block):
        found[number(port, numbers)] = path.split("?")[0]
    for port, path in re.findall(r"/dev/tcp/127\.0\.0\.1/(\d+); printf 'GET (\S+) HTTP", block):
        found[int(port)] = path
    return found


def terraform_listeners(texts: dict[str, str]) -> tuple[dict[tuple[str, str, int, str], dict[int, str]], dict[str, str]]:
    """Every service container listener as (service, container, port, protocol), with the health paths of its container."""
    numbers = local_numbers(texts)
    owners = service_containers(texts["main.tf"])
    listeners: dict[tuple[str, str, int, str], dict[int, str]] = {}
    for text in texts.values():
        starts = [match.start() for match in CONTAINER_START.finditer(text)] + [len(text)]
        for begin, end in zip(starts, starts[1:], strict=False):
            block = text[begin:end]
            name = re.search(r'\bname\s*=\s*"(\w+)"', block)
            if name is None:
                continue
            assignments = ASSIGNMENT.findall(text[:begin])
            service = owners.get(assignments[-1]) if assignments else None
            ports: list[tuple[int, str]] = []
            mapping = re.search(r"portMappings\s*=\s*\[(.*?)\]", block, re.DOTALL)
            for container_port, host_port, protocol in re.findall(
                r'containerPort\s*=\s*([\w.]+),\s*hostPort\s*=\s*([\w.]+),\s*protocol\s*=\s*"(\w+)"', mapping.group(1) if mapping else ""
            ):
                check(number(container_port, numbers) == number(host_port, numbers), f"{name.group(1)}: the container and host ports differ")
                ports.append((number(host_port, numbers), protocol))
            search = re.search(r'name = "http\.port", value = tostring\(local\.(\w+)\)', block)
            if search:
                ports.append((numbers[search.group(1)], "tcp"))
            check(service is not None or not ports, f"{name.group(1)} listens on {ports} but belongs to no service of main.tf")
            for port, protocol in ports:
                listeners[(str(service), name.group(1), port, protocol)] = health_paths(block, numbers)
    return listeners, owners


def readme_section(readme: str) -> str:
    start = readme.find(SECTION + "\n")
    if start < 0:
        return ""
    rest = readme[start + len(SECTION) :]
    end = re.search(r"^## ", rest, re.MULTILINE)
    return rest[: end.start()] if end else rest


def table(section: str, header: tuple[str, ...]) -> list[tuple[str, ...]]:
    """The rows (cells without the backticks that quote code) of the table whose header row is ``header``."""
    lines = section.splitlines()
    marker = "| " + " | ".join(header) + " |"
    if marker not in lines:
        return []
    rows: list[tuple[str, ...]] = []
    for line in lines[lines.index(marker) + 2 :]:
        if not line.startswith("|"):
            break
        rows.append(tuple(cell.strip() for cell in line.strip().strip("|").split(" | ")))
    return rows


def forward_documents() -> dict[str, int]:
    """The key and fixed host port of each web page document: the ``web_ui_forwards`` map and the OpenMetadata document."""
    documents = {key: int(port) for key, port in re.findall(r"^\s+(\w+)\s*=\s*\{ port = (\d+),", FORWARDS.read_text(), re.MULTILINE)}
    persistent = OPENMETADATA_FORWARD.read_text()
    name = re.search(r'name\s*=\s*format\(local\.platform_name, "(\w+)_forward"\)', persistent)
    port = re.search(r'properties = \{\s+portNumber\s*=\s*"(\d+)"', persistent)
    check(name is not None and port is not None, "openmetadata_persistent.tf has no forward document with a fixed port")
    if name and port:
        documents[name.group(1)] = int(port.group(1))
    return documents


def policy_documents() -> set[str]:
    """The keys of the forward documents the project policy allows, and the generic document if it names it."""
    text = (POLICIES / "healthcare_hai_prediction_ssmforward_policy.json").read_text()
    found = set(re.findall(r"document/\$\{PROJECT_NAME\}_(\w+)_forward_\$\{ENVIRONMENT\}", text))
    return found | ({GENERIC_DOCUMENT} if GENERIC_DOCUMENT in text else set())


def switch_gates(main_text: str) -> dict[str, str]:
    """The variable that gates each optional service key in main.tf."""
    gates = dict(re.findall(r"for key, service in \{\s+(\w+) = \{.*?\} : key => service if (\S+)\n", main_text, re.DOTALL))
    for key, gate in gates.items():
        if gate.startswith("local."):
            alias = re.search(rf"\b{gate.removeprefix('local.')}\s*=\s*(var\.\w+)", terraform_text()["openmetadata.tf"])
            gates[key] = alias.group(1) if alias else gate
    return {key: gate.removeprefix("var.") for key, gate in gates.items()}


def variable_default(name: str, texts: dict[str, str]) -> str | None:
    for text in texts.values():
        block = re.search(rf'variable "{name}" \{{(.*?)\n\}}', text, re.DOTALL)
        if block:
            default = re.search(r"\bdefault\s*=\s*(\S+)", block.group(1))
            return default.group(1) if default else None
    return None


def problems(readme: str) -> list[str]:
    """Every difference between the README and the code, as messages."""
    section = readme_section(readme)
    if not section:
        return [f"README.md has no {SECTION!r} section"]
    texts = terraform_text()
    listeners, owners = terraform_listeners(texts)
    found: list[str] = []

    rows = table(section, UI_HEADER)
    ui_ports: set[int] = set()
    switches: dict[str, str] = {}
    for service, container, port_cell, _page, health, switch in rows:
        if not port_cell.isdigit():
            found.append(f"port cell {port_cell!r} of {container} is not a number")
            continue
        key = (service.strip("`"), container.strip("`"), int(port_cell), "tcp")
        ui_ports.add(key[2])
        switches[key[0]] = switch.strip("`")
        if key not in listeners:
            found.append(f"the web interface table has {key} but the Terraform declares no such port")
            continue
        path = re.match(r"`(/[^`]*)`", health)
        called = listeners[key].get(key[2])
        if path and called != path.group(1):
            found.append(f"{key}: the table says health path {path.group(1)} but the health check calls {called}")
        if not path and not health.startswith("None"):
            found.append(f"{key}: the health cell must start with a code path or 'None'")
        if health.startswith("None") and called is not None:
            found.append(f"{key}: the table says no health path but the health check calls {called}")

    documents = forward_documents()
    commands = {key: int(port) for key, port in COMMAND.findall(section)}
    if ui_ports != set(documents.values()):
        found.append(f"the table ports {sorted(ui_ports)} differ from the forward document ports {sorted(set(documents.values()))}")
    if commands != documents:
        found.append(f"the forward commands {commands} differ from the forward documents {documents}")
    internal = {key[2] for key in listeners} - ui_ports
    for document, port in sorted(documents.items()):
        if port in internal:
            found.append(f"the document {document} forwards port {port}, which is a service port without a web page")
    allowed = policy_documents()
    if allowed != set(documents) | OLDER_DOCUMENTS:
        found.append(f"the project policy allows the documents {sorted(allowed)} but the Terraform defines {sorted(documents)} and {sorted(OLDER_DOCUMENTS)}")
    if GENERIC_DOCUMENT in section:
        found.append(f"the README names the generic document {GENERIC_DOCUMENT}, which the project user may not start")

    service_keys = set(owners.values())
    gates = switch_gates(texts["main.tf"])
    for service, switch in switches.items():
        if service not in service_keys:
            found.append(f"{service}: main.tf has no such service")
        elif service in gates:
            if switch != gates[service]:
                found.append(f"{service}: the table names switch {switch} but main.tf gates it with {gates[service]}")
            if variable_default(switch, texts) != "false":
                found.append(f"{service}: the switch {switch} must default to false")
        elif switch != "None":
            found.append(f"{service}: it has no gate in main.tf, so its switch must be None")
    return found


def test_the_readme_web_interfaces_match_the_terraform_the_documents_and_the_policy() -> None:
    differences = problems((ROOT / "README.md").read_text())
    check(not differences, "README.md differs from the code:\n" + "\n".join(differences))


def planted(readme: str, old: str, new: str) -> str:
    check(old in readme, f"the planted defect needs {old!r} in the README")
    return readme.replace(old, new, 1)


def test_a_wrong_port_a_missing_row_or_command_and_a_generic_document_are_caught() -> None:
    readme = (ROOT / "README.md").read_text()
    cases = {
        "a wrong port": (planted(readme, "| `grafana` | 3001 |", "| `grafana` | 3002 |"), "declares no such port"),
        "a wrong container": (
            planted(readme, "| `airflow_apiserver` | `airflow_apiserver` | 8080 |", "| `airflow_apiserver` | `airflow_other` | 8080 |"),
            "declares no such port",
        ),
        "a wrong health path": (planted(readme, "`/api/health`", "`/api/ready`"), "health path /api/ready"),
        "a wrong switch": (planted(readme, "`mlflow_enabled`", "`mlflow_on`"), "table names switch"),
        "a command for the generic document": (
            planted(readme, "--document-name <PROJECT_NAME>_streamlit_forward_<ENVIRONMENT>", f"--document-name {GENERIC_DOCUMENT}"),
            "forward commands",
        ),
        "a command with another local port": (planted(readme, "localPortNumber=9090", "localPortNumber=9091"), "forward commands"),
        "a missing command": (re.sub(r"(?m)^\s*aws ssm start-session .*_prometheus_forward_.*\n", "", readme), "forward commands"),
        "the generic document in the text": (readme + f"\n{GENERIC_DOCUMENT}\n" if SECTION in readme else readme, "generic document"),
    }
    for name, (text, expected) in cases.items():
        found = problems(text)
        check(any(expected in message for message in found), f"{name} was not reported as {expected!r}: {found}")

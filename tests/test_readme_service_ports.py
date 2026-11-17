"""The README service tables match the ports, health paths, switches and job firewall that the code declares.

The README section "Services and ports" holds four tables: the listening ports, the service switches, the host firewall rules of the job containers and
nothing else that needs a second source. This check reads the Terraform of ``infra/services`` as text and compares it with them:

- Every ``portMappings`` entry of a service container, and the OpenSearch ``http.port`` setting (a host-network listener that has no port mapping), is a row
  of the port table with the same service, container, port and protocol, and the table holds no other row.
- A row that names a health path has the same path in the container's health check on that port. A row that names none has no HTTP health check on it.
- The rows that say "open to jobs" are the ports of ``infra/iam/host_job_firewall.nft`` and of the ``host_ports_reachable_from_jobs`` output.
- The monitoring rows are the ports of the ``monitoring`` output.
- The services are the service keys of ``main.tf``, and each switch is a declared variable that gates its service, with the default the table gives.
- The firewall table lists the rules of the nft file in order.

It does not read a bind address from the Terraform: a "Loopback only" row only has to stay off the job-reachable ports. Ports opened by a process inside a
container without a port mapping, except the OpenSearch one, are not visible to it.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ROOT / "infra" / "services"
FIREWALL = ROOT / "infra" / "iam" / "host_job_firewall.nft"
SECTION = "## Services and ports"
PORT_HEADER = ("Service", "Container", "Port", "Use and job access", "Health or metrics path")
SWITCH_HEADER = ("Service", "Terraform switch", "Default")
FIREWALL_HEADER = ("Chain", "Destination", "Port", "Action")
JOBS_OPEN = "Host private IP, open to jobs."
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


def firewall_rules() -> tuple[list[tuple[str, str, str, str]], set[int]]:
    """The docker0 rules of the host firewall as (chain, destination, port, action), and the ports its host-local rule accepts."""
    rules: list[tuple[str, str, str, str]] = []
    open_ports: set[int] = set()
    chain = ""
    for line in FIREWALL.read_text().splitlines():
        line = line.strip()
        if line.startswith("chain "):
            chain = line.split()[1]
            continue
        if not line.startswith("iifname") or "ct state" in line:
            continue
        destination = "any"
        if "fib daddr type local" in line:
            destination = "host-local"
        elif address := re.search(r"ip daddr (\S+)", line):
            destination = address.group(1)
        port = "any"
        if ports := re.search(r"(tcp|udp) dport (\{[^}]*\}|\d+)", line):
            port = f"{ports.group(1)} {re.sub(r'[{}]', '', ports.group(2)).strip()}"
            if destination == "host-local":
                open_ports = {int(item) for item in re.findall(r"\d+", ports.group(2))}
        rules.append((chain, destination, port, line.split()[-1]))
    return rules, open_ports


def output_ports(texts: dict[str, str], numbers: dict[str, int]) -> tuple[set[int], dict[str, int]]:
    outputs = texts["outputs.tf"]
    reach = re.search(r'output "host_ports_reachable_from_jobs".*?value\s*=\s*(merge\(.*?\))\n', outputs, re.DOTALL)
    check(reach is not None, "outputs.tf has no host_ports_reachable_from_jobs output")
    jobs = {number(value, numbers) for _, value in re.findall(r"(\w+) = (\d+|local\.\w+)", reach.group(1))} if reach else set()
    monitoring = re.search(r"ports\s*=\s*\{([^}]*)\}", outputs)
    check(monitoring is not None, "outputs.tf has no monitoring ports")
    return jobs, {name: int(value) for name, value in re.findall(r"(\w+) = (\d+)", monitoring.group(1))} if monitoring else {}


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
    """Every difference between the README tables and the code, as messages."""
    section = readme_section(readme)
    if not section:
        return [f"README.md has no {SECTION!r} section"]
    texts = terraform_text()
    numbers = local_numbers(texts)
    listeners, owners = terraform_listeners(texts)
    found: list[str] = []

    rows = table(section, PORT_HEADER)
    documented: dict[tuple[str, str, int, str], tuple[str, str]] = {}
    for service, container, port_cell, use, health in rows:
        match = re.fullmatch(r"(\d+)( \(UDP\))?", port_cell)
        if match is None:
            found.append(f"port cell {port_cell!r} of {container} is not a number or 'number (UDP)'")
            continue
        documented[(service.strip("`"), container.strip("`"), int(match.group(1)), "udp" if match.group(2) else "tcp")] = (use, health)
    for key in sorted(set(listeners) - set(documented)):
        found.append(f"the Terraform declares {key} but the port table has no row for it")
    for key in sorted(set(documented) - set(listeners)):
        found.append(f"the port table has {key} but the Terraform declares no such port")

    jobs_in_readme = set()
    for key, (use, health) in documented.items():
        if key not in listeners:
            continue
        paths = listeners[key]
        path = re.match(r"`(/[^`]*)`", health)
        if path and paths.get(key[2]) != path.group(1):
            found.append(f"{key}: the table says health path {path.group(1)} but the health check calls {paths.get(key[2])}")
        if not path and not health.startswith("None"):
            found.append(f"{key}: the health cell must start with a code path or 'None'")
        if health.startswith("None") and key[2] in paths:
            found.append(f"{key}: the table says no health path but the health check calls {paths[key[2]]}")
        if use.endswith(JOBS_OPEN):
            jobs_in_readme.add(key[2])
        elif not use.endswith(("Loopback only.", "Not open to jobs.")):
            found.append(f"{key}: the use cell must end with a job access sentence")

    firewall, firewall_ports = firewall_rules()
    output_jobs, monitoring = output_ports(texts, numbers)
    if jobs_in_readme != firewall_ports:
        found.append(f"the rows open to jobs are {sorted(jobs_in_readme)} but the host firewall opens {sorted(firewall_ports)}")
    if firewall_ports != output_jobs:
        found.append(f"the host firewall opens {sorted(firewall_ports)} but host_ports_reachable_from_jobs names {sorted(output_jobs)}")
    monitoring_in_readme = {key[2] for key in documented if key[0] == "monitoring"}
    if monitoring_in_readme != set(monitoring.values()):
        found.append(f"the monitoring rows are {sorted(monitoring_in_readme)} but the monitoring output names {sorted(monitoring.values())}")

    readme_firewall = [(chain, destination, port, action) for chain, destination, port, action in table(section, FIREWALL_HEADER)]
    if readme_firewall != [(chain, destination.replace("host-local", "Host's own addresses"), port, action) for chain, destination, port, action in firewall]:
        found.append(f"the firewall table {readme_firewall} differs from the rules of host_job_firewall.nft {firewall}")

    switches = {service.strip("`"): (switch.strip("`"), default) for service, switch, default in table(section, SWITCH_HEADER)}
    service_keys = set(owners.values())
    if set(switches) != service_keys or {key[0] for key in documented} - service_keys:
        found.append(f"the switch table services {sorted(switches)} differ from the services of main.tf {sorted(service_keys)}")
    gates = switch_gates(texts["main.tf"])
    for service, (switch, default) in switches.items():
        if service in gates:
            if switch != gates[service]:
                found.append(f"{service}: the table names switch {switch} but main.tf gates it with {gates[service]}")
            if variable_default(switch, texts) is None or default != ("On" if variable_default(switch, texts) == "true" else "Off"):
                found.append(f"{service}: the table default {default} differs from the default of {switch}")
        elif (switch, default) != ("None", "On"):
            found.append(f"{service}: it has no gate in main.tf, so its switch must be None and its default On")
    return found


def test_the_readme_tables_match_the_terraform_ports_and_the_firewall() -> None:
    differences = problems((ROOT / "README.md").read_text())
    check(not differences, "README.md differs from the code:\n" + "\n".join(differences))


def test_the_readme_names_the_session_manager_documents_the_policy_allows() -> None:
    readme = (ROOT / "README.md").read_text()
    policy = (ROOT / "infra/iam/policies/healthcare_hai_prediction_ssm_policy.json").read_text()
    for document in ("AWS-StartPortForwardingSession", "<PROJECT_NAME>_openmetadata_forward_<ENVIRONMENT>"):
        check(document in readme, f"README.md does not name the Session Manager document {document}")
    check(
        "document/AWS-StartPortForwardingSession" in policy and "document/${PROJECT_NAME}_openmetadata_forward_${ENVIRONMENT}" in policy,
        "the project policy no longer allows the two port forward documents the README names",
    )


def planted(readme: str, old: str, new: str) -> str:
    check(old in readme, f"the planted defect needs {old!r} in the README")
    return readme.replace(old, new, 1)


def test_a_wrong_port_a_missing_row_and_a_wrong_firewall_or_health_claim_are_caught() -> None:
    readme = (ROOT / "README.md").read_text()
    cases = {
        "a wrong port": (planted(readme, "| `grafana` | 3001 |", "| `grafana` | 3002 |"), "port table has"),
        "a missing row": (
            planted(readme, "| `airflow_apiserver` | `airflow_apiserver` | 8080 |", "| `airflow_apiserver` | `airflow_other` | 8080 |"),
            "no row for it",
        ),
        "a wrong health path": (planted(readme, "`/q/health`", "`/q/ready`"), "health path /q/ready"),
        "a wrong job claim": (planted(readme, "Loopback only.", JOBS_OPEN), "rows open to jobs"),
        "a wrong firewall row": (planted(readme, "| input | 169.254.169.254 | any | drop |", "| input | 169.254.169.254 | any | accept |"), "firewall table"),
        "a wrong switch": (planted(readme, "`mlflow_enabled`", "`mlflow_on`"), "table names switch"),
    }
    for name, (text, expected) in cases.items():
        found = problems(text)
        check(any(expected in message for message in found), f"{name} was not reported as {expected!r}: {found}")

"""Exercise the local configuration lifecycle through real processes; never contact AWS."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import platform
import re
import shutil
import stat
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.infrastructure import systemd_model
from scripts.process import TimeoutExpired, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = "example_project_dev"
ADMIN = "example_administrator"
# Public synthetic value that must never appear in diagnostics or generated files.
DISCLOSURE_SENTINEL = "example_e2e_value_do_not_print"
# Production entry points copied unmodified into each synthetic workspace, including the process launcher they import.
SOURCE_PATHS = (
    "scripts/process.py",
    "scripts/infrastructure/render_project_config.py",
    "scripts/infrastructure/render_platform_config.py",
    "scripts/infrastructure/render_project_config.sh",
    "scripts/infrastructure/terraform.sh",
)
# The resource types the platform stack (the project profile) may declare, and the ones the IAM stack (the administrator) must keep.
PLATFORM_RESOURCE_TYPES = {
    "aws_cloudwatch_log_group",
    "aws_cloudwatch_metric_alarm",
    "aws_dlm_lifecycle_policy",
    "aws_ebs_volume",
    "aws_ecr_lifecycle_policy",
    "aws_ecr_repository",
    "aws_ecs_capacity_provider",
    "aws_ecs_cluster",
    "aws_ecs_cluster_capacity_providers",
    "aws_eip",
    "aws_sns_topic",
}
OWNER_RESOURCE_TYPES = {
    "aws_autoscaling_group",
    "aws_budgets_budget",
    "aws_iam_instance_profile",
    "aws_iam_role",
    "aws_iam_role_policy",
    "aws_internet_gateway",
    "aws_launch_template",
    "aws_route",
    "aws_route_table",
    "aws_route_table_association",
    "aws_security_group",
    "aws_sns_topic_policy",
    "aws_subnet",
    "aws_vpc",
    "aws_vpc_security_group_egress_rule",
    "aws_vpc_security_group_ingress_rule",
}
OUTPUT_NAMES = ("infra/deployment.auto.tfvars.json", "infra/iam/deployment.auto.tfvars.json")
PLATFORM_OUTPUT = "infra/platform/deployment.auto.tfvars.json"
SERVICES_OUTPUT = "infra/services/deployment.auto.tfvars.json"
SERVICES_FOUNDATION = {
    "project_tag": "example_tag_from_platform",
    "cluster_name": "example_cluster_from_platform",
    "ec2_capacity_provider_name": "example_capacity_from_platform",
    "execution_role_arn": "arn:aws:iam::111111111111:role/example_execution",
    "polaris_task_role_arn": "arn:aws:iam::111111111111:role/example_polaris",
    "control_plane_task_role_arn": "arn:aws:iam::111111111111:role/example_control_plane",
    "log_group_names": {name: f"/example_platform/{name}" for name in ("postgres", "redis", "polaris", "airflow", "marquez")},
    "ecr_repository_urls": {
        name: f"111111111111.dkr.ecr.eu-west-1.amazonaws.com/example_platform/{name}" for name in ("postgres", "redis", "polaris", "polaris_admin", "airflow")
    },
    "data_mount_path": "/example_platform_data",
}
SERVICES_DIGESTS = {name: "sha256:" + digit * 64 for name, digit in zip(("postgres", "redis", "polaris", "polaris_admin", "airflow"), "12345", strict=True)}
# The Terraform stand-in records which profile and credentials reach terraform output.
SERVICES_TERRAFORM = r"""
import json, os, sys
from pathlib import Path
root = Path(__file__).resolve().parent.parent
arguments = sys.argv[1:]
with (root / "terraform_calls.jsonl").open("a", encoding="utf-8") as log:
    credentials = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    log.write(json.dumps({"arguments": arguments, "profile": os.environ.get("AWS_PROFILE"),
                         "static_credentials": any(os.environ.get(name) for name in credentials)}) + "\n")
if arguments[1:] == ["output", "-json"] and arguments[0] == "-chdir=" + str(root / "infra/platform"):
    if (root / "output_error").is_file():
        sys.exit("example_e2e_value_do_not_print")
    sys.stdout.write((root / "platform_outputs.json").read_text(encoding="utf-8"))
elif arguments[1:3] == ["show", "-json"] and arguments[0] == "-chdir=" + str(root / "infra/services"):
    sys.stdout.write(Path(arguments[3]).read_text(encoding="utf-8"))
elif arguments[0] == "-chdir=" + str(root / "infra/services") and arguments[1] in ("plan", "apply", "validate"):
    pass
else:
    sys.exit("Unsupported services Terraform stand-in call")
"""
SERVICES_AWS = """
import json, sys
from pathlib import Path
arguments = sys.argv[1:]
if arguments != ["sts", "get-caller-identity", "--profile", "example_project_dev", "--region", "eu-west-1", "--output", "json", "--no-cli-pager"]:
    sys.exit("Unsupported services identity stand-in call")
root = Path(__file__).resolve().parent.parent
(root / "identity_called").write_text("verified", encoding="utf-8")
identity_marker = root / "identity"
identity = identity_marker.read_text(encoding="utf-8") if identity_marker.is_file() else "example_project_dev"
sys.stdout.write(json.dumps({"Account": "111111111111", "Arn": "arn:aws:iam::111111111111:user/" + identity}))
"""
PLATFORM_TERRAFORM = """
import json, os, sys
from pathlib import Path
root = Path(__file__).resolve().parent
observed = {name: value for name, value in os.environ.items() if name.startswith("AWS_")}
(root / "environment.json").write_text(json.dumps(observed), encoding="utf-8")
if sys.argv[2:] == ["validate"]:
    calls = root / "validate_calls.json"
    recorded = json.loads(calls.read_text(encoding="utf-8")) if calls.exists() else []
    calls.write_text(json.dumps([*recorded, sys.argv[1:]]), encoding="utf-8")
else:
    sys.stdout.write(Path(sys.argv[4]).read_text(encoding="utf-8"))
"""
BOOT_TEMPLATE = "infra/iam/host_boot.sh.tftpl"
# The host paths the boot script names in its first lines, moved under a temporary root for each run.
BOOT_PATHS = {
    "log_file": "boot.log",
    "fstab_file": "fstab",
    "ecs_config_file": "ecs.config",
    "device_directory": "devices",
    "systemd_directory": "systemd",
    "sysctl_file": "sysctl.d/90-platform-core-dumps.conf",
    "nft_rules_file": "nftables/platform_imds.nft",
    "nftables_config": "nftables.conf",
    "guard_script": "sbin/platform_imds_guard",
}
BOOT_VALUES = {
    "region": "eu-west-1",
    "cluster_name": "example_project_cluster_dev",
    "data_volume_name": "example_project_host_data_dev",
    "address_name": "example_project_host_dev",
    "project_tag": "example_project",
}
# The host finds its data volume and Elastic IP by their Name and Project tags; the aws stand-in answers the two describe calls with these IDs.
BOOT_IDS = {"volume": "vol-0123456789abcdef0", "allocation": "eipalloc-0123456789abcdef0"}
# Stand-in system commands: each succeeds at once unless a case replaces it with one that stalls.
BOOT_STUBS = {
    "dnf": "exit 0",
    "rpm": "exit 0",
    "mkfs.xfs": "exit 0",
    "mount": "exit 0",
    "mountpoint": "exit 1",
    "blkid": 'case "$1" in -p) exit 2 ;; *) echo 00000000-0000-0000-0000-000000000000 ;; esac',
}
STALL = "exec /bin/sleep 45"
STALL_IGNORING_TERM = "trap '' TERM; exec /bin/sleep 45"
# Stateful stand-ins, in Python, for the commands whose effect later steps read back. They keep their state in
# BOOT_STATE: the nftables table, the kernel settings, each systemctl call and each metadata request with its outcome.
# The metadata stand-in drops a request from a non-root user only while the table holds the root-only rule, which is
# what the kernel does with that rule; the kernel and systemd themselves are not exercised.
BOOT_PYTHON_STUBS = {
    "aws": """
import json, os, sys
from pathlib import Path
state = Path(os.environ["BOOT_STATE"])
arguments = sys.argv[1:]
with (state / "aws_calls.log").open("a", encoding="utf-8") as log:
    log.write(" ".join(arguments) + "\\n")
ids = json.loads(os.environ["BOOT_IDS"])
filters = {argument for argument in arguments if argument.startswith("Name=")}
if os.environ.get("BOOT_AWS_FAIL", "\\0") in " ".join(arguments):
    sys.exit("aws stand-in: simulated failure")
if arguments[:2] == ["ec2", "describe-volumes"]:
    wanted = {"Name=tag:Name,Values=" + os.environ["BOOT_DATA_VOLUME_NAME"], "Name=tag:Project,Values=" + os.environ["BOOT_PROJECT_TAG"]}
    print(os.environ.get("BOOT_VOLUME_RESULT", ids["volume"]) if filters == wanted else "", end="")
elif arguments[:2] == ["ec2", "describe-addresses"]:
    wanted = {"Name=tag:Name,Values=" + os.environ["BOOT_ADDRESS_NAME"], "Name=tag:Project,Values=" + os.environ["BOOT_PROJECT_TAG"]}
    print(os.environ.get("BOOT_ALLOCATION_RESULT", ids["allocation"]) if filters == wanted else "", end="")
elif arguments[:2] not in (["ec2", "associate-address"], ["ec2", "attach-volume"]):
    sys.exit(f"aws stand-in: unsupported arguments {arguments}")
""",
    "nft": """
import json, os, sys
from pathlib import Path
state = Path(os.environ["BOOT_STATE"])
table = state / "nft_table"
arguments = sys.argv[1:]
if os.environ.get("BOOT_NFT_FAIL"):
    sys.exit("Error: Could not process rule: Operation not supported")
def event(text):
    with (state / "events.log").open("a", encoding="utf-8") as log:
        log.write(text + "\\n")
def remove():
    if table.exists():
        table.unlink()
        active = json.loads((state / "active.json").read_text(encoding="utf-8")) if (state / "active.json").exists() else []
        running = [unit for unit in ("docker.service", "ecs.service") if unit in active]
        event("rule_removed containers_running=" + (",".join(running) or "none"))
def process(path):
    text = Path(path).read_text(encoding="utf-8")
    for line in (line.strip() for line in text.splitlines()):
        if line in ("flush ruleset", "delete table inet platform_imds"):
            table.unlink(missing_ok=True)
        elif line.startswith("include "):
            process(line.split('"')[1])
    if "table inet platform_imds {" in text:
        table.write_text(text[text.index("table inet platform_imds {"):], encoding="utf-8")
def load(path):
    # One nft -f file is one transaction: a table that the file deletes and adds again is never absent in between.
    before = table.read_text(encoding="utf-8") if table.exists() else None
    process(path)
    after = table.read_text(encoding="utf-8") if table.exists() else None
    if before is not None and after is None:
        table.write_text(before, encoding="utf-8")
        remove()
    elif after is not None:
        event("rule_loaded")
if arguments[:1] == ["-f"]:
    load(arguments[1])
elif arguments == ["list", "table", "inet", "platform_imds"]:
    if not table.exists():
        sys.exit("Error: No such file or directory")
    print(table.read_text(encoding="utf-8"), end="")
elif arguments in (["flush", "ruleset"], ["delete", "table", "inet", "platform_imds"]):
    remove()
else:
    sys.exit(f"nft stand-in: unsupported arguments {arguments}")
""",
    "curl": """
import json, os, sys
from pathlib import Path
state = Path(os.environ["BOOT_STATE"])
url = next((argument for argument in sys.argv[1:] if "169.254.169.254" in argument), "")
if url:
    uid = int(os.environ.get("PROBE_UID", "0"))
    table = state / "nft_table"
    rule = table.exists() and all(text in table.read_text(encoding="utf-8") for text in ("hook output", "ip daddr 169.254.169.254 meta skuid != 0 drop"))
    with (state / "metadata_requests.log").open("a", encoding="utf-8") as log:
        log.write(f"{uid} {'dropped' if uid != 0 and rule else 'reached'}\\n")
    if uid != 0 and rule:
        sys.exit(28)
    if "/tags/instance/" in url:
        sys.exit(22)
print("i-0123456789abcdef0", end="")
""",
    "setpriv": """
import os, sys
arguments = sys.argv[1:]
uid = next(argument.split("=", 1)[1] for argument in arguments if argument.startswith("--reuid="))
command = arguments[arguments.index("--") + 1:]
os.execvpe(command[0], command, {**os.environ, "PROBE_UID": uid})
""",
    "sysctl": """
import os, sys
from pathlib import Path
settings = Path(os.environ["BOOT_STATE"]) / "sysctl"
arguments = sys.argv[1:]
if arguments[:2] == ["-q", "-p"] and len(arguments) == 3:
    if not os.environ.get("BOOT_SYSCTL_IGNORE"):
        for line in Path(arguments[2]).read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition("=")
            if value:
                (settings / key.strip()).write_text(value.strip(), encoding="utf-8")
elif arguments[:1] == ["-n"] and len(arguments) == 2:
    print((settings / arguments[1]).read_text(encoding="utf-8"))
else:
    sys.exit(f"sysctl stand-in: unsupported arguments {arguments}")
""",
}
# systemctl runs the dependency model in systemd_model.py: the model's source is copied into the stand-in, which adds the units the
# image ships (the firewall service, Docker and the ECS agent) and the stall hook.
SYSTEMCTL_STUB_BODY = """
import os, subprocess, sys
config = os.environ["BOOT_NFTABLES_CONFIG"]
BUILTIN = {
    "nftables.service": (
        "[Unit]\\nDescription=Netfilter Tables\\n\\n[Service]\\nType=oneshot\\nRemainAfterExit=yes\\n"
        f"ExecStart=nft -f {config}\\nExecReload=nft -f {config}\\nExecStop=nft flush ruleset\\n"
    ),
    "docker.service": "[Unit]\\nDescription=Docker Application Container Engine\\n\\n[Service]\\nType=notify\\nExecStart=true\\n",
    "ecs.service": (
        "[Unit]\\nDescription=Amazon ECS container agent\\nRequires=docker.service\\nAfter=docker.service\\n\\n"
        "[Service]\\nType=simple\\nExecStart=true\\n"
    ),
}
def execute(command):
    return subprocess.run(command, check=False).returncode
arguments = sys.argv[1:]
with (Path(os.environ["BOOT_STATE"]) / "systemctl.log").open("a", encoding="utf-8") as log:
    log.write(" ".join(arguments) + "\\n")
if os.environ.get("BOOT_SYSTEMCTL_STALL", "\\0") in " ".join(arguments):
    os.execv("/bin/sleep", ["sleep", "45"])
main(arguments, BUILTIN, execute)
"""
# The ECS-optimized image ships a systemd-coredump pipe handler and set-user-ID dumps on.
BOOT_KERNEL_DEFAULTS = {"kernel.core_pattern": "|/usr/lib/systemd/systemd-coredump %P %u %g %s %t %c %h", "fs.suid_dumpable": "2"}
# Logs each requested deadline, then runs GNU timeout when the runner found one, or else does what GNU timeout does: TERM at
# the deadline, KILL after the grace period, status 124 or 137. Deadlines are capped at BOOT_TIMEOUT_CAP and grace
# periods at BOOT_GRACE_CAP seconds.
TIMEOUT_STUB = """#!/usr/bin/perl
use strict;
my @request = @ARGV;
my $grace = 0;
if ($ARGV[0] =~ /^--kill-after=(\\d+)$/) { $grace = $1; shift @ARGV; }
my $limit = shift @ARGV;
open(my $log, ">>", $ENV{BOOT_TIMEOUT_LOG}) or die;
print $log "@request\\n";
close $log;
$limit = $ENV{BOOT_TIMEOUT_CAP} if $ENV{BOOT_TIMEOUT_CAP} < $limit;
$grace = $ENV{BOOT_GRACE_CAP} if $grace && $ENV{BOOT_GRACE_CAP} < $grace;
if ($ENV{BOOT_GNU_TIMEOUT}) { exec($ENV{BOOT_GNU_TIMEOUT}, ($grace ? ("--kill-after=$grace") : ()), $limit, @ARGV) or exit 125; }
my $pid = fork();
if ($pid == 0) { exec(@ARGV) or exit 127; }
local $SIG{ALRM} = sub { die "deadline\\n" };
my $status;
eval { alarm $limit; waitpid($pid, 0); $status = $?; alarm 0; };
if ($@ eq "deadline\\n") {
  kill "TERM", $pid;
  if ($grace) {
    eval { alarm $grace; waitpid($pid, 0); alarm 0; };
    if ($@ eq "deadline\\n") { kill "KILL", $pid; waitpid($pid, 0); exit 137; }
  } else {
    waitpid($pid, 0);
  }
  exit 124;
}
exit(($status & 127) ? 128 + ($status & 127) : $status >> 8);
"""
FIXTURE = {
    "AWS_ACCOUNT_ID": "111111111111",
    "AWS_PROFILE": PROFILE,
    "AWS_REGION": "eu-west-1",
    "ENVIRONMENT": "dev",
    "PROJECT_NAME": "example_project",
    "S3_BUCKET": "example-project-e2e-bucket",
}


def sha256(content: bytes) -> str:
    """Hash immutable evidence bytes."""
    return hashlib.sha256(content).hexdigest()


def write_json(path: Path, value: Any) -> None:
    """Create owner-only JSON evidence without replacing earlier runs."""
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
    path.chmod(0o600)


class CheckFailed(RuntimeError):
    """Identify a failed synthetic configuration assertion."""


class Evidence:
    """Collect redacted process assertions and reproducible configuration evidence."""

    def __init__(self, output: Path, temporary: Path) -> None:
        self.output = output
        self.temporary = temporary
        self.started = time.perf_counter()
        self.redactions = {
            str(output): "<EVIDENCE>",
            str(temporary): "<TEMP>",
            str(REPO_ROOT): "<REPO>",
            sys.executable: "<PYTHON>",
            str(Path.home()): "<HOME>",
            DISCLOSURE_SENTINEL: "<REDACTED_SENTINEL>",
        }
        self.report: dict[str, Any] = {
            "schema_version": 1,
            "scope": "local_configuration_lifecycle",
            "status": "running",
            "live_aws_tested": False,
            "subprocess_responses_mocked": False,
            "started_at_utc": datetime.now(UTC).isoformat(),
            "runtime": {"python": platform.python_version(), "system": platform.system(), "machine": platform.machine()},
            "repeat_command": [".venv/bin/python", "-m", "scripts.infrastructure.run_e2e", "--output-directory", "<NEW_EVIDENCE_DIRECTORY>"],
            "source_hashes": {},
            "steps": [],
        }
        self.environment = {
            "HOME": str(temporary / "home"),
            "TMPDIR": str(temporary),
            "PATH": str(temporary / "tools"),
            "PYTHON_BIN": sys.executable,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONUTF8": "1",
            "LANG": "C",
            "LC_ALL": "C",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CONFIG_FILE": str(temporary / "absent_aws_config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(temporary / "absent_aws_credentials"),
            "AWS_PROFILE": "example_stale_profile",
            "AWS_ACCOUNT_ID": "222222222222",
            "AWS_REGION": "us-east-1",
        }

    def redact(self, value: str) -> str:
        """Replace private paths and synthetic secret sentinels in evidence."""
        for raw, replacement in sorted(self.redactions.items(), key=lambda item: len(item[0]), reverse=True):
            value = value.replace(raw, replacement)
        return value

    def run(self, name: str, command: list[str], workspace: Path, extra_environment: dict[str, str] | None = None) -> tuple[dict[str, Any], Any]:
        """Execute a real local process with synthetic inputs and record assertions."""
        step: dict[str, Any] = {
            "name": name,
            "command": [self.redact(item) for item in command],
            "cwd": self.redact(str(workspace)),
            "started_at_utc": datetime.now(UTC).isoformat(),
            "assertions": [],
        }
        self.report["steps"].append(step)
        started = time.perf_counter()
        try:
            result = run_command(command[0], command[1:], cwd=workspace, env={**self.environment, **(extra_environment or {})}, timeout=30)
            step.update(returncode=result.returncode, stdout=self.redact(result.stdout), stderr=self.redact(result.stderr))
            self.check(step, "diagnostics_do_not_disclose_sentinel", DISCLOSURE_SENTINEL not in result.stdout + result.stderr)
            return step, result
        except TimeoutExpired as error:
            step.update(returncode=None, stdout=self.redact(as_text(error.stdout)), stderr=self.redact(as_text(error.stderr)), timed_out=True)
            raise CheckFailed(f"{name}: child process exceeded 30 seconds") from None
        finally:
            step["duration_seconds"] = round(time.perf_counter() - started, 6)

    @staticmethod
    def check(step: dict[str, Any], name: str, condition: bool) -> None:
        """Append an assertion and stop the suite when it fails."""
        step["assertions"].append({"name": name, "passed": condition})
        if not condition:
            raise CheckFailed(f"{step['name']}: {name}")

    def capture_outputs(self, step: dict[str, Any], workspace: Path, region: str, project: str = "example_project") -> dict[str, str]:
        """Verify exact synthetic outputs, private modes and stable hashes."""
        common = {
            "expected_account_id": "111111111111",
            "aws_region": region,
            "project_name": project,
            "data_bucket_name": "example-project-e2e-bucket",
            "aws_profile": f"{project}_dev",
            "environment": "dev",
        }
        expected = (common, common)
        hashes = {}
        step["outputs"] = []
        for name, values in zip(OUTPUT_NAMES, expected, strict=True):
            path = workspace / name
            self.check(step, f"{name}: exists", path.is_file())
            content = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            self.check(step, f"{name}: owner_only_permissions", mode == 0o600)
            self.check(step, f"{name}: exact_json", json.loads(content) == values)
            self.check(step, f"{name}: no_secret_or_administrator", DISCLOSURE_SENTINEL.encode() not in content and ADMIN.encode() not in content)
            artifact = self.output / step["name"] / name
            artifact.parent.mkdir(parents=True, exist_ok=True)
            with artifact.open("xb") as handle:
                handle.write(content)
            artifact.chmod(0o600)
            hashes[name] = sha256(content)
            step["outputs"].append({"path": artifact.relative_to(self.output).as_posix(), "sha256": hashes[name], "mode": oct(mode)})
        self.check(step, "no_temporary_configuration_files", not list(workspace.rglob(".config_*")))
        return hashes

    def finish(self) -> None:
        """Persist the report and a checksum manifest for successful or failed runs."""
        self.report["finished_at_utc"] = datetime.now(UTC).isoformat()
        self.report["duration_seconds"] = round(time.perf_counter() - self.started, 6)
        self.report["process_count"] = len(self.report["steps"])
        self.report["assertion_count"] = sum(len(step["assertions"]) for step in self.report["steps"])
        self.report["requirements_sha256"] = sha256((REPO_ROOT / "requirements.txt").read_bytes())
        self.report["prerequisites"] = "Project Python 3.12 environment with pinned requirements; real bash and dirname. No cloud credentials needed."
        self.report["limits"] = (
            "Configuration processes and pre-deployment guards only. No Terraform deployment or live AWS identity verified. The host boot"
            " script runs in a temporary root against stand-in system commands, not on a host: stateful stand-ins model the"
            " nftables table, kernel settings, systemd starts and metadata requests, so the kernel and systemd are not exercised."
        )
        self.report["cleanup"] = "Temporary synthetic workspace is removed when the runner exits; immutable evidence is retained."
        write_json(self.output / "report.json", self.report)
        hashes = {path.relative_to(self.output).as_posix(): sha256(path.read_bytes()) for path in sorted(self.output.rglob("*")) if path.is_file()}
        write_json(self.output / "evidence_manifest.json", {"algorithm": "sha256", "files": hashes})


def as_text(value: str | bytes | None) -> str:
    """Normalize captured process diagnostics before redaction."""
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value or ""


def fixture_sources(evidence: Evidence) -> dict[str, bytes]:
    """Copy actual implementation bytes and record their hashes without patching."""
    sources = {}
    for name in SOURCE_PATHS:
        original = (REPO_ROOT / name).read_bytes()
        sources[name] = original
        evidence.report["source_hashes"][name] = {"original_sha256": sha256(original), "fixture_sha256": sha256(original)}
    evidence.report["source_hashes"]["run_e2e.py"] = {"original_sha256": sha256(Path(__file__).read_bytes())}
    evidence.report["production_sources_unchanged"] = True
    return sources


def write_environment(workspace: Path, values: dict[str, str]) -> None:
    """Create a private dotenv file from generic synthetic fixture values."""
    path = workspace / ".env"
    path.write_text("".join(f'export {key}="{value}" # synthetic fixture\n' for key, value in values.items()), encoding="utf-8")
    path.chmod(0o600)


def copied_workspace(temporary: Path, name: str, sources: dict[str, bytes], values: dict[str, str]) -> Path:
    """Create a disposable workspace containing unmodified production entry points."""
    workspace = temporary / name
    for relative, content in sources.items():
        (workspace / relative).parent.mkdir(parents=True, exist_ok=True)
        (workspace / relative).write_bytes(content)
    write_environment(workspace, values)
    return workspace


def check_saved_plans(evidence: Evidence, sources: dict[str, bytes], renderer: list[str], shell_renderer: list[str]) -> None:
    """Plan the real IAM variables with the real terraform command, then check which saved plans the renderer accepts."""
    workspace = copied_workspace(evidence.temporary, "saved_plan", sources, FIXTURE)
    step, result = evidence.run("render_for_saved_plan", shell_renderer, workspace)
    evidence.check(step, "render_succeeded", result.returncode == 0 and not result.stderr)
    variables = (REPO_ROOT / "infra/iam/variables.tf").read_bytes()
    evidence.report["source_hashes"]["infra/iam/variables.tf"] = {"original_sha256": sha256(variables), "fixture_sha256": sha256(variables)}
    stack = workspace / "infra" / "iam"
    (stack / "variables.tf").write_bytes(variables)
    terraform_environment = {"CHECKPOINT_DISABLE": "1", "TF_IN_AUTOMATION": "1"}
    terraform = ["terraform", f"-chdir={stack}"]
    plan_arguments = ["plan", "-input=false", "-lock=false", f"-var=admin_profile={ADMIN}", "-var=example_extra_input=example_value"]
    check_plan = [*renderer, "--stack", "iam", "--admin-profile", ADMIN, "--check"]
    step, result = evidence.run("terraform_init", [*terraform, "init", "-input=false", "-backend=false"], workspace, terraform_environment)
    evidence.check(step, "init_succeeded", result.returncode == 0)
    step, result = evidence.run("plan_with_defaults", [*terraform, *plan_arguments[:-1], "-out=defaults.tfplan"], workspace, terraform_environment)
    evidence.check(step, "plan_succeeded", result.returncode == 0)
    step, result = evidence.run(
        "accept_saved_plan_with_defaults", [*check_plan, "--check-plan", str(stack / "defaults.tfplan")], workspace, terraform_environment
    )
    evidence.check(step, "committed_defaults_accepted", result.returncode == 0 and "Verified .env-derived" in result.stdout)
    override = "example-overridden-name"
    evidence.redactions[override] = "<REDACTED_OVERRIDE>"
    (stack / "override.auto.tfvars.json").write_text(json.dumps({"bls_secret_name": override}), encoding="utf-8")
    step, result = evidence.run("plan_with_file_override", [*terraform, *plan_arguments[:-1], "-out=file_override.tfplan"], workspace, terraform_environment)
    evidence.check(step, "plan_succeeded", result.returncode == 0)
    step, result = evidence.run("reject_file_override", [*check_plan, "--check-plan", str(stack / "file_override.tfplan")], workspace, terraform_environment)
    evidence.check(step, "override_rejected", result.returncode == 2 and "bls_secret_name" in result.stderr and not result.stdout)
    evidence.check(step, "value_not_echoed", override not in result.stdout + result.stderr)
    (stack / "override.auto.tfvars.json").unlink()
    step, result = evidence.run(
        "plan_with_command_line_override",
        [*terraform, *plan_arguments[:-1], f"-var=hud_secret_name={override}", "-out=line_override.tfplan"],
        workspace,
        terraform_environment,
    )
    evidence.check(step, "plan_succeeded", result.returncode == 0)
    step, result = evidence.run(
        "reject_command_line_override", [*check_plan, "--check-plan", str(stack / "line_override.tfplan")], workspace, terraform_environment
    )
    evidence.check(step, "override_rejected", result.returncode == 2 and "hud_secret_name" in result.stderr and not result.stdout)
    evidence.check(step, "value_not_echoed", override not in result.stdout + result.stderr)
    for label, value in {"empty": "", "wildcard": "example*", "question": "example?"}.items():
        step, result = evidence.run(
            f"reject_terraform_{label}_name", [*terraform, *plan_arguments[:-1], f"-var=jev_secret_name={value}"], workspace, terraform_environment
        )
        evidence.check(step, "variable_rule_rejected", result.returncode != 0 and "var.jev_secret_name" in result.stderr)
    # A parameter name must be a slash path of letters, digits and underscores, so the dashed override above would fail the variable's own rule.
    managed_override = "/example_overridden/name"
    evidence.redactions[managed_override] = "<REDACTED_MANAGED_OVERRIDE>"
    step, result = evidence.run(
        "plan_with_managed_secret_override",
        [*terraform, *plan_arguments[:-1], f"-var=airflow_jwt_secret_name={managed_override}", "-out=managed_override.tfplan"],
        workspace,
        terraform_environment,
    )
    evidence.check(step, "plan_succeeded", result.returncode == 0)
    step, result = evidence.run(
        "reject_managed_secret_override", [*check_plan, "--check-plan", str(stack / "managed_override.tfplan")], workspace, terraform_environment
    )
    evidence.check(step, "override_rejected", result.returncode == 2 and "airflow_jwt_secret_name" in result.stderr and not result.stdout)
    evidence.check(step, "value_not_echoed", managed_override not in result.stdout + result.stderr)
    (stack / "extra.tf").write_text('variable "example_extra_input" {\n  type = string\n}\n', encoding="utf-8")
    step, result = evidence.run("plan_with_extra_input", [*terraform, *plan_arguments, "-out=extra.tfplan"], workspace, terraform_environment)
    evidence.check(step, "plan_succeeded", result.returncode == 0)
    step, result = evidence.run(
        "reject_extra_input_without_default", [*check_plan, "--check-plan", str(stack / "extra.tfplan")], workspace, terraform_environment
    )
    evidence.check(step, "extra_input_rejected", result.returncode == 2 and "example_extra_input" in result.stderr and not result.stdout)
    evidence.check(step, "value_not_echoed", "example_value" not in result.stdout + result.stderr)


def platform_plan(values: dict[str, Any], defaults: dict[str, Any] | None = None, **changes: Any) -> dict[str, Any]:
    """Build a synthetic saved-plan document with the selected stack's declared variables and committed defaults."""
    declared: dict[str, dict[str, Any]] = {name: {} for name in values}
    defaults = {"data_volume_gb": 60, "snapshot_retain_count": 7} if defaults is None else defaults
    declared |= {name: {"default": value} for name, value in defaults.items()}
    variables = {name: {"value": value} for name, value in values.items()}
    variables |= {name: {"value": value} for name, value in defaults.items()}
    plan = {"complete": True, "errored": False, "variables": variables, "configuration": {"root_module": {"variables": declared}}}
    for key, value in changes.items():
        if key == "variables":
            plan["variables"] = {**variables, **value}
        else:
            plan[key] = value
    return plan


def run_platform_cases(evidence: Evidence, sources: dict[str, bytes]) -> None:
    """Render platform inputs, reject stale ones and check saved platform plans through a stand-in terraform."""
    renderer = [sys.executable, "-m", "scripts.infrastructure.render_platform_config"]
    workspace = copied_workspace(evidence.temporary, "platform", sources, FIXTURE)
    expected = {
        "aws_profile": PROFILE,
        "aws_region": FIXTURE["AWS_REGION"],
        "environment": "dev",
        "expected_account_id": FIXTURE["AWS_ACCOUNT_ID"],
        "project_name": FIXTURE["PROJECT_NAME"],
    }
    step, result = evidence.run("render_platform", renderer, workspace)
    evidence.check(step, "render_succeeded", result.returncode == 0 and "Rendered private platform inputs" in result.stdout)
    path = workspace / PLATFORM_OUTPUT
    evidence.check(step, "exact_platform_inputs", path.is_file() and json.loads(path.read_bytes()) == expected)
    evidence.check(step, "owner_only_permissions", stat.S_IMODE(path.stat().st_mode) == 0o600)
    evidence.check(step, "only_platform_output", not (workspace / OUTPUT_NAMES[0]).exists() and not (workspace / OUTPUT_NAMES[1]).exists())
    step, result = evidence.run("check_platform", [*renderer, "--check"], workspace)
    evidence.check(step, "matching_inputs_accepted", result.returncode == 0 and "Verified .env-derived platform inputs" in result.stdout)
    write_environment(workspace, {**FIXTURE, "AWS_REGION": "us-east-2"})
    before = sha256(path.read_bytes())
    step, result = evidence.run("reject_stale_platform", [*renderer, "--check"], workspace)
    evidence.check(step, "stale_inputs_rejected", result.returncode == 2 and "Generated configuration is stale" in result.stderr)
    evidence.check(step, "output_unchanged", sha256(path.read_bytes()) == before)
    write_environment(workspace, FIXTURE)

    # The stand-in prints the synthetic plan document passed as the saved-plan path; the real CLI is never run. It sits in a folder of its own
    # that only these runs put on the PATH, because the IAM saved-plan cases need the real terraform in the shared tools folder.
    stand_in = evidence.temporary / "platform_tools"
    stand_in.mkdir(exist_ok=True)
    (stand_in / "terraform").write_text(f"#!{sys.executable}\n" + PLATFORM_TERRAFORM, encoding="utf-8")
    (stand_in / "terraform").chmod(0o700)
    (stand_in / "dirname").symlink_to("/usr/bin/dirname")
    required_plan = platform_plan(expected)
    required_plan["configuration"]["root_module"]["variables"]["example_required"] = {}
    required_plan["variables"]["example_required"] = {"value": DISCLOSURE_SENTINEL}
    cases = {
        "reject_platform_required_variable": (
            required_plan,
            2,
            "A platform variable without a default is not rendered from .env; create and review a fresh plan.",
        ),
        "accept_matching_platform_plan": (platform_plan(expected), 0, "Rendered private platform inputs"),
        "reject_changed_default": (
            platform_plan(expected, variables={"data_volume_gb": {"value": 200}}),
            2,
            "differ from .env or the committed defaults",
        ),
        "reject_changed_env_input": (
            platform_plan(expected, variables={"aws_region": {"value": "us-east-2"}}),
            2,
            "differ from .env or the committed defaults",
        ),
        "reject_undeclared_variable": (platform_plan(expected, variables={"example_extra": {"value": "x"}}), 2, "declared variables"),
        "reject_incomplete_plan": (platform_plan(expected, complete=False), 2, "Only a complete, successful saved plan"),
    }
    for name, (plan, returncode, message) in cases.items():
        plan_path = workspace / f"{name}.json"
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        step, result = evidence.run(name, [*renderer, "--check-plan", str(plan_path)], workspace, {"PATH": str(stand_in)})
        evidence.check(step, "expected_outcome", result.returncode == returncode and message in result.stdout + result.stderr)
        inherited = {name: value for name, value in evidence.environment.items() if name.startswith("AWS_")}
        evidence.check(step, "platform_inspection_environment_unchanged", json.loads((stand_in / "environment.json").read_bytes()) == inherited)
    step, result = evidence.run(
        "reject_unreadable_platform_plan", [*renderer, "--check-plan", str(workspace / "absent.tfplan")], workspace, {"PATH": str(stand_in)}
    )
    evidence.check(step, "original_inspection_error", result.returncode == 2 and "The saved plan could not be inspected; no apply was run." in result.stderr)
    path.unlink()
    step, result = evidence.run(
        "wrapper_platform_validate_renders", ["/bin/bash", "scripts/infrastructure/terraform.sh", "--platform", "validate"], workspace, {"PATH": str(stand_in)}
    )
    evidence.check(step, "platform_inputs_rendered", path.is_file() and json.loads(path.read_bytes()) == expected)
    calls = json.loads((stand_in / "validate_calls.json").read_bytes())
    evidence.check(step, "one_platform_validate_call", result.returncode == 0 and calls == [[f"-chdir={(workspace / 'infra/platform').resolve()}", "validate"]])
    iam_path = workspace / OUTPUT_NAMES[1]
    iam_path.unlink(missing_ok=True)
    before_calls = calls
    step, result = evidence.run(
        "wrapper_iam_validate_renders",
        ["/bin/bash", "scripts/infrastructure/terraform.sh", "--iam", "--admin-profile", ADMIN, "validate"],
        workspace,
        {"PATH": str(stand_in)},
    )
    evidence.check(
        step, "iam_inputs_rendered", iam_path.is_file() and json.loads(iam_path.read_bytes()) == {**expected, "data_bucket_name": FIXTURE["S3_BUCKET"]}
    )
    calls = json.loads((stand_in / "validate_calls.json").read_bytes())
    evidence.check(
        step, "one_iam_validate_call", result.returncode == 0 and calls == [*before_calls, [f"-chdir={(workspace / 'infra/iam').resolve()}", "validate"]]
    )

    # The project user must not own anything a launch or the cost budget is built from: the network, the launch template, the host
    # group, the budget and every role belong to the administrator-applied IAM stack. The platform stack may declare only what
    # the approved permissions list gives the project profile, and the IAM stack must declare the moved resources.
    declared = re.compile(r'^resource\s+"(aws_[a-z0-9_]+)"\s+"([a-z_]+)"', re.MULTILINE)

    def resource_types(stack: str) -> set[str]:
        return {match[1] for path in (REPO_ROOT / "infra" / stack).glob("*.tf") for match in declared.finditer(path.read_text(encoding="utf-8"))}

    platform_types, iam_types = resource_types("platform"), resource_types("iam")
    step = evidence.report["steps"][-1]
    evidence.check(step, "platform_stack_declares_only_what_the_project_profile_may_create", platform_types <= PLATFORM_RESOURCE_TYPES)
    evidence.check(step, "platform_stack_owns_no_template_network_group_budget_or_role", not platform_types & OWNER_RESOURCE_TYPES)
    evidence.check(step, "iam_stack_declares_the_moved_resources", iam_types >= OWNER_RESOURCE_TYPES)
    evidence.check(step, "iam_stack_has_no_budget_action_so_nothing_stops_spending", "aws_budgets_budget_action" not in iam_types)
    evidence.check(step, "iam_stack_declares_nothing_the_project_profile_owns", iam_types & PLATFORM_RESOURCE_TYPES <= {"aws_sns_topic"})
    evidence.check(
        step,
        "iam_stack_attaches_nothing_to_users_or_groups",
        not {name for name in iam_types if re.search(r"(user|group)_?(policy|membership|attachment)|policy_attachment", name)},
    )


def run_services_cases(evidence: Evidence, sources: dict[str, bytes], bash: str) -> None:
    """Exercise services rendering and the actual wrapper with synthetic platform outputs and command stand-ins."""
    workspace = copied_workspace(evidence.temporary, "services", sources, FIXTURE).resolve()
    tools = workspace / "tools"
    tools.mkdir()
    for name, body in {"terraform": SERVICES_TERRAFORM, "aws": SERVICES_AWS}.items():
        (tools / name).write_text(f"#!{sys.executable}\n" + body, encoding="utf-8")
        (tools / name).chmod(0o700)
    (tools / "dirname").symlink_to("/usr/bin/dirname")
    environment = {
        "PATH": str(tools),
        "AWS_ACCESS_KEY_ID": DISCLOSURE_SENTINEL,
        "AWS_SECRET_ACCESS_KEY": DISCLOSURE_SENTINEL,
    }
    outputs = {name: {"value": value} for name, value in SERVICES_FOUNDATION.items()}
    outputs["aws_region"] = {"value": "us-east-2"}
    call_path = workspace / "terraform_calls.jsonl"
    output_path = workspace / "platform_outputs.json"
    output_path.write_text(json.dumps(outputs), encoding="utf-8")
    expected = {
        "aws_profile": PROFILE,
        "aws_region": "eu-west-1",
        "environment": "dev",
        "expected_account_id": "111111111111",
        "project_name": "example_project",
        **SERVICES_FOUNDATION,
    }
    renderer = [sys.executable, "-m", "scripts.infrastructure.render_platform_config", "--stack", "services"]
    wrapper = [bash, "scripts/infrastructure/terraform.sh", "--services"]
    path = workspace / SERVICES_OUTPUT
    step, result = evidence.run("render_services", renderer, workspace, environment)
    evidence.check(step, "render_succeeded", result.returncode == 0 and "Rendered private services inputs" in result.stdout)
    evidence.check(step, "exact_services_inputs_from_env_and_platform", path.is_file() and json.loads(path.read_bytes()) == expected)
    evidence.check(step, "owner_only_permissions", stat.S_IMODE(path.stat().st_mode) == 0o600)
    evidence.check(step, "only_services_output", not any((workspace / name).exists() for name in (*OUTPUT_NAMES, PLATFORM_OUTPUT)))
    evidence.check(step, "values_not_printed", all(value not in result.stdout + result.stderr for value in string_values(expected)))
    original = sha256(path.read_bytes())
    step, result = evidence.run("rerender_services", renderer, workspace, environment)
    evidence.check(step, "stable_bytes", result.returncode == 0 and sha256(path.read_bytes()) == original)
    step, result = evidence.run("check_services", [*renderer, "--check"], workspace, environment)
    evidence.check(step, "matching_inputs_accepted", result.returncode == 0)
    output_path.write_text(json.dumps({**outputs, "cluster_name": {"value": "example_changed_cluster"}}), encoding="utf-8")
    step, result = evidence.run("reject_stale_services", [*renderer, "--check"], workspace, environment)
    evidence.check(step, "stale_inputs_rejected", result.returncode == 2 and "Generated configuration is stale" in result.stderr)
    evidence.check(step, "output_unchanged", sha256(path.read_bytes()) == original)
    for missing in SERVICES_FOUNDATION:
        output_path.write_text(json.dumps({name: item for name, item in outputs.items() if name != missing}), encoding="utf-8")
        step, result = evidence.run(f"reject_missing_services_{missing}", renderer, workspace, environment)
        evidence.check(step, "missing_output_named", result.returncode == 2 and f"Missing platform output: {missing}" in result.stderr)
        evidence.check(step, "output_unchanged", sha256(path.read_bytes()) == original)
    for name, broken in {"null": {"value": None}, "no_value": {}, "wrong_shape": DISCLOSURE_SENTINEL}.items():
        output_path.write_text(json.dumps({**outputs, "cluster_name": broken}), encoding="utf-8")
        step, result = evidence.run(f"reject_invalid_services_output_{name}", renderer, workspace, environment)
        evidence.check(step, "missing_output_named", result.returncode == 2 and "Missing platform output: cluster_name" in result.stderr)
    output_path.write_text(json.dumps(outputs), encoding="utf-8")
    error_marker = workspace / "output_error"
    error_marker.touch()
    step, result = evidence.run("reject_failed_services_output", renderer, workspace, environment)
    evidence.check(step, "read_failed_without_values", result.returncode == 2 and "platform outputs could not be read" in result.stderr)
    for name, missing_env in (("validate_services_without_platform", False), ("validate_services_without_env", True)):
        if missing_env:
            (workspace / ".env").rename(workspace / "unused.env")
        before_calls = len(call_path.read_text(encoding="utf-8").splitlines())
        step, result = evidence.run(name, [*wrapper, "validate"], workspace, environment)
        calls = [json.loads(line) for line in call_path.read_text(encoding="utf-8").splitlines()][before_calls:]
        evidence.check(step, "validation_ignores_render_inputs", result.returncode == 0 and [call["arguments"][1] for call in calls] == ["validate"])
        evidence.check(step, "services_validation_directory", calls[-1]["arguments"] == [f"-chdir={workspace / 'infra/services'}", "validate"])
        evidence.check(step, "generated_inputs_unchanged", sha256(path.read_bytes()) == original)
        if missing_env:
            (workspace / "unused.env").rename(workspace / ".env")
    error_marker.unlink()
    for name, foundation_value in SERVICES_FOUNDATION.items():
        bad_values = (
            {"type": DISCLOSURE_SENTINEL, "member": {"component": 7}}
            if isinstance(foundation_value, dict)
            else {"type": {"bad": DISCLOSURE_SENTINEL}, "empty": ""}
        )
        for kind, bad in bad_values.items():
            output_path.write_text(json.dumps({**outputs, name: {"value": bad}}), encoding="utf-8")
            step, result = evidence.run(f"reject_services_output_{kind}_{name}", renderer, workspace, environment)
            evidence.check(step, "output_type_named", result.returncode == 2 and f"Invalid platform output type: {name}" in result.stderr)
            evidence.check(step, "output_unchanged", sha256(path.read_bytes()) == original)
    output_path.write_text("[]", encoding="utf-8")
    step, result = evidence.run("reject_nonobject_services_outputs", renderer, workspace, environment)
    evidence.check(step, "object_required", result.returncode == 2 and "Terraform must return a JSON object" in result.stderr)
    output_path.write_text(DISCLOSURE_SENTINEL, encoding="utf-8")
    step, result = evidence.run("reject_malformed_services_output", renderer, workspace, environment)
    evidence.check(step, "invalid_response_rejected", result.returncode == 2 and "platform outputs" in result.stderr)
    output_path.write_text(json.dumps(outputs), encoding="utf-8")
    images = workspace / "infra/services/images.auto.tfvars.json"
    images.write_text(json.dumps({"image_digests": SERVICES_DIGESTS}), encoding="utf-8")
    images_before = sha256(images.read_bytes())
    plan_values = {**expected, "image_digests": SERVICES_DIGESTS}
    plan = platform_plan(plan_values, defaults={"host_memory_mib": 16384})
    plan_path = workspace / "infra/services/reviewed.tfplan"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    step, result = evidence.run("accept_matching_services_plan", [*renderer, "--check-plan", str(plan_path)], workspace, environment)
    evidence.check(step, "matching_plan_accepted", result.returncode == 0)
    images_content = images.read_bytes()
    for label, contents, message in (
        ("missing", None, "Record pushed image digests"),
        ("malformed", DISCLOSURE_SENTINEL, "Record pushed image digests"),
        ("wrong_shape", "[]", "must contain the image_digests map"),
        ("wrong_map", json.dumps({"image_digests": "invalid"}), "must contain the image_digests map"),
    ):
        if contents is None:
            images.unlink()
        else:
            images.write_text(contents, encoding="utf-8")
        step, result = evidence.run(f"reject_services_images_{label}", [*renderer, "--check-plan", str(plan_path)], workspace, environment)
        evidence.check(step, "image_record_rejected", result.returncode == 2 and message in result.stderr)
        images.write_bytes(images_content)
    for label, contents, message in (
        ("missing", None, "The saved plan could not be inspected"),
        ("malformed", DISCLOSURE_SENTINEL, "Terraform returned invalid JSON"),
        ("nonobject", "[]", "Terraform must return a JSON object"),
    ):
        if contents is None:
            plan_path.unlink()
        else:
            plan_path.write_text(contents, encoding="utf-8")
        step, result = evidence.run(f"reject_services_plan_document_{label}", [*renderer, "--check-plan", str(plan_path)], workspace, environment)
        evidence.check(step, "plan_document_rejected", result.returncode == 2 and message in result.stderr)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    changed_inputs = {**expected, "image_digests": {"postgres": "sha256:" + "f" * 64}, "host_memory_mib": 32768}
    for name, value in changed_inputs.items():
        changed = "example_plan_override" if isinstance(value, str) else {"example_override": "example_value"} if isinstance(value, dict) else value
        plan_path.write_text(
            json.dumps(platform_plan(plan_values, defaults={"host_memory_mib": 16384}, variables={name: {"value": changed}})), encoding="utf-8"
        )
        before_calls = len(call_path.read_text(encoding="utf-8").splitlines())
        step, result = evidence.run(f"reject_services_plan_{name}", [*wrapper, "apply", "reviewed.tfplan"], workspace, environment)
        calls = [json.loads(line) for line in call_path.read_text(encoding="utf-8").splitlines()][before_calls:]
        evidence.check(step, "apply_not_started", not any(call["arguments"][1] == "apply" for call in calls))
        evidence.check(step, "changed_input_refused_before_apply", result.returncode == 2 and "Saved plan inputs differ" in result.stderr)
        evidence.check(step, "output_unchanged", sha256(path.read_bytes()) == original)
        evidence.check(step, "values_not_printed", "example_plan_override" not in result.stdout + result.stderr)
    required_plan = platform_plan(plan_values, defaults={"host_memory_mib": 16384})
    required_plan["configuration"]["root_module"]["variables"]["example_required"] = {}
    required_plan["variables"]["example_required"] = {"value": DISCLOSURE_SENTINEL}
    missing_input = platform_plan(plan_values, defaults={"host_memory_mib": 16384})
    for section in (missing_input["variables"], missing_input["configuration"]["root_module"]["variables"]):
        section.pop("cluster_name")
    for name, broken_plan, message in (
        ("required_variable", required_plan, "A services variable without a default is not rendered from its input sources"),
        ("missing_input", missing_input, "declared variables"),
        ("errored", platform_plan(plan_values, defaults={"host_memory_mib": 16384}, errored=True), "Only a complete, successful saved plan"),
        ("incomplete", platform_plan(plan_values, defaults={"host_memory_mib": 16384}, complete=False), "Only a complete, successful saved plan"),
        (
            "undeclared",
            platform_plan(plan_values, defaults={"host_memory_mib": 16384}, variables={"example_extra": {"value": "example_value"}}),
            "declared variables",
        ),
    ):
        plan_path.write_text(json.dumps(broken_plan), encoding="utf-8")
        step, result = evidence.run(f"reject_services_plan_{name}", [*renderer, "--check-plan", str(plan_path)], workspace, environment)
        evidence.check(step, "invalid_plan_refused", result.returncode == 2 and message in result.stderr)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    call_path.write_text("", encoding="utf-8")
    step, result = evidence.run("wrapper_services_plan", [*wrapper, "plan", "-out=services.tfplan"], workspace, environment)
    calls = [json.loads(line) for line in call_path.read_text(encoding="utf-8").splitlines()]
    evidence.check(
        step,
        "services_stack_selected",
        result.returncode == 0 and calls[-1]["arguments"] == [f"-chdir={workspace / 'infra/services'}", "plan", "-input=false", "-out=services.tfplan"],
    )
    evidence.check(step, "identity_verified", (workspace / "identity_called").exists())
    evidence.check(step, "platform_output_uses_project_profile", calls[0]["profile"] == PROFILE and all(not call["static_credentials"] for call in calls))
    evidence.check(step, "values_not_printed", all(value not in result.stdout + result.stderr for value in string_values(expected)))
    (workspace / "identity_called").unlink()
    step, result = evidence.run("wrapper_services_apply", [*wrapper, "apply", "reviewed.tfplan"], workspace, environment)
    calls = [json.loads(line) for line in call_path.read_text(encoding="utf-8").splitlines()]
    evidence.check(
        step,
        "saved_services_plan_applied",
        result.returncode == 0 and calls[-1]["arguments"] == [f"-chdir={workspace / 'infra/services'}", "apply", "-input=false", "reviewed.tfplan"],
    )
    evidence.check(step, "apply_identity_verified", (workspace / "identity_called").is_file())
    evidence.check(step, "image_record_unchanged", sha256(images.read_bytes()) == images_before)
    (workspace / "identity").write_text("example_other_user", encoding="utf-8")
    step, result = evidence.run("reject_services_identity", [*wrapper, "plan"], workspace, environment)
    evidence.check(step, "wrong_identity_refused", result.returncode == 2 and "same-named IAM user" in result.stderr)
    before_calls = len(call_path.read_text(encoding="utf-8").splitlines())
    step, result = evidence.run("reject_services_apply_identity", [*wrapper, "apply", "reviewed.tfplan"], workspace, environment)
    calls = [json.loads(line) for line in call_path.read_text(encoding="utf-8").splitlines()][before_calls:]
    evidence.check(step, "wrong_apply_identity_refused", result.returncode == 2 and "same-named IAM user" in result.stderr)
    evidence.check(step, "apply_not_started", not any(call["arguments"][1] == "apply" for call in calls))


def string_values(value: Any) -> list[str]:
    """List every string leaf so process diagnostics can be checked for input disclosure."""
    if isinstance(value, dict):
        return [text for item in value.values() for text in string_values(item)]
    return [value] if isinstance(value, str) else []


def render_host_boot(template: str, root: Path) -> str:
    """Fill the boot template as Terraform does and move its host paths under a temporary root."""
    script = template
    for name, value in {**BOOT_VALUES, "mount_path": str(root / "data")}.items():
        script = script.replace("${" + name + "}", value)
    if "${" in script or "%{" in script:
        raise CheckFailed("The boot template holds an expression the runner does not fill")
    for name, relative in BOOT_PATHS.items():
        script, count = re.subn(rf'^{name}="[^"]*"$', f'{name}="{root / relative}"', script, flags=re.MULTILINE)
        if count != 1:
            raise CheckFailed(f"The boot template must set {name} once on its own line")
    return script


PRIVILEGE_SETTING = "ECS_DISABLE_PRIVILEGED=true\n"


def privileged_containers_disabled(config: str) -> bool:
    """True when the last ECS_DISABLE_PRIVILEGED line of the agent configuration is true: the agent reads the last value of a variable."""
    values = [line.partition("=")[2] for line in config.splitlines() if line.startswith("ECS_DISABLE_PRIVILEGED=")]
    return bool(values) and values[-1] == "true"


def allow_privileged_containers(template: str, how: str) -> str:
    """Plant the defect of review round 5: the setting is missing, false or overridden later, so a task definition could ask for a privileged container."""
    if template.count(PRIVILEGE_SETTING) != 1:
        raise CheckFailed("The boot template must write the privileged-container setting once")
    replacement = {"missing": "", "false": "ECS_DISABLE_PRIVILEGED=false\n", "overridden": PRIVILEGE_SETTING + "ECS_DISABLE_PRIVILEGED=false\n"}[how]
    return template.replace(PRIVILEGE_SETTING, replacement)


def misplace_dependency(template: str) -> str:
    """Plant the defect the review found: the Docker and ECS drop-ins keep Requires= but in [Service], where systemd ignores it."""
    correct = "[Unit]\nRequires=platform_imds_guard.service\nAfter=platform_imds_guard.service\n\n[Service]\nExecStartPre=$guard_script apply\n"
    misplaced = "[Unit]\nAfter=platform_imds_guard.service\n\n[Service]\nRequires=platform_imds_guard.service\nExecStartPre=$guard_script apply\n"
    if template.count(correct) != 1:
        raise CheckFailed("The boot template must hold the Docker and ECS drop-in text the planted defect replaces")
    return template.replace(correct, misplaced)


def prepare_host_root(root: Path, template: str, stub_overrides: dict[str, str]) -> Path:
    """Create a temporary host root with stand-in commands, a data volume device and the image's kernel defaults."""
    tools = root / "tools"
    for directory in (tools, root / "devices", root / "systemd", root / "state" / "sysctl"):
        directory.mkdir(parents=True)
    for command, body in {**BOOT_STUBS, **stub_overrides}.items():
        (tools / command).write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    for command, source in BOOT_PYTHON_STUBS.items():
        (tools / command).write_text(f"#!{sys.executable} -I\n{source}", encoding="utf-8")
    (tools / "systemctl").write_text(
        f"#!{sys.executable} -I\n{Path(systemd_model.__file__).read_text(encoding='utf-8')}\n{SYSTEMCTL_STUB_BODY}", encoding="utf-8"
    )
    (tools / "timeout").write_text(TIMEOUT_STUB, encoding="utf-8")
    for tool in tools.iterdir():
        tool.chmod(0o700)
    for key, value in BOOT_KERNEL_DEFAULTS.items():
        (root / "state" / "sysctl" / key).write_text(value, encoding="utf-8")
    (root / "devices" / f"nvme-Amazon_Elastic_Block_Store_{BOOT_IDS['volume'].replace('-', '')}").touch()
    (root / "fstab").touch()
    script = root / "host_boot.sh"
    script.write_text(render_host_boot(template, root), encoding="utf-8")
    return script


def read_lines(path: Path) -> list[str]:
    """Return a state file's lines, or none when the step never wrote it."""
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def probe_metadata(evidence: Evidence, root: Path, environment: dict[str, str], name: str, uid: int) -> int | None:
    """Request the metadata service as one user through the stand-ins and return curl's exit status."""
    tools = root / "tools"
    request = ["curl", "-s", "-o", "/dev/null", "--max-time", "3", "http://169.254.169.254/latest/meta-data/"]
    command = (
        [str(tools / "curl"), *request[1:]] if uid == 0 else [str(tools / "setpriv"), f"--reuid={uid}", f"--regid={uid}", "--clear-groups", "--", *request]
    )
    _, result = evidence.run(name, command, root, environment)
    status: int | None = result.returncode
    return status


def check_metadata_isolation(evidence: Evidence, root: Path, environment: dict[str, str], name: str) -> None:
    """Require root to reach the metadata service and two application users to be dropped."""
    outcomes = {uid: probe_metadata(evidence, root, environment, f"{name}_uid_{uid}", uid) for uid in (0, 1000, 50000)}
    step = evidence.report["steps"][-1]
    evidence.check(step, f"{name}_root_allowed_application_users_denied", outcomes == {0: 0, 1000: 28, 50000: 28})


def unit_command(root: Path, unit: str, key: str) -> list[str]:
    """Return the one command a written unit file sets for a key, split as systemd splits a plain command line."""
    lines = [line.removeprefix(key) for line in read_lines(root / "systemd" / unit) if line.startswith(key)]
    if len(lines) != 1:
        raise CheckFailed(f"{unit} must set {key} once")
    return lines[0].split()


def parsed_unit(root: Path, unit: str) -> dict[str, dict[str, list[str]]]:
    """Read a written unit and its drop-ins the way systemd does: each directive only in the section that defines it."""
    sections: dict[str, dict[str, list[str]]] = systemd_model.read_unit(root / "systemd", unit, {}, [])
    return sections


def unit_values(root: Path, unit: str, section: str, key: str) -> list[str]:
    """Return the values of one directive as systemd reads them, or none when the section does not define it."""
    return parsed_unit(root, unit).get(section, {}).get(key, [])


def check_unit_sections(evidence: Evidence, root: Path) -> None:
    """Require every written unit to keep each directive in its own section and the dependencies to read as intended."""
    step = evidence.report["steps"][-1]
    guard = str(root / BOOT_PATHS["guard_script"])
    firewall = "platform_imds_guard.service"
    evidence.check(step, "units_have_no_misplaced_directive", not set(read_lines(root / "state" / "systemd_warnings.log")))
    evidence.check(
        step,
        "guard_unit_dependencies_in_unit_section",
        unit_values(root, firewall, "Unit", "PartOf") == ["nftables.service"]
        and unit_values(root, firewall, "Unit", "ReloadPropagatedFrom") == ["nftables.service"]
        and unit_values(root, firewall, "Unit", "After") == ["nftables.service"]
        and unit_values(root, firewall, "Unit", "Before") == ["docker.service", "ecs.service"],
    )
    evidence.check(
        step,
        "guard_unit_commands_in_service_section",
        unit_values(root, firewall, "Service", "Type") == ["oneshot"]
        and unit_values(root, firewall, "Service", "RemainAfterExit") == ["yes"]
        and unit_values(root, firewall, "Service", "ExecStart") == [f"{guard} apply"],
    )
    for name in ("docker.service", "ecs.service"):
        evidence.check(
            step,
            f"{name.removesuffix('.service')}_requires_the_guard",
            unit_values(root, name, "Unit", "Requires") == [firewall]
            and unit_values(root, name, "Unit", "After") == [firewall]
            and unit_values(root, name, "Service", "ExecStartPre") == [f"{guard} apply"]
            and not unit_values(root, name, "Service", "Requires"),
        )
    evidence.check(
        step,
        "timer_directives_in_timer_section",
        unit_values(root, "platform_imds_watch.timer", "Timer", "OnUnitActiveSec") == ["10s"]
        and unit_values(root, "platform_imds_watch.timer", "Install", "WantedBy") == ["timers.target"],
    )


def restart_firewall(evidence: Evidence, root: Path, environment: dict[str, str], label: str) -> list[str]:
    """Start the ECS agent as the image does after boot, restart the firewall service and return what happened in order."""
    tools = root / "tools"
    state = root / "state"
    step, result = evidence.run(f"{label}_start_agent", [str(tools / "systemctl"), "start", "ecs.service"], root, environment)
    evidence.check(
        step, "docker_and_agent_running", result.returncode == 0 and {"docker.service", "ecs.service"} <= set(json.loads((state / "active.json").read_text()))
    )
    (state / "events.log").write_text("", encoding="utf-8")
    step, result = evidence.run(f"{label}_restart_firewall", [str(tools / "systemctl"), "restart", "nftables.service"], root, environment)
    evidence.check(step, "restart_succeeded", result.returncode == 0)
    return read_lines(state / "events.log")


def run_firewall_cases(evidence: Evidence, root: Path, environment: dict[str, str]) -> None:
    """After a successful boot, reload, flush and restart the firewall and restart Docker and the ECS agent."""
    tools = root / "tools"
    state = root / "state"
    check_unit_sections(evidence, root)
    events = restart_firewall(evidence, root, environment, "dependency")
    step = evidence.report["steps"][-1]
    step["events"] = [evidence.redact(line) for line in events]
    removed = [index for index, line in enumerate(events) if line.startswith("rule_removed")]
    stopped = [events.index(f"stop {unit}") for unit in ("ecs.service", "docker.service") if f"stop {unit}" in events]
    evidence.check(step, "containers_stopped_with_the_firewall", len(stopped) == 2 and events.index("stop platform_imds_guard.service") > max(stopped))
    evidence.check(
        step,
        "containers_stopped_before_the_rule_went",
        bool(removed) and all(events[index].endswith("containers_running=none") for index in removed) and max(stopped) < min(removed),
    )
    loaded = events.index("rule_loaded") if "rule_loaded" in events else len(events)
    evidence.check(
        step,
        "containers_started_after_the_rule_returned",
        all(f"start {unit}" in events and events.index(f"start {unit}") > loaded for unit in ("docker.service", "ecs.service")),
    )
    evidence.check(step, "containers_running_again", {"docker.service", "ecs.service"} <= set(json.loads((state / "active.json").read_text())))
    check_metadata_isolation(evidence, root, environment, "after_firewall_restart")

    check_metadata_isolation(evidence, root, environment, "after_boot")
    reload_file = root / "nftables_reload.nft"
    reload_file.write_text(f'flush ruleset\ninclude "{root / BOOT_PATHS["nftables_config"]}"\n', encoding="utf-8")
    step, result = evidence.run("nftables_include_reload", [str(tools / "nft"), "-f", str(reload_file)], root, environment)
    evidence.check(step, "reload_succeeded", result.returncode == 0)
    check_metadata_isolation(evidence, root, environment, "after_nftables_reload")
    (state / "events.log").write_text("", encoding="utf-8")
    step, result = evidence.run("nftables_service_reload", [str(tools / "systemctl"), "reload", "nftables.service"], root, environment)
    events = read_lines(state / "events.log")
    evidence.check(
        step,
        "reload_reaches_the_guard_and_stops_nothing",
        result.returncode == 0 and "reload platform_imds_guard.service" in events and not any(line.startswith(("stop ", "rule_removed")) for line in events),
    )
    check_metadata_isolation(evidence, root, environment, "after_firewall_service_reload")

    watch = unit_command(root, "platform_imds_watch.service", "ExecStart=")
    for restart in ("docker.service", "ecs.service", None):
        step, result = evidence.run(f"flush_before_{restart or 'timer'}", [str(tools / "nft"), "flush", "ruleset"], root, environment)
        evidence.check(
            step, "rule_removed", result.returncode == 0 and probe_metadata(evidence, root, environment, f"flushed_before_{restart or 'timer'}", 50000) == 0
        )
        command = [str(tools / "systemctl"), "restart", restart] if restart else watch
        step, result = evidence.run(f"restart_{restart}" if restart else "timer_check_restores_rule", command, root, environment)
        evidence.check(step, "rule_loaded_before_start", result.returncode == 0)
        check_metadata_isolation(evidence, root, environment, f"after_{restart or 'timer'}")
    evidence.check(evidence.report["steps"][-1], "timer_stopped_nothing", not any(line.startswith("stop ") for line in read_lines(state / "systemctl.log")))

    failing = {**environment, "BOOT_NFT_FAIL": "1"}
    for restart in ("docker.service", "ecs.service"):
        evidence.run(f"flush_before_failed_{restart}", [str(tools / "nft"), "flush", "ruleset"], root, environment)
        step, result = evidence.run(f"restart_{restart}_without_rule", [str(tools / "systemctl"), "restart", restart], root, failing)
        evidence.check(step, "start_refused", result.returncode != 0 and "Could not load the metadata firewall rule." in result.stderr)
    step, result = evidence.run("timer_check_cannot_restore_rule", watch, root, failing)
    evidence.check(step, "timer_failed", result.returncode != 0 and "stopping the ECS agent and Docker" in result.stderr)
    evidence.check(step, "containers_stopped", read_lines(state / "systemctl.log")[-1].split()[:3] == ["stop", "ecs.service", "docker.service"])


def run_planted_dependency_defect(evidence: Evidence, root: Path, environment: dict[str, str]) -> None:
    """Prove the dependency checks catch the review's defect: Requires= in [Service] is reported and leaves containers running while the rule is gone."""
    state = root / "state"
    warnings = set(read_lines(state / "systemd_warnings.log"))
    step = evidence.report["steps"][-1]
    evidence.check(
        step,
        "misplaced_directive_reported",
        any("Unknown key name 'Requires' in section 'Service'" in line for line in warnings)
        and not warnings.isdisjoint({line for line in warnings if "docker.service.d" in line}),
    )
    evidence.check(step, "misplaced_directive_is_not_a_dependency", not unit_values(root, "docker.service", "Unit", "Requires"))
    events = restart_firewall(evidence, root, environment, "planted")
    step = evidence.report["steps"][-1]
    step["events"] = [evidence.redact(line) for line in events]
    unsafe = [line for line in events if line.startswith("rule_removed") and not line.endswith("containers_running=none")]
    evidence.check(step, "containers_keep_running_while_the_rule_is_gone", bool(unsafe))
    stopped = [unit_name for unit_name in ("docker.service", "ecs.service") if f"stop {unit_name}" in events]
    evidence.check(step, "firewall_restart_did_not_stop_the_containers", not stopped)


def boot_host(
    evidence: Evidence, bash: str, name: str, template: str, stub_overrides: dict[str, str], case_environment: dict[str, str], gnu_timeout: str
) -> tuple[Path, dict[str, str], dict[str, Any], Any]:
    """Run the boot script once in its own temporary host root and return the root, the stand-in environment, the step and the result."""
    root = evidence.temporary / name
    script = prepare_host_root(root, template, stub_overrides)
    environment = {
        "PATH": f"{root / 'tools'}:/usr/bin:/bin",
        "BOOT_TIMEOUT_LOG": str(root / "deadlines.log"),
        "BOOT_TIMEOUT_CAP": "2",
        "BOOT_GRACE_CAP": "1",
        "BOOT_GNU_TIMEOUT": gnu_timeout,
        "BOOT_STATE": str(root / "state"),
        "BOOT_SYSTEMD": str(root / "systemd"),
        "BOOT_IDS": json.dumps(BOOT_IDS),
        "BOOT_DATA_VOLUME_NAME": BOOT_VALUES["data_volume_name"],
        "BOOT_ADDRESS_NAME": BOOT_VALUES["address_name"],
        "BOOT_PROJECT_TAG": BOOT_VALUES["project_tag"],
        "BOOT_NFTABLES_CONFIG": str(root / BOOT_PATHS["nftables_config"]),
        "BOOT_STATE_INITIAL": "nftables.service",
    }
    step, result = evidence.run(name, [bash, str(script)], root, {**environment, **case_environment})
    return root, environment, step, result


def run_host_boot_cases(evidence: Evidence, bash: str) -> None:
    """Run the host boot script against stand-in system commands; a stalled or failed step must stop the boot in time."""
    template = (REPO_ROOT / BOOT_TEMPLATE).read_text(encoding="utf-8")
    evidence.report["source_hashes"][BOOT_TEMPLATE] = {"original_sha256": sha256(template.encode())}
    gnu_timeout = shutil.which("gtimeout") or shutil.which("timeout") or ""
    evidence.report["boot_timeout"] = gnu_timeout or "the GNU-compatible stand-in in TIMEOUT_STUB; no GNU timeout on PATH"
    cases: dict[str, tuple[dict[str, str], dict[str, str], str | None]] = {
        "boot_completes": ({}, {}, None),
        "boot_stalled_service_start": (
            {},
            {"BOOT_SYSTEMCTL_STALL": "amazon-ssm-agent"},
            "Starting the Session Manager agent did not finish within 120 seconds.",
        ),
        "boot_stalled_format": ({"mkfs.xfs": STALL}, {}, "Formatting the data volume did not finish within 300 seconds."),
        "boot_stalled_mount": ({"mount": STALL}, {}, "Mounting the data volume did not finish within 120 seconds."),
        "boot_format_ignores_term": (
            {"mkfs.xfs": STALL_IGNORING_TERM},
            {},
            "Formatting the data volume did not finish within 300 seconds and ignored the stop signal, so it was killed 10 seconds later.",
        ),
        "boot_firewall_fails_to_load": ({}, {"BOOT_NFT_FAIL": "1"}, "The metadata firewall did not start, so the host does not join the cluster."),
        "boot_core_dump_settings_ignored": ({}, {"BOOT_SYSCTL_IGNORE": "1"}, "The core dump settings did not take effect"),
        "boot_data_volume_not_found": ({}, {"BOOT_VOLUME_RESULT": ""}, "Exactly one data volume must carry the project tags"),
        "boot_data_volume_duplicated": (
            {},
            {"BOOT_VOLUME_RESULT": "vol-0123456789abcdef0\tvol-0fedcba987654321"},
            "Exactly one data volume must carry the project tags",
        ),
        "boot_data_volume_malformed": ({}, {"BOOT_VOLUME_RESULT": "not-an-id"}, "Exactly one data volume must carry the project tags"),
        "boot_data_volume_search_fails": ({}, {"BOOT_AWS_FAIL": "describe-volumes"}, "The data volume could not be listed."),
        "boot_elastic_ip_not_found": ({}, {"BOOT_ALLOCATION_RESULT": ""}, "Exactly one Elastic IP must carry the project tags"),
        "boot_elastic_ip_duplicated": (
            {},
            {"BOOT_ALLOCATION_RESULT": "eipalloc-0123456789abcdef0\teipalloc-0fedcba987654321"},
            "Exactly one Elastic IP must carry the project tags",
        ),
        "boot_elastic_ip_malformed": ({}, {"BOOT_ALLOCATION_RESULT": "not-an-id"}, "Exactly one Elastic IP must carry the project tags"),
        "boot_elastic_ip_search_fails": ({}, {"BOOT_AWS_FAIL": "describe-addresses"}, "The Elastic IP could not be listed."),
        "boot_with_misplaced_dependency": ({}, {}, None),
        "boot_with_privileged_containers_allowed": ({}, {}, None),
        "boot_without_the_privilege_setting": ({}, {}, None),
        "boot_with_the_privilege_setting_overridden": ({}, {}, None),
    }
    planted_privilege = {
        "boot_with_privileged_containers_allowed": "false",
        "boot_without_the_privilege_setting": "missing",
        "boot_with_the_privilege_setting_overridden": "overridden",
    }
    for name, (stub_overrides, case_environment, message) in cases.items():
        planted = name == "boot_with_misplaced_dependency"
        case_template = (
            misplace_dependency(template)
            if planted
            else allow_privileged_containers(template, planted_privilege[name])
            if name in planted_privilege
            else template
        )
        root, environment, step, result = boot_host(evidence, bash, name, case_template, stub_overrides, case_environment, gnu_timeout)
        log = (root / "boot.log").read_text(encoding="utf-8") if (root / "boot.log").exists() else ""
        config = (root / "ecs.config").read_text(encoding="utf-8") if (root / "ecs.config").exists() else ""
        deadlines = read_lines(root / "deadlines.log")
        metadata = read_lines(root / "state" / "metadata_requests.log")
        step["boot_log"] = evidence.redact(log)
        step["requested_deadlines"] = [evidence.redact(line) for line in deadlines]
        step["metadata_requests"] = metadata
        evidence.check(step, "every_deadline_kills_after_a_grace_period", all(line.startswith("--kill-after=") for line in deadlines))
        rule_checked_first = "65534 dropped" in metadata and metadata.index("65534 dropped") < metadata.index("0 reached") if "0 reached" in metadata else True
        evidence.check(step, "no_root_metadata_read_before_the_rule", rule_checked_first)
        if message is None:
            sysctl = root / "state" / "sysctl"
            evidence.check(step, "boot_succeeded", result.returncode == 0)
            evidence.check(step, "cluster_joined_last", config.rstrip("\n").splitlines()[-1] == f"ECS_CLUSTER={BOOT_VALUES['cluster_name']}")
            evidence.check(step, "host_network_task_roles", "ECS_ENABLE_TASK_IAM_ROLE_NETWORK_HOST=true" in config.splitlines())
            if name in planted_privilege:
                evidence.check(step, "planted_privileged_containers_are_detected", not privileged_containers_disabled(config))
            else:
                evidence.check(step, "privileged_containers_disabled", privileged_containers_disabled(config))
            evidence.check(
                step,
                "every_slow_step_bounded",
                {
                    "--kill-after=10 120 systemctl enable --now platform_imds_guard.service",
                    "--kill-after=10 120 systemctl enable --now amazon-ssm-agent",
                    f"--kill-after=10 300 mkfs.xfs {(root / 'devices').resolve()}/nvme-Amazon_Elastic_Block_Store_{BOOT_IDS['volume'].replace('-', '')}",
                    f"--kill-after=10 120 mount {root / 'data'}",
                    f"--kill-after=10 30 sysctl -q -p {root / BOOT_PATHS['sysctl_file']}",
                }
                <= set(deadlines),
            )
            calls = read_lines(root / "state" / "aws_calls.log")
            step["aws_calls"] = calls
            evidence.check(
                step,
                "volume_and_address_found_by_tag_under_a_deadline",
                all(any(line.startswith(f"--kill-after=10 60 aws ec2 {call}") for line in deadlines) for call in ("describe-volumes", "describe-addresses")),
            )
            evidence.check(
                step,
                "found_ids_used_for_association_and_attach",
                any("associate-address" in line and BOOT_IDS["allocation"] in line for line in calls)
                and any("attach-volume" in line and BOOT_IDS["volume"] in line for line in calls),
            )
            evidence.check(step, "data_volume_in_fstab", f"{root / 'data'} xfs defaults,nofail 0 2" in (root / "fstab").read_text(encoding="utf-8"))
            evidence.check(
                step,
                "plain_core_dumps_without_set_user_id",
                (sysctl / "kernel.core_pattern").read_text(encoding="utf-8") == "core" and (sysctl / "fs.suid_dumpable").read_text(encoding="utf-8") == "0",
            )
            evidence.check(step, "timer_started", "enable --now platform_imds_watch.timer" in read_lines(root / "state" / "systemctl.log"))
            if planted:
                run_planted_dependency_defect(evidence, root, environment)
            elif name not in planted_privilege:
                run_firewall_cases(evidence, root, environment)
        else:
            evidence.check(step, "boot_failed", result.returncode not in (0, None))
            evidence.check(step, "failure_reported", message in log)
            evidence.check(step, "stopped_within_deadline", step["duration_seconds"] < 15)
            evidence.check(step, "cluster_not_joined", "ECS_CLUSTER=" not in config)


def run_suite(evidence: Evidence) -> None:
    """Verify configuration lifecycle and pre-deployment guards without AWS access."""
    sources = fixture_sources(evidence)
    (evidence.temporary / "home").mkdir()
    tools = evidence.temporary / "tools"
    tools.mkdir()
    dirname = shutil.which("dirname", path="/usr/bin:/bin")
    bash = shutil.which("bash", path="/bin:/usr/bin")
    if not dirname or not bash:
        raise CheckFailed("This check requires the real bash and dirname utilities")
    (tools / "dirname").symlink_to(dirname)
    terraform = shutil.which("terraform")
    if not terraform:
        raise CheckFailed("This check requires the real terraform command")
    (tools / "terraform").symlink_to(terraform)
    workspace = copied_workspace(evidence.temporary, "lifecycle", sources, FIXTURE)
    renderer = [sys.executable, "-m", "scripts.infrastructure.render_project_config"]
    shell_renderer = [bash, "scripts/infrastructure/render_project_config.sh", "--admin-profile", ADMIN]
    step, result = evidence.run("render_initial", shell_renderer, workspace)
    evidence.check(step, "render_succeeded", result.returncode == 0 and not result.stderr)
    initial_hashes = evidence.capture_outputs(step, workspace, FIXTURE["AWS_REGION"])
    step, result = evidence.run("check_initial", [*renderer, "--check"], workspace)
    evidence.check(step, "matching_inputs_accepted", result.returncode == 0 and "Verified .env-derived" in result.stdout)
    write_environment(workspace, {**FIXTURE, "AWS_REGION": "us-east-2"})
    for stack in ("storage", "iam"):
        step, result = evidence.run(f"reject_stale_{stack}", [*renderer, "--stack", stack, "--check"], workspace)
        evidence.check(step, "stale_inputs_rejected", result.returncode == 2 and "Generated configuration is stale" in result.stderr and not result.stdout)
        evidence.check(step, "outputs_unchanged", {name: sha256((workspace / name).read_bytes()) for name in OUTPUT_NAMES} == initial_hashes)
    step, result = evidence.run("render_refreshed", shell_renderer, workspace)
    evidence.check(step, "rerender_succeeded", result.returncode == 0 and not result.stderr)
    evidence.capture_outputs(step, workspace, "us-east-2")
    step, result = evidence.run("check_refreshed", [*renderer, "--check"], workspace)
    evidence.check(step, "refreshed_inputs_accepted", result.returncode == 0 and "Verified .env-derived" in result.stdout)
    run_platform_cases(evidence, sources)
    run_services_cases(evidence, sources, bash)
    run_host_boot_cases(evidence, bash)

    alternate = copied_workspace(
        evidence.temporary,
        "alternate_project",
        sources,
        {
            **FIXTURE,
            "PROJECT_NAME": "sample_project",
            "AWS_PROFILE": "sample_project_dev",
            "AWS_REGION": "ap-southeast-2",
        },
    )
    step, result = evidence.run("render_alternate_project", shell_renderer, alternate)
    evidence.check(step, "alternate_project_accepted_without_source_patching", result.returncode == 0 and not result.stderr)
    evidence.capture_outputs(step, alternate, "ap-southeast-2", "sample_project")

    isolated = copied_workspace(evidence.temporary, "unsupported_setting", sources, {**FIXTURE, "EXAMPLE_UNUSED_SETTING": DISCLOSURE_SENTINEL})
    step, result = evidence.run("reject_unsupported_setting", renderer, isolated)
    evidence.check(step, "unsupported_setting_rejected", result.returncode == 2 and "Unsupported .env settings" in result.stderr)
    evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())

    for admin in ("", " ", PROFILE):
        isolated = copied_workspace(evidence.temporary, f"invalid_admin_{len(admin)}", sources, FIXTURE)
        step, result = evidence.run(f"reject_admin_{len(admin)}", [*renderer, "--admin-profile", admin], isolated)
        evidence.check(step, "invalid_admin_rejected", result.returncode == 2 and "explicitly selected administrator" in result.stderr)
        evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())

    invalid_values = {
        "AWS_ACCOUNT_ID": "invalid_private_account",
        "AWS_REGION": "invalid_private_region",
        "PROJECT_NAME": "invalid-private-project",
        "S3_BUCKET": "invalid_private_bucket*",
        "AWS_PROFILE": "example_unrelated_user",
        "ENVIRONMENT": "invalid_private_environment",
    }
    for key, value in invalid_values.items():
        evidence.redactions[value] = "<REDACTED_INVALID_VALUE>"
        isolated = copied_workspace(evidence.temporary, f"invalid_{key.lower()}", sources, {**FIXTURE, key: value})
        step, result = evidence.run(f"reject_invalid_{key.lower()}", renderer, isolated)
        evidence.check(step, "invalid_input_rejected", result.returncode == 2 and f"Set a valid {key}" in result.stderr)
        evidence.check(step, "offending_value_not_echoed", value not in result.stdout + result.stderr)
        evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())

    isolated = copied_workspace(evidence.temporary, "missing_env", sources, FIXTURE)
    step, result = evidence.run("reject_missing_env", [*renderer, "--env-file", "missing.env"], isolated)
    evidence.check(step, "missing_env_rejected", result.returncode == 2 and "selected .env file does not exist" in result.stderr)
    evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())
    isolated = copied_workspace(evidence.temporary, "shell_expansion", sources, {**FIXTURE, "AWS_ACCOUNT_ID": "${PRIVATE_ACCOUNT}"})
    step, result = evidence.run("reject_shell_expansion", renderer, isolated, {"PRIVATE_ACCOUNT": "111111111111"})
    evidence.check(step, "shell_value_not_expanded", result.returncode == 2 and "Set a valid AWS_ACCOUNT_ID" in result.stderr)
    evidence.check(step, "no_output_or_value_disclosure", not result.stdout and "${PRIVATE_ACCOUNT}" not in result.stderr and not (isolated / "infra").exists())

    isolated = copied_workspace(evidence.temporary, "wrapper_guards", sources, FIXTURE)
    guards: list[tuple[list[str], dict[str, str], str]] = [
        (["--iam", "plan"], {}, "IAM updates require --admin-profile."),
        (["--admin-profile", ADMIN, "plan"], {}, "Administrator profiles are allowed only with --iam."),
        (["--platform", "--admin-profile", ADMIN, "plan"], {}, "Administrator profiles are allowed only with --iam."),
        (["--iam", "--platform", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
        (["--platform", "--platform", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
        (["--services", "--admin-profile", ADMIN, "plan"], {}, "Administrator profiles are allowed only with --iam."),
        (["--services", "--platform", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
        (["--platform", "--services", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
        (["--services", "--services", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
        (["--services", "apply"], {}, "Apply requires one reviewed saved-plan path."),
        (["--platform", "apply"], {}, "Apply requires one reviewed saved-plan path."),
        (["apply"], {}, "Apply requires one reviewed saved-plan path."),
        (["plan", "-var=aws_profile=example_unapproved"], {}, "Set project inputs only in .env."),
        (["plan", "-chdir=example_other"], {}, "Set project inputs only in .env."),
    ]
    for name, arguments in (
        ("TF_CLI_ARGS", ["plan"]),
        ("TF_CLI_ARGS_plan", ["plan"]),
        ("TF_CLI_ARGS_apply", ["apply", "example.tfplan"]),
        ("TF_CLI_ARGS_show", ["apply", "example.tfplan"]),
        ("TF_CLI_ARGS_validate", ["validate"]),
        ("TF_CLI_ARGS_future", ["validate"]),
    ):
        guards.append((arguments, {name: "-var=project_name=example_unapproved"}, f"Unset {name};"))
    guards.extend(
        [
            (["--iam", "--jobs", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
            (["--jobs", "--iam", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
            (["--platform", "--jobs", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
            (["--jobs", "--platform", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
            (["--jobs", "--jobs", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
            (["--jobs", "--admin-profile", ADMIN, "plan"], {}, "Administrator profiles are allowed only with --iam."),
            (["--jobs", "apply"], {}, "Apply requires one reviewed saved-plan path."),
            (["--services", "--jobs", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
            (["--jobs", "--services", "plan"], {}, "Choose one stack: --iam, --platform, --services or --jobs."),
        ]
    )
    for index, (arguments, exports, message) in enumerate(guards, start=1):
        step, result = evidence.run(
            f"wrapper_guard_{index:02d}",
            [bash, "scripts/infrastructure/terraform.sh", *arguments],
            isolated,
            {**exports, "PYTHON_BIN": str(isolated / "must_not_run")},
        )
        step["synthetic_environment_overrides"] = {key: evidence.redact(value) for key, value in exports.items()}
        evidence.check(step, "guard_rejected_before_renderer", result.returncode == 2 and message in result.stderr and "must_not_run" not in result.stderr)
        evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())
        evidence.check(step, "no_injected_value_disclosure", "example_unapproved" not in result.stderr)

    check_saved_plans(evidence, sources, renderer, shell_renderer)

    before = {path.relative_to(evidence.output).as_posix(): sha256(path.read_bytes()) for path in evidence.output.rglob("*") if path.is_file()}
    step, result = evidence.run(
        "refuse_existing_evidence",
        [sys.executable, "-m", "scripts.infrastructure.run_e2e", "--output-directory", str(evidence.output)],
        REPO_ROOT,
    )
    evidence.check(step, "existing_directory_rejected", result.returncode == 2 and "Output directory already exists" in result.stderr)
    after = {path.relative_to(evidence.output).as_posix(): sha256(path.read_bytes()) for path in evidence.output.rglob("*") if path.is_file()}
    evidence.check(step, "existing_evidence_unchanged", before == after)


def main() -> None:
    """Run synthetic E2E checks and retain redacted evidence at a fresh path."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True, help="New directory for redacted evidence; existing paths are refused.")
    arguments = parser.parse_args()
    output = arguments.output_directory.expanduser().absolute()
    try:
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("Output directory already exists; choose a new directory.")
    output.chmod(0o700)
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix="configuration_e2e_") as directory:
        evidence = Evidence(output, Path(directory))
        try:
            run_suite(evidence)
            evidence.report["status"] = "passed"
        except (CheckFailed, OSError, ValueError, TypeError) as error:
            evidence.report.update(status="failed", failure=evidence.redact(str(error)))
        finally:
            evidence.finish()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger(__name__).info("event=configuration_e2e status=%s processes=%s", evidence.report["status"], evidence.report["process_count"])
    if evidence.report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""Run repository source checks from the same inventory in pre-commit and CI.

Run ``.venv/bin/python -m scripts.quality.lint_sources <check>``. Ruff and shell syntax cover every nonignored
first-party source. Native language tools check tracked sources. SQL profiles follow the executing engine.
No script body, SQL statement or container is executed by this module.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import shlex
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.process import LaunchesNotStopped, TimeoutExpired, find_program, run_command
from scripts.quality.repo_checks import load_yaml

ROOT = Path(__file__).resolve().parents[2]
# The limit in seconds of one SQLFluff run. A run over every dbt source takes about 14 minutes on a hosted runner and cannot run in parallel
# (`processes = 1` in .sqlfluff). The sql_style group took 674 to 908 seconds in six hosted runs (Oct 29 to 31 2026), of which about 30 seconds are
# the macro usage check, so 900 seconds left the slowest run 2 percent of room. This limit only stops a hung run: it stays below the 30 minute
# job timeout, which leaves about 100 seconds for setup and the other steps of that job.
SQLFLUFF_LIMIT = 1500
CHECKS = ("ruff", "shell-syntax", "shellcheck", "actionlint", "yamllint", "build-contexts", "hadolint", "gofmt", "go-vet", "sql")
PRIVATE = {".git", ".venv", ".tools", "data", "plans", "node_modules", ".aws", ".codex", ".agents"}
SHELLS = {"sh", "bash", "dash", "zsh", "ksh"}
TINY_FIXTURE_REASON = "synthetic dbt project left from the retired transform scheduler E2E"
EXCLUDED_SQL = {
    "services/analytics/bronze.sql": "DuckDB CLI init script whose LOAD, CREATE SECRET and ATTACH statements are outside the pinned grammar",
    "tests/orchestration/fixtures/tiny/models/catalog_marker.sql": TINY_FIXTURE_REASON,
    "tests/orchestration/fixtures/tiny/models/gold_one.sql": TINY_FIXTURE_REASON,
    "tests/orchestration/fixtures/tiny/models/int_silver_build_marker.sql": TINY_FIXTURE_REASON,
    "tests/orchestration/fixtures/tiny/models/silver_one.sql": TINY_FIXTURE_REASON,
    "tests/orchestration/fixtures/tiny/models/silver_two.sql": TINY_FIXTURE_REASON,
    "tests/orchestration/fixtures/tiny/tests/cross_layer_rows.sql": TINY_FIXTURE_REASON,
}


def paths(tracked: bool = False) -> list[str]:
    arguments = ["ls-files", "-z", "--cached", "--exclude-standard", *([] if tracked else ["--others"])]
    listed = run_command("git", arguments, cwd=ROOT, timeout=60, check=True).stdout
    selected = []
    for name in sorted(set(listed.split("\0")) - {""}):
        path = Path(name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"{name}:1: unsafe source path")
        if path.parts[0] in PRIVATE or path.name == ".env" or path.name.startswith(".env."):
            continue
        if {"secrets", "keys", "tokens", ".ssh"}.intersection(path.parts) or path.suffix in {".pem", ".key"} or name.endswith(".tfvars.json"):
            continue
        selected.append(name)
    return selected


def safe_source(name: str) -> Path:
    path = ROOT / name
    if path.is_symlink() or any(parent.is_symlink() for parent in path.parents if parent != ROOT and ROOT in parent.parents):
        raise ValueError(f"{name}:1: source symlinks are not supported. Replace the link with reviewed source or ask the owner.")
    if not path.is_file():
        raise ValueError(f"{name}:1: source is missing or not a regular file")
    return path


def shell_sources(tracked: bool = False) -> dict[str, str]:
    selected = {}
    for name in paths(tracked):
        path = ROOT / name
        extension = path.suffix in {".sh", ".bash", ".zsh", ".ksh"}
        if not extension and path.suffix:
            continue
        path = safe_source(name)
        with path.open("rb") as stream:
            first = stream.readline(512)
        if not first.startswith(b"#!"):
            if extension:
                raise ValueError(f"{name}:1: shell source needs a declared interpreter")
            continue
        try:
            words = shlex.split(first[2:].decode("utf-8", errors="replace").strip())
        except ValueError as error:
            raise ValueError(f"{name}:1: invalid shell shebang: {error}. Fix the declared interpreter or ask the owner.") from error
        if words and Path(words[0]).name == "env":
            words = words[1:]
            if words and words[0] == "-S":
                words = words[1:]
        shell = Path(words[0]).name if words else ""
        if shell in SHELLS:
            interpreter = words[0]
            if Path(interpreter).is_absolute() and Path(interpreter).parent not in {
                Path("/bin"),
                Path("/usr/bin"),
                Path("/usr/local/bin"),
                Path("/opt/homebrew/bin"),
            }:
                raise ValueError(f"{name}:1: the declared shell is outside supported system tool directories. Ask the owner before executing it.")
            selected[name] = interpreter
        elif extension:
            raise ValueError(f"{name}:1: unsupported declared shell {shell!r}. Ask the owner before changing its interpreter.")
    return selected


def call(program: str, arguments: list[str], *, environment: dict[str, str] | None = None, timeout: int = 180) -> int:
    sys.stdout.flush()
    result = run_command(program, arguments, cwd=ROOT, env=environment, timeout=timeout)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.returncode


def native(name: str) -> str:
    program = ROOT / ".tools/bin" / name
    pinned = json.loads((ROOT / "config/quality_tools.json").read_text())["tools"][name]["version"]
    flag = "-version" if name == "actionlint" else "--version"
    version = run_command(str(program), [flag], cwd=ROOT, timeout=30)
    if version.returncode or not re.search(rf"(?<![\d.]){re.escape(pinned)}(?![\d.])", version.stdout + version.stderr):
        raise ValueError(f"config/quality_tools.json:1: {name} must be version {pinned}. Run scripts/quality/install_tools.py.")
    return str(program)


def selected_suffix(suffixes: tuple[str, ...], *, tracked: bool = False) -> list[str]:
    selected = [name for name in paths(tracked) if name.lower().endswith(suffixes)]
    for name in selected:
        safe_source(name)
    return selected


def ruff() -> int:
    selected = selected_suffix((".py", ".pyi", ".pyw"))
    sys.stdout.write(f"Ruff: {len(selected)} nonignored Python sources\n")
    if not selected:
        return 0
    lint = call(sys.executable, ["-m", "ruff", "check", "--", *selected])
    formatting = call(sys.executable, ["-m", "ruff", "format", "--check", "--", *selected])
    return int(bool(lint or formatting))


def shell_syntax() -> int:
    selected = shell_sources()
    sys.stdout.write(f"Shell syntax: {len(selected)} nonignored shell sources\n")
    failed = False
    for name, interpreter in selected.items():
        arguments = [*(["--noprofile", "--norc"] if Path(interpreter).name == "bash" else []), "-n", "./" + name]
        failed |= bool(call(interpreter, arguments, environment={**os.environ, "BASH_ENV": "/dev/null", "ENV": "/dev/null"}))
    return int(failed)


SIZE_SKIP = re.compile(r"Length of file '([^']+)' is \d+ bytes which is over the limit")


def refuse_size_skips(output: str, aliases: dict[str, str] | None = None) -> int:
    """Fail when SQLFluff skipped an actively linted file for size; macros stay covered through their callers."""
    skipped = [(aliases or {}).get(name, name).removeprefix(str(ROOT) + "/").removeprefix("./") for name in SIZE_SKIP.findall(output)]
    blocked = [name for name in skipped if not name.startswith("dbt/macros/")]
    for name in blocked:
        sys.stderr.write(f"{name}:1: SQLFluff skipped this file for size, so it was not linted. Set large_file_skip_byte_limit = 0 in .sqlfluff.\n")
    return int(bool(blocked))


def sql() -> int:
    selected = selected_suffix((".sql",), tracked=True)
    profiles: dict[str, list[str]] = {"dbt": [], "duckdb": [], "postgres": []}
    excluded = []
    for name in selected:
        if name.startswith(("dbt/models/", "dbt/tests/", "dbt/macros/")):
            profiles["dbt"].append(name)
        elif name in EXCLUDED_SQL:
            excluded.append(name)
        elif name == "services/analytics/resources.sql":
            profiles["duckdb"].append(name)
        elif name == "services/lakehouse/polaris/create-polaris-schema.sql":
            profiles["postgres"].append(name)
        else:
            raise ValueError(f"{name}:1: SQL has no declared runtime and dialect. Ask the owner before assigning a profile.")
    sys.stdout.write(f"SQL: {len(selected) - len(excluded)} active sources, {len(excluded)} excluded sources\n")
    for name in excluded:
        sys.stdout.write(f"excluded {name}: {EXCLUDED_SQL[name]}\n")
    failed = bool(macro_usage(profiles["dbt"])) if profiles["dbt"] else False
    for profile, sources in profiles.items():
        if sources:
            if profile == "duckdb":
                failed |= bool(masked_cli_sql(sources))
                continue
            arguments = ["lint", "--config", str(ROOT / ".sqlfluff")]
            if profile != "dbt":
                arguments += ["--templater", "raw", "--dialect", profile]
            result = run_command(str(ROOT / ".tools/sqlfluff/bin/sqlfluff"), [*arguments, *["./" + name for name in sources]], cwd=ROOT, timeout=SQLFLUFF_LIMIT)
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr)
            failed |= bool(result.returncode) | bool(refuse_size_skips(result.stdout + result.stderr))
    return int(failed)


def macro_usage(sources: list[str]) -> int:
    inventory = {name: safe_source(name).read_text() for name in sources if name.startswith("dbt/macros/")}
    if not inventory:
        return 0
    project = load_yaml(safe_source("dbt/dbt_project.yml").read_text())
    if not isinstance(project, dict) or not isinstance(project.get("name"), str):
        raise ValueError("dbt/dbt_project.yml:1: macro inventory requires a project name")
    with tempfile.TemporaryDirectory(prefix="hai_macro_usage_") as temporary:
        result = run_command(
            str(ROOT / ".tools/sqlfluff/bin/dbt"),
            [
                "parse",
                "--project-dir",
                str(ROOT / "dbt"),
                "--profiles-dir",
                str(ROOT / "dbt"),
                "--profile",
                "hai",
                "--target",
                "lint",
                "--target-path",
                str(Path(temporary) / "target"),
                "--log-path",
                str(Path(temporary) / "log"),
                "--no-partial-parse",
                "--no-send-anonymous-usage-stats",
                "--no-version-check",
                "--no-use-colors",
            ],
            cwd=ROOT,
            timeout=180,
        )
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        if result.returncode:
            return result.returncode
        manifest = json.loads((Path(temporary) / "target/manifest.json").read_text())
    if not isinstance(manifest, dict) or not isinstance(manifest.get("macros"), dict) or not isinstance(manifest.get("nodes"), dict):
        raise ValueError("dbt/macros:1: fresh manifest has no readable macro/node inventory")
    macros = manifest["macros"]
    own = {key: row for key, row in macros.items() if row["package_name"] == project["name"] and row["original_file_path"].startswith("macros/")}
    if {"dbt/" + row["original_file_path"] for row in own.values()} - inventory.keys():
        raise ValueError("dbt/macros:1: manifest contains project macro files outside the tracked readable inventory")
    declared: dict[tuple[str, str], int] = {}
    for path, source in inventory.items():
        unquoted = re.sub(r"{#.*?#}", lambda match: "\n" * match.group(0).count("\n"), source, flags=re.DOTALL)
        for match in re.finditer(r"{%[-+]?\s*(macro|test)\s+(\w+)\s*\(", unquoted):
            name = ("test_" if match.group(1) == "test" else "") + match.group(2)
            declared[path, name] = unquoted[: match.start()].count("\n") + 1
    discovered = {("dbt/" + row["original_file_path"], row["name"]) for row in own.values()}
    if declared.keys() != discovered:
        raise ValueError("dbt/macros:1: source definitions and fresh parsed macro inventory do not match")
    callers = [row for row in manifest["nodes"].values() if row["resource_type"] in {"model", "test", "operation"}]
    callers.extend(macros.values())
    called = {key for row in callers for key in row["depends_on"]["macros"]}
    if unknown := sorted(called - macros.keys()):
        raise ValueError(f"dbt/macros:1: manifest cannot resolve macro dependency {unknown[0]}")
    unused = sorted(own.keys() - called)
    sys.stdout.write(f"Macro usage: {len(own)} definitions in {len(inventory)} files. {len(own) - len(unused)} have callers recorded in the fresh manifest\n")
    for key in unused:
        name, path = own[key]["name"], "dbt/" + own[key]["original_file_path"]
        line = declared[path, name]
        sys.stderr.write(f"{path}:{line}: unused macro {name}. Call it through a recorded model, test, hook or macro, or review its retirement.\n")
    return int(bool(unused))


def masked_cli_sql(sources: list[str]) -> int:
    with tempfile.TemporaryDirectory(prefix="hai_cli_sql_") as temporary:
        aliases = {}
        for name in sources:
            path = Path(temporary) / Path(name).name
            masked = [
                "-- DuckDB CLI command" + ("\n" if line.endswith("\n") else "") if line.startswith(".") else line
                for line in safe_source(name).read_text().splitlines(keepends=True)
            ]
            path.write_text("".join(masked))
            aliases[str(path)] = name
        result = run_command(
            str(ROOT / ".tools/sqlfluff/bin/sqlfluff"),
            ["lint", "--config", str(ROOT / ".sqlfluff"), "--templater", "raw", "--dialect", "duckdb", *aliases],
            cwd=ROOT,
            timeout=900,
        )
        output, errors = result.stdout, result.stderr
        for temporary_name, name in aliases.items():
            output, errors = output.replace(temporary_name, name), errors.replace(temporary_name, name)
        sys.stdout.write(output)
        sys.stderr.write(errors)
        return result.returncode or refuse_size_skips(output + errors)


def go(check: str) -> int:
    selected = selected_suffix((".go",), tracked=True)
    sys.stdout.write(f"{check}: {len(selected)} tracked Go sources\n")
    if not selected:
        return 0
    program = find_program("go")
    if program is None:
        raise ValueError("Go:1: installed Go 1.27.3 is required. Ask the owner before installing a toolchain.")
    environment = {**os.environ, "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off"}
    version = run_command(program, ["version"], cwd=ROOT, env=environment, timeout=30)
    if not re.search(r"\bgo1\.27\.3\b", version.stdout) or version.returncode:
        raise ValueError("Go:1: the check requires installed Go 1.27.3. No toolchain is downloaded.")
    if check == "gofmt":
        result = run_command(str(Path(program).with_name("gofmt")), ["-l", *["./" + name for name in selected]], cwd=ROOT, env=environment, timeout=120)
        for name in result.stdout.splitlines():
            sys.stderr.write(f"{name}:1: gofmt formatting differs. Run gofmt -w on this file or ask the owner.\n")
        sys.stderr.write(result.stderr)
        return int(bool(result.returncode or result.stdout))
    failed = False
    for architecture in ("amd64", "arm64"):
        sys.stdout.write(f"go vet target: linux/{architecture}\n")
        target = {**environment, "GOOS": "linux", "GOARCH": architecture, "CGO_ENABLED": "0"}
        for name in selected:
            failed |= bool(call(program, ["vet", "./" + name], environment=target, timeout=180))
    return int(failed)


def dockerfiles() -> list[str]:
    selected = [name for name in paths(tracked=True) if Path(name).name == "Dockerfile" or Path(name).name.startswith("Dockerfile.")]
    for name in selected:
        safe_source(name)
    return selected


def compose_contexts() -> dict[str, list[tuple[str, set[str]]]]:
    project = load_yaml(safe_source("docker-compose.yaml").read_text())
    if not isinstance(project, dict) or not isinstance(project.get("services"), dict):
        raise ValueError("docker-compose.yaml:1: expected a services mapping")
    selected: dict[str, list[tuple[str, set[str]]]] = {}
    for service, settings in project["services"].items():
        if not isinstance(settings, dict) or "build" not in settings:
            continue
        build = settings["build"]
        if isinstance(build, str):
            build = {"context": build}
        if not isinstance(build, dict):
            raise ValueError(f"docker-compose.yaml:1: invalid build for {service}")
        context, filename = build.get("context", "."), build.get("dockerfile", "Dockerfile")
        if not isinstance(context, str) or not isinstance(filename, str):
            raise ValueError(f"docker-compose.yaml:1: build paths for {service} must be literal strings")
        name = (ROOT / context / filename).resolve().relative_to(ROOT).as_posix()
        additional = build.get("additional_contexts", {})
        if isinstance(additional, dict):
            declarations = [f"{key}={value}" for key, value in additional.items()]
        elif isinstance(additional, list) and all(isinstance(value, str) for value in additional):
            declarations = additional
        else:
            raise ValueError(f"docker-compose.yaml:1: invalid additional_contexts for {service}")
        contexts = set()
        for declaration in declarations:
            key, separator, value = declaration.partition("=")
            if not separator or not key or not value:
                raise ValueError(f"docker-compose.yaml:1: invalid context declaration for {service}")
            contexts.add(key)
        selected.setdefault(name, []).append((str(service), contexts))
    return selected


def image_contexts() -> dict[str, list[tuple[str, set[str]]]]:
    """The named build contexts that ``infra/services/images.json`` passes to each image it builds, keyed by Dockerfile."""
    document = json.loads(safe_source("infra/services/images.json").read_text())
    selected: dict[str, list[tuple[str, set[str]]]] = {}
    for key, image in document["images"].items():
        if image.get("kind") == "build" and image.get("build_contexts"):
            name = (ROOT / image["context"] / image.get("dockerfile", "Dockerfile")).resolve().relative_to(ROOT).as_posix()
            selected.setdefault(name, []).append((f"images.json image {key}", set(image["build_contexts"])))
    return selected


def header_contexts(name: str) -> set[str]:
    commands = []
    for line in safe_source(name).read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            break
        command = line.lstrip().removeprefix("#").strip()
        if not re.match(r"docker\s+build(?:\s|$)", command):
            continue
        words = shlex.split(command)
        if words[:2] == ["docker", "build"]:
            commands.append(words)
    if len(commands) != 1:
        raise ValueError(f"{name}:1: missing header build command or ambiguous build commands")
    contexts = set()
    for index, word in enumerate(commands[0]):
        if word == "--build-context":
            declaration = commands[0][index + 1] if index + 1 < len(commands[0]) else ""
        elif word.startswith("--build-context="):
            declaration = word.removeprefix("--build-context=")
        else:
            continue
        key, separator, value = declaration.partition("=")
        if not separator or not key or not value:
            raise ValueError(f"{name}:1: invalid header build-context declaration")
        contexts.add(key)
    if not contexts:
        raise ValueError(f"{name}:1: header build command declares no contexts")
    return contexts


def docker_bodies(name: str) -> list[tuple[int, str, str]]:
    """Every instruction of one Dockerfile as its first line number, upper-case operation and joined body."""
    selected = []
    pending, start = "", 0
    for number, line in enumerate(safe_source(name).read_text().splitlines(), 1):
        text = line.strip()
        if text.startswith("# escape=") and text != "# escape=\\":
            raise ValueError(f"{name}:{number}: build-context checking does not support this escape directive")
        if not text or text.startswith("#"):
            continue
        start = start or number
        if text.endswith("\\"):
            pending += text[:-1] + " "
            continue
        instruction = pending + text
        if "<<" in instruction:
            raise ValueError(f"{name}:{start}: build-context checking does not support Dockerfile heredocs")
        operation, *remainder = instruction.split(maxsplit=1)
        selected.append((start, operation.upper(), remainder[0] if remainder else ""))
        pending, start = "", 0
    if pending:
        raise ValueError(f"{name}:{start}: unterminated Dockerfile continuation")
    return selected


def docker_instructions(name: str) -> list[tuple[int, str, list[str]]]:
    return [(number, operation, shlex.split(body)) for number, operation, body in docker_bodies(name) if operation in {"FROM", "COPY"}]


def build_contexts() -> int:
    selected = dockerfiles()
    declared = compose_contexts()
    for name, extra in image_contexts().items():
        declared[name] = [*declared.get(name, []), *extra]
    sys.stdout.write(f"Build contexts: {len(selected)} tracked Dockerfiles\n")
    failed = False
    for name in selected:
        bindings = declared.get(name, [("no Compose build", set())])
        if name == "services/orchestration/Dockerfile":
            bindings = [("header build command", header_contexts(name))]
        earlier: set[str] = set()
        current_alias, stage = "", -1
        for number, operation, words in docker_instructions(name):
            if operation == "FROM":
                if current_alias:
                    earlier.add(current_alias.casefold())
                current_alias = words[-1] if len(words) >= 3 and words[-2].upper() == "AS" else ""
                stage += 1
                continue
            references = []
            for index, word in enumerate(words):
                if word.startswith("--from="):
                    references.append(word.removeprefix("--from="))
                elif word == "--from":
                    references.append(words[index + 1] if index + 1 < len(words) else "")
            for reference in references:
                for service, contexts in bindings:
                    if reference.casefold() in earlier or (reference.isdigit() and int(reference) < stage) or reference in contexts:
                        continue
                    sys.stderr.write(
                        f"{name}:{number}: unknown COPY --from={reference} for {service}. Use an earlier stage or a declared named build context.\n"
                    )
                    failed = True
    return int(failed)


OCTAL_MODE = re.compile(r"[0-7]{3,4}")
CONTROL_OPERATORS = {"&&", "||", ";", "|", "&"}
ENTERABLE = 0o005  # read and search permission for other users: COPY writes root-owned files and the image runs as another user
VARIABLE = re.compile(r"\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))")
ATTACHED_MODE = re.compile(r"^-[A-Za-z]*m(.+)$")
# Folders that every base image holds and that other users can enter; any other folder, a first-level one included, exists only when the Dockerfile makes it.
PRESENT = frozenset(
    {"/opt", "/usr", "/usr/local", "/usr/local/bin", "/usr/bin", "/usr/share", "/usr/lib", "/etc", "/var", "/var/lib", "/home", "/srv", "/mnt", "/run"}
)


class Unresolved(ValueError):
    """A path of the image that holds a variable which the Dockerfile never sets with ENV or ARG."""


def instruction_words(body: str) -> list[str]:
    """The words of an instruction in exec form (a JSON array) or shell form."""
    if body.lstrip().startswith("["):
        try:
            words = json.loads(body)
        except ValueError:
            words = None
        if isinstance(words, list):
            return [str(word) for word in words]
    return shlex.split(body)


def shell_commands(body: str) -> list[list[str]]:
    """The simple commands of one RUN body, split at the shell's control operators, without the RUN flags."""
    if body.lstrip().startswith("["):
        return [instruction_words(body)]
    lexer = shlex.shlex(body, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    commands: list[list[str]] = [[]]
    for token in lexer:
        if token in CONTROL_OPERATORS:
            commands.append([])
        else:
            commands[-1].append(token)
    while commands[0] and commands[0][0].startswith("--"):
        commands[0].pop(0)
    return [command for command in commands if command]


def expand(text: str, variables: dict[str, str]) -> str:
    """The text with each variable that ENV or ARG set replaced; any other variable stays in the text."""
    return VARIABLE.sub(lambda match: variables.get(match.group(1) or match.group(2), match.group(0)), text)


def folder_chain(folder: str) -> list[str]:
    """The folder and each parent that a COPY may create: every folder except the root and the folders that each base image holds."""
    parts = PurePosixPath(folder).parts
    return [created for created in ("/" + "/".join(parts[1:index]) for index in range(len(parts), 1, -1)) if created not in PRESENT]


def opens(mode: str) -> bool:
    """Whether an octal mode gives other users read and search permission on a folder."""
    return bool(OCTAL_MODE.fullmatch(mode)) and int(mode, 8) & ENTERABLE == ENTERABLE


def command_mode(words: list[str]) -> int | None:
    """The mode that mkdir or install names (``-m 0700``, ``--mode=0700`` or ``-m0700``): 0 when it is not octal, None when there is none."""
    for index, word in enumerate(words):
        attached = ATTACHED_MODE.match(word)
        value = (
            word.split("=", 1)[1]
            if word.startswith("--mode=")
            else words[index + 1]
            if word in {"-m", "--mode"} and index + 1 < len(words)
            else attached.group(1)
            if attached
            else ""
        )
        if value:
            return int(value, 8) if OCTAL_MODE.fullmatch(value) else 0
    return None


@dataclass
class Closed:
    """A folder that a COPY --chmod created closed to other users: where, with which mode, and whether its unknown contents may hold more closed folders."""

    number: int
    mode: str
    subtree: bool


class StageModes:
    """The folders of one build stage: those known to be enterable, and those that a COPY --chmod created without search permission."""

    def __init__(self, name: str, tracked: list[str], contexts: list[str]) -> None:
        self.name = name
        self.workdir: str | None = "/"
        self.variables: dict[str, str] = {}
        self.known: set[str] = set()
        self.pending: dict[str, Closed] = {}
        self.nonroot = False
        self.tracked, self.contexts = tracked, contexts

    def folder(self, path: str, number: int) -> str:
        """The absolute folder that a path of the image names, with the variables that ENV and ARG set."""
        text = expand(path, self.variables)
        if self.workdir is None and not text.startswith("/"):
            raise Unresolved(f"{self.name}:{number}: copy-mode checking cannot resolve {path}; the working folder is not known")
        joined = posixpath.normpath(posixpath.join(self.workdir or "/", text))
        if "$" in joined or "`" in joined:
            raise Unresolved(f"{self.name}:{number}: copy-mode checking cannot resolve {path}; write the folder in full or set the variable with ENV before")
        return joined

    def set_variables(self, operation: str, body: str) -> None:
        try:
            words = shlex.split(body)
        except ValueError:
            return
        if operation == "ENV" and words and "=" not in words[0]:
            self.variables[words[0]] = expand(" ".join(words[1:]), self.variables)
            return
        for word in words:
            key, separator, value = word.partition("=")
            if separator:
                self.variables[key] = expand(value, self.variables)

    def source_kind(self, source: str) -> tuple[str, set[str]]:
        """``file``, or ``dir`` with the folders below it, for a source that Git tracks in a build context; ``unknown`` for anything else."""
        if re.search(r"[*?\[]", source) or "$" in source or source.startswith("/"):
            return "unknown", set()
        for context in self.contexts:
            base = posixpath.normpath(posixpath.join(context, source))
            if base in self.tracked:
                return "file", set()
            prefix = "" if base == "." else base + "/"
            below = [name[len(prefix) :] for name in self.tracked if name.startswith(prefix)]
            if below:
                return "dir", {"/".join(PurePosixPath(name).parts[:depth]) for name in below for depth in range(1, len(PurePosixPath(name).parts))}
        return "unknown", set()

    def copy(self, number: int, words: list[str]) -> None:
        flags = [word for word in words if word.startswith("--")]
        paths = [word for index, word in enumerate(words) if not word.startswith("--") and not (index and words[index - 1] == "--from")]
        if len(paths) < 2:
            return
        mode = next((flag.split("=", 1)[1] for flag in flags if flag.startswith("--chmod=")), "")
        if mode and not OCTAL_MODE.fullmatch(mode):
            raise ValueError(f"{self.name}:{number}: copy-mode checking cannot read the mode {mode}; write it as an octal number")
        try:
            destination = self.folder(paths[-1], number)
        except Unresolved:
            if mode and not opens(mode):
                raise
            return
        sources = paths[:-1]
        kinds = [
            ("unknown", set()) if any(flag.startswith("--from") for flag in flags) or "--from" in words else self.source_kind(source) for source in sources
        ]
        # A source not proved to be a file may be a folder, so its destination is a folder; one file to a named destination makes only the folders above.
        made: dict[str, bool] = {}
        if paths[-1].endswith("/") or len(sources) > 1 or any(kind != "file" for kind, _ in kinds):
            unknown = any(kind == "unknown" for kind, _ in kinds)
            made.update({folder: False for folder in folder_chain(destination)})
            made[destination] = unknown
            for _, nested in kinds:
                made.update({posixpath.join(destination, name): False for name in nested})
        else:
            made.update({folder: False for folder in folder_chain(posixpath.dirname(destination))})
        if mode and not opens(mode):
            self.pending.update({folder: Closed(number, mode, subtree) for folder, subtree in made.items() if folder not in self.known})
        self.known.update(made)

    def operands(self, number: int, arguments: list[str]) -> list[str]:
        found = []
        for word in arguments:
            if not word.startswith("-") and not OCTAL_MODE.fullmatch(word):
                try:
                    found.append(self.folder(word, number))
                except Unresolved:
                    continue
        return found

    def run(self, number: int, body: str) -> None:
        for command in shell_commands(body):
            program, arguments = posixpath.basename(command[0]), command[1:]
            if program == "mkdir" or (program == "install" and {"-d", "--directory"} & set(arguments)):
                made_mode = command_mode(arguments)
                paths = self.operands(number, arguments)
                made = [folder for path in paths for folder in folder_chain(path)]
                self.known.update(made if made_mode is None or made_mode & ENTERABLE == ENTERABLE else [folder for folder in made if folder not in paths])
            elif program == "chmod":
                mode = next((word for word in arguments if OCTAL_MODE.fullmatch(word)), "")
                if opens(mode):
                    self.cover(self.operands(number, arguments), any(word in {"-R", "--recursive"} for word in arguments))
            elif program == "find":
                self.find(number, arguments)

    def find(self, number: int, arguments: list[str]) -> None:
        """Only ``find ROOT -type d -exec chmod MODE {} +`` with no other test covers every folder below each ROOT; a filter leaves folders closed."""
        start = next((index for index, word in enumerate(arguments) if word.startswith("-") or word in {"(", "!"}), len(arguments))
        test = arguments[start:]
        if test[:4] != ["-type", "d", "-exec", "chmod"] or len(test) < 6 or not opens(test[4]) or test[5:] not in (["{}", "+"], ["{}"]):
            return
        self.cover(self.operands(number, arguments[:start]), True)

    def cover(self, roots: list[str], recursive: bool) -> None:
        for folder, closed in list(self.pending.items()):
            if (folder in roots and (recursive or not closed.subtree)) or (recursive and any(folder.startswith(root.rstrip("/") + "/") for root in roots)):
                del self.pending[folder]

    def report(self) -> bool:
        for folder, closed in sorted(self.pending.items(), key=lambda item: item[1].number):
            sys.stderr.write(
                f"{self.name}:{closed.number}: COPY --chmod={closed.mode} gives the folder {folder} that it creates the same mode, "
                "so the image user cannot enter it. "
                f"Make the folder first with RUN install -d -m 0755 {folder} (when the source is one file), "
                "or give every folder a mode with search permission before USER, "
                "with exactly RUN find /app -type d -exec chmod 0555 {} + (a find with another test is not accepted).\n"
            )
        failed = bool(self.pending)
        self.pending = {}
        return failed

    def finish(self) -> bool:
        """Report the closed folders at the end of a stage, but only when the stage ends as another user than root."""
        if self.nonroot:
            return self.report()
        self.pending = {}
        return False


def build_folders() -> dict[str, list[str]]:
    """The build context folder of each Dockerfile that Compose or the image list builds; a file that is absent or malformed names none."""
    found: dict[str, list[str]] = {}

    def add(context: object, filename: object) -> None:
        if isinstance(context, str) and isinstance(filename, str):
            found.setdefault(posixpath.normpath(posixpath.join(context, filename)), []).append(posixpath.normpath(context))

    try:
        project: Any = load_yaml(safe_source("docker-compose.yaml").read_text())
        for settings in project["services"].values():
            build = settings.get("build") if isinstance(settings, dict) else None
            if isinstance(build, str):
                add(build, "Dockerfile")
            elif isinstance(build, dict):
                add(build.get("context", "."), build.get("dockerfile", "Dockerfile"))
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        pass
    try:
        for image in json.loads(safe_source("infra/services/images.json").read_text())["images"].values():
            if image.get("kind") == "build":
                add(image.get("context"), image.get("dockerfile", "Dockerfile"))
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        pass
    return found


def copy_modes(selected: list[str]) -> int:
    """Flag a COPY --chmod whose mode leaves a folder it creates closed to the image user, unless a folder step before USER opens it.

    Docker 29 gives each folder that COPY --chmod creates the mode of the file, so ``--chmod=0444`` made /app/scripts closed and the
    non-root model API and page tasks exited 1 (Nov 15 2026). The check assumes the worst for everything it cannot prove. A source that Git does
    not track in the build context may be a folder, so its destination and everything below it count as created closed; a folder source also creates its
    nested folders; a folder that no earlier instruction made (a first-level one included, apart from the folders of the base image) counts as created.
    Only an exact repair clears a folder: ``install -d`` or ``mkdir`` with an open mode (``-m``, ``-m0755`` or ``--mode=``) before the COPY, ``chmod`` with
    an open mode, or ``find ROOT -type d -exec chmod MODE {} +`` with no other test. A stage is checked only when it ends as a user other than root.
    """
    sys.stdout.write(f"Image folder modes: {len(selected)} tracked Dockerfiles\n")
    tracked, folders = set(paths(tracked=True)), build_folders()
    failed = False
    for name in selected:
        contexts = [*dict.fromkeys([*folders.get(name, []), posixpath.dirname(name) or "."])]
        stage = StageModes(name, sorted(tracked), contexts)
        for number, operation, body in docker_bodies(name):
            if operation == "FROM":
                failed |= stage.finish()
                stage = StageModes(name, sorted(tracked), contexts)
            elif operation in {"ENV", "ARG"}:
                stage.set_variables(operation, body)
            elif operation == "WORKDIR":
                try:
                    stage.workdir = stage.folder(body, number)
                    stage.known.update(folder_chain(stage.workdir))
                except Unresolved:
                    stage.workdir = None
            elif operation == "COPY":
                stage.copy(number, instruction_words(body))
            elif operation == "RUN":
                stage.run(number, body)
            elif operation == "USER":
                stage.nonroot = body.split(":")[0].strip() not in {"root", "0"}
                if stage.nonroot:
                    failed |= stage.report()
        failed |= stage.finish()
    return int(failed)


def yamllint_environment(root: Path) -> tuple[Path, str]:
    """The python that runs yamllint and its name: the one decision, for the lint command and for the pinned-tools check (``scripts.quality.check_tools``).

    A private ``.tools/yamllint`` environment is chosen whenever its python path exists, executable or not, so a file that cannot run is named and refused and
    never skipped; otherwise the project environment's ``.venv`` python. Each caller starts the program in its own module."""
    private = root / ".tools/yamllint/bin/python"
    return (private, ".tools/yamllint") if private.exists() else (root / ".venv/bin/python", ".venv")


def yamllint_version(returncode: int, output: str, root: Path) -> tuple[str | None, str]:
    """What ``python -m yamllint --version`` reported (None when it exited with an error) and the version that ``config/quality_tools.json`` pins.

    The lint command and the pinned-tools check (``scripts.quality.check_tools``) each run that command with the python of ``yamllint_environment`` and judge
    the result here, so both accept the same yamllint."""
    pinned = json.loads((root / "config/quality_tools.json").read_text(encoding="utf-8"))["tools"]["yamllint"]["version"]
    return (None if returncode else output.strip()), pinned


def lint(check: str) -> int:
    if check == "ruff":
        return ruff()
    if check == "shell-syntax":
        return shell_syntax()
    if check == "sql":
        return sql()
    if check in {"gofmt", "go-vet"}:
        return go(check)
    if check == "build-contexts":
        return build_contexts()
    if check == "image-modes":
        return copy_modes(dockerfiles())
    if check == "shellcheck":
        selected = list(shell_sources(tracked=True))
    elif check == "actionlint":
        selected = [name for name in selected_suffix((".yml", ".yaml"), tracked=True) if name.startswith(".github/workflows/")]
    elif check == "hadolint":
        selected = dockerfiles()
    else:
        selected = selected_suffix((".yml", ".yaml"), tracked=True)
        if (ROOT / ".yamllint").exists():
            safe_source(".yamllint")
            selected = sorted({*selected, ".yamllint"})
    sys.stdout.write(f"{check}: {len(selected)} tracked sources\n")
    if not selected:
        return 0
    if check == "yamllint":
        python, label = yamllint_environment(ROOT)
        program = str(python)
        try:
            version = run_command(program, ["-m", "yamllint", "--version"], cwd=ROOT, timeout=30)
            reported, pinned = yamllint_version(version.returncode, version.stdout, ROOT)
        except OSError:  # a python that cannot start (no execute permission, not found) reports no version, like one that exits with an error
            reported, pinned = None, yamllint_version(0, "", ROOT)[1]
        if reported != f"yamllint {pinned}":
            raise ValueError(f"pyproject.toml:1: yamllint {pinned} is required in the project environment, but {label} reports {reported or 'no version'}.")
        return call(program, ["-m", "yamllint", "--strict", "--config-file", str(ROOT / ".yamllint"), "--", *selected])
    program = native(check)
    arguments = ["--norc"] if check == "shellcheck" else ["-oneline"] if check == "actionlint" else []
    environment = {**os.environ, "PATH": str(ROOT / ".tools/bin") + os.pathsep + os.environ.get("PATH", "")}
    return call(program, [*arguments, *["./" + name for name in selected]], environment=environment)


def main() -> int:
    """Run the requested source gate and return failure for any rejected or uncheckable input."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("check", choices=(*CHECKS, "fast"))
    args = parser.parse_args()
    failed = False
    for check in CHECKS[:-1] if args.check == "fast" else (args.check,):
        for part in ("image-modes", check) if check == "hadolint" else (check,):
            try:
                failed |= bool(lint(part))
            except (OSError, ValueError, TimeoutExpired) as error:
                sys.stderr.write(f"{part}: {error}\n")
                failed = True
            except LaunchesNotStopped as error:  # the launcher recorded it, so the process still ends with its cleanup-failure status
                sys.stderr.write(f"{part}: the stop of a probe is not confirmed ({error}); rerun once `ps` runs\n")
                failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())

"""Run repository source checks from the same inventory in pre-commit and CI.

Run ``.venv/bin/python -m scripts.quality.lint_sources <check>``. Ruff and shell syntax cover every nonignored
first-party source. Native language tools check tracked sources. SQL profiles follow the executing engine.
No script body, SQL statement or container is executed by this module.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
import tempfile
from pathlib import Path

from scripts.process import TimeoutExpired, find_program, run_command
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
TINY_FIXTURE_REASON = "synthetic dbt project of the transform scheduler E2E, built and checked by scripts/orchestration/run_transform_e2e.py"
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
        sys.stderr.write(
            f"{name}:1: SQLFluff skipped this file for size, so it was not linted. Set large_file_skip_byte_limit = 0 in .sqlfluff (docs/quality_checks.md).\n"
        )
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
    sys.stdout.write(f"Macro usage: {len(own)} definitions in {len(inventory)} files; {len(own) - len(unused)} have callers recorded in the fresh manifest\n")
    for key in unused:
        name, path = own[key]["name"], "dbt/" + own[key]["original_file_path"]
        line = declared[path, name]
        sys.stderr.write(f"{path}:{line}: unused macro {name}; call it through a recorded model, test, hook or macro, or review its retirement.\n")
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


def docker_instructions(name: str) -> list[tuple[int, str, list[str]]]:
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
        body = remainder[0] if remainder else ""
        if operation.upper() in {"FROM", "COPY"}:
            selected.append((start, operation.upper(), shlex.split(body)))
        pending, start = "", 0
    if pending:
        raise ValueError(f"{name}:{start}: unterminated Dockerfile continuation")
    return selected


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
                        f"{name}:{number}: unknown COPY --from={reference} for {service}; use an earlier stage or a declared named build context.\n"
                    )
                    failed = True
    return int(failed)


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
        private = ROOT / ".tools/yamllint/bin/python"
        program = str(private) if private.exists() else sys.executable
        version = run_command(program, ["-m", "yamllint", "--version"], cwd=ROOT, timeout=30)
        pinned = json.loads((ROOT / "config/quality_tools.json").read_text())["tools"]["yamllint"]["version"]
        if version.returncode or version.stdout.strip() != f"yamllint {pinned}":
            raise ValueError(f"requirements.txt:1: yamllint {pinned} is required in the project environment.")
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
        try:
            failed |= bool(lint(check))
        except (OSError, ValueError, TimeoutExpired) as error:
            sys.stderr.write(f"{check}: {error}\n")
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())

"""Check first-party imports against declared container mounts without importing application code.

Run ``.venv/bin/python -m scripts.quality.container_mounts [--root <checkout>]`` in pre-commit and CI.
Compose volumes, launcher Mount calls, job tables and pipeline JobSpec declarations remain the mount and entry-point authority.
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import itertools
import re
import shlex
import sys
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.process import run_command
from scripts.quality.container_imports import Bind, Container, Entry, ImportGraph, dotted, locations, modules_at
from scripts.quality.repo_checks import load_yaml

LAUNCHER = "orchestration/launcher.py"
PIPELINE = "orchestration/pipeline_render.py"
TRAINING = "orchestration/training_render.py"
CODE_DIGEST = "orchestration/code_digest.py"
WORKSPACE = "/workspace"
RULE = "docs/quality_checks.md#container-imports"
PRIVATE = {".git", ".venv", ".tools", ".aws", ".codex", ".agents", "data", "plans", "node_modules"}
VARIABLE = re.compile(r"\$\{([^}:]+)(?::[^}]*)?\}")
PYTHON = re.compile(r"python(?:\d+(?:\.\d+)*)?$")
PYTHON_SOURCES = {
    "scripts": "imports",
    "orchestration": "imports",
    "dags": "entry",
    "services/lineage": "entry",
    "services/analytics": "entry",
    "feature_store": "entry",
}


def inventory(root: Path) -> set[str]:
    """List nonignored source paths without reading credentials, data or acquisition registries."""
    listed = run_command("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root, timeout=30, check=True).stdout
    result = set()
    for name in set(listed.split("\0")) - {""}:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"{name}:1: invalid repository source path")
        if path.parts[0] in PRIVATE or "secrets" in path.parts or path.name.startswith(".env") or name.startswith("config/acquisition/"):
            continue
        compose = (
            "compose" in path.name and path.suffix in {".yml", ".yaml"} and (len(path.parts) == 1 or path.parts[0] in {"orchestration", "services", "config"})
        )
        if path.suffix == ".py" or path.name == "Dockerfile" or compose:
            if any((root / PurePosixPath(*path.parts[:length])).is_symlink() for length in range(1, len(path.parts) + 1)):
                raise ValueError(f"{name}:1: source symlinks cannot establish container file locations")
            result.add(name)
    return result


def command_entries(words: list[str], path: str, line: int = 1, *, container: str = "") -> list[Entry]:
    location = f"{path}:{line}: " + (f"container {container}: " if container else "")
    for index, word in enumerate(words):
        if PYTHON.fullmatch(PurePosixPath(word).name):
            args = words[index + 1 :]
            if "-m" in args:
                selected = args.index("-m") + 1
                if selected == len(args) or not all(part.isidentifier() for part in args[selected].split(".")):
                    raise ValueError(f"{location}Python module entry point cannot be resolved")
                return [Entry(args[selected])]
            if "-c" in args:
                raise ValueError(f"{location}inline Python entry points need explicit static support")
            if scripts := [argument for argument in args if argument.endswith(".py")]:
                return [Entry(scripts[0], False)]
    if any("python" in word.lower() or word.endswith(".py") for word in words):
        raise ValueError(f"{location}Python entry point cannot be resolved statically")
    return []


def literal_words(value: object) -> list[str]:
    return shlex.split(value) if isinstance(value, str) else [str(item) for item in value] if isinstance(value, list) else []


class Declarations:
    """Read the repository's declarative Compose and Python launcher forms without evaluating them."""

    def __init__(self, root: Path, files: set[str]) -> None:
        self.root = root
        self.files = files
        self.python = {name for name in files if name.endswith(".py")}
        self.services: dict[tuple[str, str], dict[str, Any]] = {}
        self.composes = sorted(name for name in files if "compose" in PurePosixPath(name).name and name.endswith((".yaml", ".yml")))
        for name in self.composes:
            data = load_yaml((root / name).read_text(encoding="utf-8"))
            if not isinstance(data, dict) or not isinstance(data.get("services"), dict):
                raise ValueError(f"{name}:1: a Compose declaration needs a services mapping")
            for service, spec in data["services"].items():
                if not isinstance(spec, dict):
                    raise ValueError(f"{name}:1: service {service} needs a mapping")
                self.services[name, str(service)] = spec
        self.images: dict[str, tuple[Bind, ...]] = {}
        for spec in self.services.values():
            build = spec.get("build")
            context = build if isinstance(build, str) else build.get("context") if isinstance(build, dict) else None
            if context:
                self.images[str(spec["image"])] = tuple(self.image_binds(str(context)))
        self.trees = {name: ast.parse((root / name).read_text(encoding="utf-8"), filename=name) for name in (LAUNCHER, PIPELINE, TRAINING, CODE_DIGEST)}
        self.functions = {name: {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)} for name, tree in self.trees.items()}
        self.constants: dict[str, dict[str, Any]] = {}
        for name, tree in self.trees.items():
            values: dict[str, Any] = {}
            for node in tree.body:
                if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None:
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    try:
                        value = ast.literal_eval(node.value)
                    except (ValueError, TypeError):
                        continue
                    values.update({target.id: value for target in targets if isinstance(target, ast.Name)})
            self.constants[name] = values

    def image_binds(self, context: str) -> list[Bind]:
        folder = PurePosixPath(context.removeprefix("./"))
        dockerfile = str(folder / "Dockerfile")
        if dockerfile not in self.files:
            raise ValueError(f"{dockerfile}:1: declared build context has no inspected Dockerfile")
        text = (self.root / dockerfile).read_text(encoding="utf-8").replace("\\\n", " ")
        lines = text.splitlines()
        final = max(index for index, line in enumerate(lines) if line.upper().startswith("FROM "))
        result = []
        for line in lines[final + 1 :]:
            if not line.upper().startswith("COPY ") or "--from=" in line:
                continue
            words = shlex.split(line)[1:]
            words = [word for word in words if not word.startswith("--")]
            if not words or not words[-1].startswith("/"):
                raise ValueError(f"{dockerfile}:1: COPY needs a supported absolute destination")
            for value in words[:-1]:
                source = str(folder / value)
                target = words[-1]
                if target.endswith("/"):
                    target += PurePosixPath(value).name
                result.append(Bind(source, target))
        return result

    def compose_binds(self, name: str, service: str) -> tuple[Bind, ...]:
        spec = self.services[name, service]
        result = list(self.images.get(str(spec.get("image", "")), ()))
        for volume in spec.get("volumes", []):
            if isinstance(volume, str):
                expanded = VARIABLE.sub(lambda match: "/__checkout__" if match[1].endswith("_REPO") else "/__external__", volume)
                parts = expanded.split(":")
                if len(parts) not in (2, 3):
                    raise ValueError(f"{name}:1: container {service}: volume syntax cannot be checked")
                source, target = parts[:2]
            elif isinstance(volume, dict) and isinstance(volume.get("source"), str) and isinstance(volume.get("target"), str):
                source, target = volume["source"], volume["target"]
                source = VARIABLE.sub(lambda match: "/__checkout__" if match[1].endswith("_REPO") else "/__external__", source)
            else:
                raise ValueError(f"{name}:1: container {service}: volume declaration cannot be checked")
            relative = source.removeprefix("/__checkout__/") if source.startswith("/__checkout__/") else source.removeprefix("./")
            repository = not relative.startswith("/") and ".." not in PurePosixPath(relative).parts
            result.append(Bind(relative if repository else None, target))
        return tuple(result)

    def strings(self, expr: ast.AST, path: str, function: ast.FunctionDef, values: dict[str, set[str]], seen: frozenset[str] = frozenset()) -> set[str]:
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            return {expr.value}
        if isinstance(expr, ast.Name) and expr.id not in seen:
            if expr.id in values:
                return values[expr.id]
            if isinstance(value := self.constants[path].get(expr.id), str):
                return {value}
            assignments = [
                node.value
                for node in ast.walk(function)
                if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == expr.id for target in node.targets)
            ]
            return set().union(*(self.strings(value, path, function, values, seen | {expr.id}) for value in assignments))
        if isinstance(expr, ast.JoinedStr):
            parts = [self.strings(part.value if isinstance(part, ast.FormattedValue) else part, path, function, values, seen) for part in expr.values]
            return {"".join(group) for group in itertools.product(*parts)}
        if isinstance(expr, ast.Subscript | ast.Attribute) and (key := ast.unparse(expr)) in values:
            return values[key]
        return set()

    def repository_path(self, expr: ast.AST, path: str, function: ast.FunctionDef, seen: frozenset[str] = frozenset()) -> str | None:
        if dotted(expr) in {"self.checkout", "checkout"}:
            return ""
        if isinstance(expr, ast.Call) and expr.args and dotted(expr.func) in {"str", "Path"}:
            return self.repository_path(expr.args[0], path, function, seen)
        if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute) and expr.func.attr in {"resolve", "absolute"}:
            return self.repository_path(expr.func.value, path, function, seen)
        if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Div):
            parent = self.repository_path(expr.left, path, function, seen)
            child = self.strings(expr.right, path, function, {})
            if parent is not None and len(child) == 1:
                return str(PurePosixPath(parent) / next(iter(child)))
        if isinstance(expr, ast.Name) and expr.id not in seen:
            candidates = [
                node.value
                for node in ast.walk(function)
                if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == expr.id for target in node.targets)
            ]
            paths = {self.repository_path(candidate, path, function, seen | {expr.id}) for candidate in candidates}
            if len(paths) == 1:
                return paths.pop()
        return None

    def mounts(self, path: str, function: ast.FunctionDef, seen: frozenset[str] = frozenset()) -> tuple[Bind, ...]:
        if function.name in seen:
            return ()
        result = []
        parents = {child: node for node in ast.walk(function) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(function):
            parts = (
                node.args
                if isinstance(node, ast.Call) and dotted(node.func) == "Mount"
                else node.elts
                if isinstance(node, ast.Tuple) and len(node.elts) == 3
                else []
            )
            if len(parts) >= 2:
                target = self.strings(parts[1], path, function, {})
                if len(target) == 1 and next(iter(target)).startswith("/"):
                    source = self.repository_path(parts[0], path, function)
                    bind = Bind(source, next(iter(target)))
                    if source and any(name == source or name.startswith(source + "/") for name in self.python):
                        parent = parents.get(node)
                        while parent is not None and parent is not function:
                            if isinstance(parent, ast.If | ast.IfExp | ast.For):
                                raise ValueError(f"{path}:{getattr(node, 'lineno', 1)}: conditional Python source mount needs a separate container invocation")
                            parent = parents.get(parent)
                    result.append(bind)
            if isinstance(node, ast.Call) and dotted(node.func) == "self.compose.mounts" and node.args:
                services = self.strings(node.args[0], path, function, {})
                for service in services:
                    matches = [name for name, candidate in self.services if candidate == service]
                    if "docker-compose.yaml" in matches:
                        selected = "docker-compose.yaml"
                    elif len(matches) == 1:
                        selected = matches[0]
                    else:
                        raise ValueError(f"{path}:{node.lineno}: Compose mounts for service {service!r} cannot be resolved")
                    result.extend(self.compose_binds(selected, service))
            if isinstance(node, ast.Call) and (called := dotted(node.func)).startswith("self."):
                method = called.removeprefix("self.")
                callee = self.functions[path].get(method)
                if callee is not None and (
                    method.startswith("render_")
                    or callee.returns is not None
                    and any(isinstance(value, ast.Name) and value.id == "Mount" for value in ast.walk(callee.returns))
                ):
                    result.extend(self.mounts(path, self.functions[path][method], seen | {function.name}))
        return tuple(dict.fromkeys(result))

    def entries(self, path: str, function: ast.FunctionDef, overrides: dict[str, set[str]], seen: frozenset[str] = frozenset()) -> list[Entry]:
        if function.name in seen:
            return []
        values = dict(overrides)
        defaults = zip(function.args.args[-len(function.args.defaults) :], function.args.defaults, strict=True) if function.args.defaults else []
        for argument, default in defaults:
            if argument.arg not in values:
                values[argument.arg] = self.strings(default, path, function, values)
        result: list[Entry] = []
        for node in ast.walk(function):
            if isinstance(node, ast.List | ast.Tuple) and node.elts:
                first = self.strings(node.elts[0], path, function, values)
                if any(PYTHON.fullmatch(PurePosixPath(word).name) for word in first) and len(node.elts) > 1:
                    flag = self.strings(node.elts[1], path, function, values)
                    target = node.elts[2] if flag == {"-m"} and len(node.elts) > 2 else node.elts[1]
                    modules = self.strings(target, path, function, values)
                    if function.name == "render_pipeline" and dotted(target) == "spec.module":
                        continue
                    if not modules:
                        raise ValueError(f"{path}:{node.lineno}: Python entry point cannot be resolved statically")
                    result.extend(Entry(module, flag == {"-m"}) for module in sorted(modules))
            if isinstance(node, ast.Call) and dotted(node.func) == "JobSpec":
                modules = self.strings(node.args[1], path, function, values)
                if not modules:
                    raise ValueError(f"{path}:{node.lineno}: JobSpec module cannot be resolved statically")
                result.extend(Entry(module) for module in sorted(modules))
            if isinstance(node, ast.Call) and (called := dotted(node.func)).startswith("self.render_"):
                method = called.removeprefix("self.")
                if method not in self.functions[path]:
                    raise ValueError(f"{path}:{node.lineno}: renderer {method} cannot be resolved")
                callee = self.functions[path][method]
                arguments = {
                    argument.arg: self.strings(value, path, function, values) for argument, value in zip(callee.args.args[1:], node.args, strict=False)
                }
                arguments.update({word.arg: self.strings(word.value, path, function, values) for word in node.keywords if word.arg})
                result.extend(self.entries(path, callee, values | {name: value for name, value in arguments.items() if value}, seen | {function.name}))
        return list(dict.fromkeys(result))

    def module_choices(self, expr: ast.AST) -> set[str]:
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            return {expr.value}
        if isinstance(expr, ast.IfExp):
            return self.module_choices(expr.body) | self.module_choices(expr.orelse)
        return set()

    def training(self) -> tuple[tuple[Bind, ...], set[str]]:
        """Derive the training snapshot binds, action mounts and entry modules from the digest roots and the renderer's specification builder."""
        roots = self.constants[CODE_DIGEST].get("TRAINING_ROOTS")
        if not isinstance(roots, tuple) or not roots or not all(isinstance(root, str) for root in roots):
            raise ValueError(f"{CODE_DIGEST}:1: TRAINING_ROOTS must be a literal tuple of repository paths")
        build = self.functions[TRAINING].get("build")
        if build is None:
            raise ValueError(f"{TRAINING}:1: the training specification builder build cannot be resolved")
        binds: list[Bind] = []
        for root in roots:
            if "*" in root:
                if any(fnmatch.fnmatch(name, root) for name in self.python):
                    raise ValueError(f"{CODE_DIGEST}:1: a snapshot root pattern {root!r} selects Python sources")
                continue
            binds.append(Bind(root, f"{WORKSPACE}/{root}"))
        snapshot_rule = False
        modules: set[str] = set()
        for node in ast.walk(build):
            if isinstance(node, ast.Tuple) and len(node.elts) == 3 and isinstance(node.elts[2], ast.Constant) and isinstance(node.elts[2].value, bool):
                target = node.elts[1]
                prefix, dynamic = "", False
                if isinstance(target, ast.Constant) and isinstance(target.value, str):
                    prefix = target.value
                elif isinstance(target, ast.JoinedStr):
                    for part in target.values:
                        if not isinstance(part, ast.Constant):
                            dynamic = True
                            break
                        prefix += str(part.value)
                    if dynamic and prefix == f"{WORKSPACE}/" and len(target.values) == 2:
                        snapshot_rule = True
                        continue
                else:
                    continue
                if not prefix.startswith("/"):
                    raise ValueError(f"{TRAINING}:{node.lineno}: training mount target cannot be resolved statically")
                binds.append(Bind(None, prefix.rstrip("/") if dynamic else prefix))
            if isinstance(node, ast.Call) and dotted(node.func) == "JobSpec":
                choices = self.module_choices(node.args[1]) if len(node.args) > 1 else set()
                if not choices:
                    raise ValueError(f"{TRAINING}:{node.lineno}: JobSpec module cannot be resolved statically")
                modules |= choices
        if not snapshot_rule:
            raise ValueError(f"{TRAINING}:{build.lineno}: the {WORKSPACE} snapshot mount rule cannot be resolved")
        if not modules:
            raise ValueError(f"{TRAINING}:{build.lineno}: no training JobSpec module was found")
        return tuple(dict.fromkeys(binds)), modules

    def working_folder(self, path: str, function: ast.FunctionDef, seen: frozenset[str] = frozenset()) -> str:
        folders: set[str] = set()
        if function.name in seen:
            raise ValueError(f"{path}:{function.lineno}: working folder cannot be resolved through recursive renderers")
        for node in ast.walk(function):
            if not isinstance(node, ast.Call):
                continue
            called = dotted(node.func)
            if called == "self.assemble":
                value = next((word.value for word in node.keywords if word.arg == "workdir"), None)
                selected = self.strings(value, path, function, {}) if value is not None else set()
                if len(selected) != 1 or not next(iter(selected)).startswith("/") or "$" in next(iter(selected)):
                    raise ValueError(f"{path}:{node.lineno}: working folder cannot be resolved from assemble(workdir=...)")
                folders.update(selected)
            elif called.startswith("self.render_") and (callee := self.functions[path].get(called.removeprefix("self."))) is not None:
                folders.add(self.working_folder(path, callee, seen | {function.name}))
        if len(folders) != 1:
            raise ValueError(f"{path}:{function.lineno}: working folder cannot be resolved for renderer {function.name}")
        return folders.pop()

    def classify_mounts(self, binds: tuple[Bind, ...], path: str, line: int, container: str) -> None:
        for bind in binds:
            if bind.source is None:
                continue
            contains_python = any(not bind.source or name == bind.source or name.startswith(bind.source + "/") for name in self.python)
            if contains_python and not any(bind.source == prefix or bind.source.startswith(prefix + "/") for prefix in PYTHON_SOURCES):
                raise ValueError(f"{path}:{line}: container {container}: unclassified Python mount {bind.source!r} at {bind.target!r}")

    def containers(self) -> list[Container]:
        """Derive container invocations from Compose, launcher tables and renderer declarations."""
        result: list[Container] = []
        for (name, service), spec in self.services.items():
            binds = self.compose_binds(name, service)
            environment = spec.get("environment") or {}
            if not isinstance(environment, dict):
                raise ValueError(f"{name}:1: container {service}: environment needs a mapping")
            label = f"{name}:{service}"
            self.classify_mounts(binds, name, 1, label)
            working = spec.get("working_dir")
            if working is not None and (not isinstance(working, str) or not working.startswith("/") or "$" in working):
                raise ValueError(f"{name}:1: container {label}: working folder cannot be resolved")
            pythonpath = str(environment.get("PYTHONPATH", ""))
            python_roots = tuple(part for part in pythonpath.split(":") if part)
            roots = tuple(dict.fromkeys([*([working] if working else []), *python_roots]))
            if any(not root.startswith("/") or "$" in root for root in roots):
                raise ValueError(f"{name}:1: container {label}: working folder cannot be resolved for PYTHONPATH")
            entries = command_entries(literal_words(spec.get("entrypoint")) + literal_words(spec.get("command")), name, container=label)
            entries.extend(Entry(bind.target, False) for bind in self.images.get(str(spec.get("image", "")), ()) if bind.source in self.python)
            for bind in binds:
                if bind.source == "dags":
                    roots += (bind.target,)
            mapped = modules_at(locations(self.python, binds), roots)
            for module, source in mapped.items():
                if source and any(role == "entry" and source.startswith(prefix + "/") for prefix, role in PYTHON_SOURCES.items()):
                    entries.append(Entry(module))
            if any(entry.module for entry in entries) and not roots:
                raise ValueError(f"{name}:1: container {label}: working folder cannot be resolved and no PYTHONPATH is declared")
            result.extend(Container(label, binds, roots if entry.module else python_roots, entry, name, workdir=working) for entry in dict.fromkeys(entries))
        for table, renderer, label in (("SPARK_JOBS", "render_spark", "spark"), ("GOLD_JOBS", "render_gold_publish", "spark")):
            jobs = self.constants[LAUNCHER].get(table)
            if not isinstance(jobs, dict) or not jobs:
                raise ValueError(f"{LAUNCHER}:1: {table} must declare its job modules")
            function = self.functions[LAUNCHER][renderer]
            for job, spec in jobs.items():
                if not isinstance(spec, dict) or not isinstance(spec.get("module"), str):
                    raise ValueError(f"{LAUNCHER}:1: {table} job {job} needs a literal module")
                entries = self.entries(LAUNCHER, function, {"spec['module']": {spec["module"]}})
                binds = self.mounts(LAUNCHER, function)
                workdir = self.working_folder(LAUNCHER, function)
                self.classify_mounts(binds, LAUNCHER, function.lineno, f"launcher:{label}/{job}")
                result.extend(
                    Container(f"launcher:{label}/{job}", binds, (workdir,) if entry.module else (), entry, LAUNCHER, function.lineno, workdir)
                    for entry in entries
                )
        all_job_modules = {spec["module"] for table in ("SPARK_JOBS", "GOLD_JOBS") for spec in self.constants[LAUNCHER][table].values()}
        for method, function in self.functions[LAUNCHER].items():
            if not method.startswith("render_") or method in {
                "render_spark",
                "render_spark_read",
                "render_spark_write",
                "render_spark_e2e",
                "render_gold_publish",
            }:
                continue
            overrides = {"spec['module']": all_job_modules}
            binds = self.mounts(LAUNCHER, function)
            if method == "render_training":
                training_binds, training_modules = self.training()
                overrides["spec.module"] = training_modules
                binds = tuple(dict.fromkeys([*training_binds, *binds]))
            entries = self.entries(LAUNCHER, function, overrides)
            label = f"launcher:{method.removeprefix('render_')}"
            self.classify_mounts(binds, LAUNCHER, function.lineno, label)
            if entries:
                workdir = self.working_folder(LAUNCHER, function)
                result.extend(Container(label, binds, (workdir,) if entry.module else (), entry, LAUNCHER, function.lineno, workdir) for entry in entries)
        wrapper = self.functions[LAUNCHER]["sealed_reader"]
        for bind in self.mounts(LAUNCHER, wrapper):
            if bind.source and bind.source.endswith(".py"):
                result.append(Container("launcher:sealed_reader", (bind,), (), Entry(bind.target, False), LAUNCHER, wrapper.lineno))
        base = self.mounts(PIPELINE, self.functions[PIPELINE]["build"])
        pipeline_workdir = self.working_folder(LAUNCHER, self.functions[LAUNCHER]["render_pipeline"])
        for method, function in self.functions[PIPELINE].items():
            entries = self.entries(PIPELINE, function, {"layer": {"silver", "gold"}})
            if entries:
                binds = tuple(dict.fromkeys([*base, *self.mounts(PIPELINE, function)]))
                self.classify_mounts(binds, PIPELINE, function.lineno, f"launcher:pipeline/{method}")
                result.extend(
                    Container(f"launcher:pipeline/{method}", binds, (pipeline_workdir,), entry, PIPELINE, function.lineno, pipeline_workdir)
                    for entry in entries
                )
        if not result:
            raise ValueError("docker-compose.yaml:1: no first-party Python container entry points were found")
        return result


def check(root: Path) -> tuple[list[str], int]:
    """Return import findings and the number of container invocations inspected."""
    files = inventory(root)
    declarations = Declarations(root, files)
    containers = declarations.containers()
    first_party = {name.split(".")[0] for container in containers for name in modules_at(locations(declarations.python, container.binds), container.roots)}
    grouped: dict[tuple[str, int, str], tuple[set[str], set[str], set[str]]] = {}
    trees: dict[str, ast.Module] = {}
    for container in containers:
        graph = ImportGraph(root, declarations.python, container, first_party, trees)
        for finding in graph.check():
            reasons, labels, entries = grouped.setdefault((finding.path, finding.line, finding.name), (set(), set(), set()))
            reasons.add(finding.reason)
            labels.add(f"container {container.name}")
            entries.add(container.entry.value)
    findings = [
        f"{path}:{line}: import {name!r} {', '.join(sorted(reasons))}. Containers: {', '.join(sorted(labels))}. Entry points: {', '.join(sorted(entries))}"
        for (path, line, name), (reasons, labels, entries) in sorted(grouped.items())
    ]
    return findings, len(containers)


def main(argv: list[str] | None = None) -> int:
    """Check every declared container entry point and report actionable source locations."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    try:
        findings, count = check(args.root)
    except (OSError, ValueError, KeyError, SyntaxError) as error:
        sys.stderr.write(f"{error}. Fix the declaration or ask the owner ({RULE}).\n")
        return 1
    for finding in findings:
        sys.stderr.write(f"{finding}\n")
    if findings:
        sys.stderr.write(f"Use a module available at this container's mounted paths or correct its declaration. Ask the owner if the rule is wrong ({RULE}).\n")
    else:
        sys.stdout.write(f"Container imports passed: {count} declared invocations.\n")
    return int(bool(findings))


if __name__ == "__main__":
    sys.exit(main())

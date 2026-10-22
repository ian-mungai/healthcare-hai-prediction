"""Launcher cases of the gold publish templates, against the fake Docker Engine.

Run ``.venv/bin/python -m scripts.lakehouse.run_gold_publish_e2e --mode launcher``. Each case uses the launcher E2E's own test registry, fake
daemon and evidence folders (scripts/lakehouse/run_launcher_e2e.py); nothing real is started, and no real secret exists: the compose file of a
test registry holds random sentinel values, which the cases look for in every record.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.lakehouse import run_launcher_e2e as base
from scripts.lakehouse.run_launcher_e2e import Env, expect_error, hold_forever, refused
from scripts.lakehouse.run_lock import Registry, transaction

GOLD_RUN = "20261014T010203Z-0a1b2c3d"
SEALED = b"sealed build fixture"
L1B_COMPLETION: dict[str, Any] = {
    "bronze_pins_sha256": "2222222222222222222222222222222222222222222222222222222222222222",
    "build_attempt": "at-0123456789ab",
    "build_generation": 1,
    "copy_method": "cp -c",
    "dbt_tree_sha256": "1111111111111111111111111111111111111111111111111111111111111111",
    "device": 16777234,
    "file": "sealed/65fcbe2a7349449a8d4a629354424d6b6c986b192336af59cf79e41d6f7329cd.duckdb",
    "git_revision": "3333333333333333333333333333333333333333",
    "inode": 410491409,
    "invocation_id": "invocation-sample",
    "run_id": "20261014T010203Z-0a1b2c3d",
    "sealed_utc": "2026-10-14T08:42:04.740474+00:00",
    "sha256": "65fcbe2a7349449a8d4a629354424d6b6c986b192336af59cf79e41d6f7329cd",
    "size": 46,
}
SEALED_SHA = str(L1B_COMPLETION["sha256"])
WRITER_NAMES = {"POLARIS_GOLD_WRITER_CLIENT_ID": "gold-writer-id", "POLARIS_GOLD_WRITER_CLIENT_SECRET": "pgw-"}
JOBS = ("publish_candidates", "reconcile_candidates", "catalog_backup", "commit_publish", "gold_maintenance")

GoldCase = Callable[[Env], dict[str, bool]]


def sealed_run(record: dict[str, Any], run_id: str = GOLD_RUN) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "build_state": "sealed",
        "build_binding": {"attempt": L1B_COMPLETION["build_attempt"], "generation": 1},
        "build_attempt": L1B_COMPLETION["build_attempt"],
        "generation": 1,
        "completion": record,
    }


def with_writer(env: Env) -> None:
    """Add the gold writer's sentinel credentials to the test registry's stand-in Secrets Manager before the launcher reads them."""
    for name, prefix in WRITER_NAMES.items():
        env.secret_values[name] = prefix + base.os.urandom(6).hex() if prefix.endswith("-") else prefix
        env.stand_in.values[launcher.COMPOSE_SECRETS[name]] = env.secret_values[name]


def sealed_file(env: Env, run_id: str, content: bytes = SEALED) -> Path:
    """The run's sealed copy under the test registry's runs root, named by the digest of the completion record (the bytes are a stand-in)."""
    sha = SEALED_SHA
    folder = env.temp / "runs" / run_id / "sealed"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{sha}.duckdb"
    path.write_bytes(content)
    path.chmod(0o444)
    return path


def publish_run(env: Env, run_id: str, mode: str, prefixes: list[str], sealed: bool = True, accepted: bool = False) -> dict[str, Any]:
    """A held transform run in a publish mode that declares its gold prefix, with the sealed-build record L1b writes in a real run.

    With ``accepted`` the run also has a consumer result in its publish folder and the launcher's acceptance record for it (the fixture of the cases that
    are about something else; ``gold_consumer_acceptance`` goes through the accept method itself).
    """
    principal = env.acquire_run(run_id, "transform", "publish_fixture", write_prefixes=prefixes)
    digest = write_result(env, consumer_result(), run_id) if accepted else ""
    with transaction(Registry(env.registry, "test")) as state:
        state["runs"][run_id]["mode"] = mode
        if sealed:
            state["runs"][run_id].update(sealed_run({**L1B_COMPLETION, "run_id": run_id}, run_id))
        if accepted:
            state["runs"][run_id]["consumer_acceptance"] = {
                "attempt_id": "at-fixture",
                "result_sha256": digest,
                "manifest_sha256": "b" * 64,
                "sealed_sha256": SEALED_SHA,
            }
    if sealed:
        sealed_file(env, run_id)
    return principal


def render_one(
    env: Env, run_id: str, template: str, params: dict[str, Any], args: list[str], mode: str, with_completion: bool = True, accepted: str = ""
) -> dict[str, Any]:
    """Render a gold template for a run folder in a test registry; returns the facts the cases compare."""
    compose = launcher.ComposeModel(base.ROOT / "docker-compose.yaml")
    renderer = launcher.Renderer(env.config, compose, base.ROOT, base.ROOT, b"k" * 32)
    registry_id = json.loads((env.registry / "registry_id").read_text())["registry_id"]
    run_folder = env.temp / "runs" / run_id
    completion = launcher.completion_of(sealed_run({**L1B_COMPLETION, "run_id": run_id}, run_id)) if with_completion else None
    inp = launcher.RenderInput(
        template,
        "owner",
        "g1",
        1,
        1,
        "op-1",
        dict(launcher.Launcher.PLACEHOLDER_ENV),
        int(time.time()) + 600,
        args,
        params,
        {"hai.registry_id": registry_id},
        None,
        run_folder,
        {},
        (),
        completion,
        mode,
        accepted,
    )
    return base.facts(renderer.render(inp, registry_id))


def wrapped(tail: list[str]) -> list[str]:
    return [launcher.DEADLINE_ENTRYPOINT, *tail]


def gold_publish_templates(env: Env) -> dict[str, bool]:
    """Each gold job's effective specification: the principal, mounts, network, entrypoint and the arguments the launcher builds itself."""
    with_writer(env)
    env.start()
    checks: dict[str, bool] = {}
    home = str(launcher.account_home())
    registry_id = json.loads((env.registry / "registry_id").read_text())["registry_id"]
    network = f"hai-{registry_id[:8]}-net"
    runs = env.temp / "runs" / GOLD_RUN
    sealed_file(env, GOLD_RUN)
    accepted = write_result(env, consumer_result())
    secrets = env.secret_values
    root = base.ROOT
    contract = (str(root / launcher.GOLD_CONTRACT), f"/workspace/{launcher.GOLD_CONTRACT}", True)
    common = {
        (str(root / "scripts"), "/workspace/scripts", True),
        (str(root / "config"), "/workspace/config", True),
        (f"{home}/.aws", "/aws", True),
        contract,
        (str((runs / "publish").resolve()), "/workspace/out/publish", False),
    }
    for mode in ("publish_fixture", "real"):
        for job in JOBS:
            sealed_job = job != "gold_maintenance"
            f = render_one(env, GOLD_RUN, "gold_publish", {"job": job}, [], mode, with_completion=sealed_job, accepted=accepted if sealed_job else "")
            reader = job == "reconcile_candidates"
            step = {"publish_candidates": "candidates", "reconcile_candidates": "reconcile", "catalog_backup": "backup", "commit_publish": "commit"}.get(job)
            module = "scripts.lakehouse.gold_maintenance" if job == "gold_maintenance" else "scripts.lakehouse.gold_publish"
            expected_cmd = (
                ["--run", GOLD_RUN, "--mode", mode, "--protected", "/workspace/out/publish/protected.json"]
                if step is None
                else [step, "--run", GOLD_RUN, "--mode", mode, "--sealed-sha256", SEALED_SHA, "--accepted-result-sha256", accepted]
            )
            principal_id = secrets["POLARIS_READER_CLIENT_ID" if reader else "POLARIS_GOLD_WRITER_CLIENT_ID"]
            principal_secret = secrets["POLARIS_READER_CLIENT_SECRET" if reader else "POLARIS_GOLD_WRITER_CLIENT_SECRET"]
            other_secrets = [value for name, value in secrets.items() if name.endswith(("SECRET", "PASSWORD")) and value not in (principal_secret,)]
            extra = (
                {(str(root / "infra/deployment.auto.tfvars.json"), "/workspace/infra/deployment.auto.tfvars.json", True)} if job == "catalog_backup" else set()
            )
            checks[f"{job} ({mode}): the principal, the arguments, the mounts and the network"] = (
                f["env"]["POLARIS_CLIENT_ID"] == principal_id
                and f["env"]["POLARIS_CLIENT_SECRET"] == principal_secret
                and not any(value in other_secrets for value in f["env"].values())
                and not any(name.startswith(("POLARIS_GOLD_WRITER", "POLARIS_READER", "POLARIS_ROOT")) for name in f["env"])
                and f["cmd"] == expected_cmd
                and f["entrypoint"] == wrapped(["python", "-m", module])
                and f["network"] == network
                and f["mounts"] == common | extra
                and (str(root), "/workspace", False) not in f["mounts"]
            )
    checks["the writer's credential is never present in the reconcile job"] = secrets["POLARIS_GOLD_WRITER_CLIENT_SECRET"] not in json.dumps(
        render_one(env, GOLD_RUN, "gold_publish", {"job": "reconcile_candidates"}, [], "publish_fixture", accepted=accepted)["body"]
    )
    refusals = {
        "an argument from the client is refused": lambda: render_one(env, GOLD_RUN, "gold_publish", {"job": "commit_publish"}, ["--mode", "real"], "real"),
        "an unknown job is refused": lambda: render_one(env, GOLD_RUN, "gold_publish", {"job": "bronze"}, [], "real"),
        "a run in fixture mode is refused": lambda: render_one(env, GOLD_RUN, "gold_publish", {"job": "commit_publish"}, [], "fixture"),
        "a sealed job without the sealed-build record is refused": lambda: render_one(
            env, GOLD_RUN, "gold_publish", {"job": "commit_publish"}, [], "real", with_completion=False
        ),
        "an extra parameter is refused": lambda: render_one(env, GOLD_RUN, "gold_publish", {"job": "commit_publish", "image": "x"}, [], "real"),
        "a sealed job without an accepted consumer result is refused": lambda: render_one(env, GOLD_RUN, "gold_publish", {"job": "commit_publish"}, [], "real"),
        "a sealed job whose accepted digest is not the result's is refused": lambda: render_one(
            env, GOLD_RUN, "gold_publish", {"job": "commit_publish"}, [], "real", accepted="e" * 64
        ),
    }
    for label, action in refusals.items():
        checks[label] = isinstance(expect_error_of(action), launcher.LauncherError)

    f = render_one(env, GOLD_RUN, "gold_export", {}, [], "publish_fixture")
    sealed = env.temp / "runs" / GOLD_RUN / "sealed" / f"{SEALED_SHA}.duckdb"
    checks["gold_export: one read-only sealed file, a writable publish folder, network none and no credential"] = (
        f["network"] == "none"
        and f["image"] == launcher.ComposeModel(root / "docker-compose.yaml").image("quality")
        and f["entrypoint"] == wrapped(["python", "-m", "scripts.lakehouse.gold_export"])
        and f["cmd"] == ["--run", GOLD_RUN, "--sealed-sha256", SEALED_SHA]
        and f["mounts"]
        == {
            (str(root / "scripts"), "/workspace/scripts", True),
            contract,
            (str(sealed.resolve()), "/workspace/sealed/sealed.duckdb", True),
            (str((runs / "publish").resolve()), "/workspace/out/publish", False),
        }
        and not any(name.startswith(("AWS", "POLARIS")) for name in f["env"])
    )
    checks["gold_export: a parameter on a run owner is refused"] = isinstance(
        expect_error_of(lambda: render_one(env, GOLD_RUN, "gold_export", {"sealed_host": str(sealed)}, [], "publish_fixture")), launcher.LauncherError
    )
    link = sealed.with_name("c" * 64 + ".duckdb")
    link.symlink_to(sealed)
    record = {"sha256": "c" * 64, "file": f"sealed/{link.name}"}
    checks["gold_export: a sealed path that is a link is refused"] = isinstance(
        expect_error_of(lambda: render_with_record(env, record)), launcher.LauncherError
    )
    return checks


def render_with_record(env: Env, record: dict[str, Any]) -> dict[str, Any]:
    """A gold_export render with a hand-made sealed-build record."""
    compose = launcher.ComposeModel(base.ROOT / "docker-compose.yaml")
    renderer = launcher.Renderer(env.config, compose, base.ROOT, base.ROOT, b"k" * 32)
    registry_id = json.loads((env.registry / "registry_id").read_text())["registry_id"]
    inp = launcher.RenderInput(
        "gold_export",
        "owner",
        "g1",
        1,
        1,
        "op-1",
        dict(launcher.Launcher.PLACEHOLDER_ENV),
        int(time.time()) + 600,
        [],
        {},
        {"hai.registry_id": registry_id},
        None,
        env.temp / "runs" / GOLD_RUN,
        {},
        (),
        record,
        "publish_fixture",
    )
    return base.facts(renderer.render(inp, registry_id))


def expect_error_of(function: Callable[[], Any]) -> BaseException | None:
    try:
        function()
    except launcher.LauncherError as error:
        return error
    return None


def gold_export_offline_params(env: Env) -> dict[str, bool]:
    """An allocation (the offline checks) names the file, folder, run and digest itself; a folder outside the data folder is refused."""
    env.start()
    compose = launcher.ComposeModel(base.ROOT / "docker-compose.yaml")
    renderer = launcher.Renderer(env.config, compose, base.ROOT, base.ROOT, b"k" * 32)
    registry_id = json.loads((env.registry / "registry_id").read_text())["registry_id"]
    case_folder, _ = env.case_folders("export")
    sealed = case_folder / "sealed.duckdb"
    sealed.write_bytes(SEALED)
    publish = case_folder / "publish"
    publish.mkdir(exist_ok=True)

    def render(params: dict[str, Any]) -> dict[str, Any]:
        inp = launcher.RenderInput(
            "gold_export",
            "owner",
            "g1",
            1,
            1,
            "op-1",
            dict(launcher.Launcher.PLACEHOLDER_ENV),
            int(time.time()) + 600,
            [],
            params,
            {"hai.registry_id": registry_id},
        )
        return base.facts(renderer.render(inp, registry_id))

    good = {"sealed_host": str(sealed), "publish_host": str(publish), "run": GOLD_RUN, "sealed_sha256": SEALED_SHA}
    f = render(good)
    outside = env.temp
    return {
        "the offline export mounts the named file read-only and the folder writable": f["mounts"]
        >= {(str(sealed.resolve()), "/workspace/sealed/sealed.duckdb", True), (str(publish.resolve()), "/workspace/out/publish", False)}
        and f["network"] == "none",
        "a sealed file outside the data folder is refused": isinstance(
            expect_error_of(lambda: render({**good, "sealed_host": str(outside / "x.duckdb")})), launcher.LauncherError
        ),
        "a bad digest is refused": isinstance(expect_error_of(lambda: render({**good, "sealed_sha256": "xyz"})), launcher.LauncherError),
        "a bad run ID is refused": isinstance(expect_error_of(lambda: render({**good, "run": "run1"})), launcher.LauncherError),
        "a missing parameter is refused": isinstance(expect_error_of(lambda: render({"sealed_host": str(sealed)})), launcher.LauncherError),
    }


def gold_publish_admission(env: Env) -> dict[str, bool]:
    """The run predicates of the gold templates hold at submit: domain, mode, declared gold prefix and a sealed build."""
    with_writer(env)
    hold_forever(env)
    base.catalog_script(env)
    env.start()
    base.catalog_up(env)
    client = env.client()
    checks: dict[str, bool] = {}

    def submit(principal: dict[str, Any], template: str, launch: str, job: str | None = None, args: list[str] | None = None) -> dict[str, Any]:
        return env.submit(principal, template, launch, args or [], {"job": job} if job else {})

    def finish(run_id: str, principal: dict[str, Any]) -> None:
        for attempt_id, attempt in env.state()["attempts"].items():
            if attempt["owner"] == f"run:{run_id}" and attempt["phase"] != "ended":
                client.call("cancel", {"attempt_id": attempt_id}, principal)
                env.wait_ended(attempt_id)
        client.call("finalize", {}, principal)
        client.call("release", {"record": str(base.record_file(env, run_id))}, principal, timeout=60)

    fixture = env.acquire_run("20261014T010203Z-0000000a", "transform", "fixture", write_prefixes=["lakehouse/gold_e2e/"])
    checks["a run in fixture mode is refused"] = refused(
        expect_error(lambda: submit(fixture, "gold_publish", "a", "commit_publish")), "refused", "mode publish_fixture or real"
    )
    finish("20261014T010203Z-0000000a", fixture)
    bare = publish_run(env, "20261014T010203Z-0000000b", "publish_fixture", [])
    checks["a publish run that declares no write prefix is refused"] = refused(
        expect_error(lambda: submit(bare, "gold_publish", "a", "commit_publish")), "refused", "no write prefix"
    )
    finish("20261014T010203Z-0000000b", bare)
    wrong = publish_run(env, "20261014T010203Z-0000000c", "publish_fixture", ["lakehouse/gold/"])
    checks["a publish_fixture run that declares only the real gold prefix is refused"] = refused(
        expect_error(lambda: submit(wrong, "gold_publish", "a", "commit_publish")), "refused", "declares"
    )
    finish("20261014T010203Z-0000000c", wrong)
    unsealed = publish_run(env, "20261014T010203Z-0000000d", "publish_fixture", ["lakehouse/gold_e2e/"], sealed=False)
    checks["a sealed job on a run with no sealed build is refused"] = refused(
        expect_error(lambda: submit(unsealed, "gold_publish", "a", "commit_publish")), "refused", "not sealed"
    ) and refused(expect_error(lambda: submit(unsealed, "gold_export", "b")), "refused", "not sealed")
    checks["maintenance needs no sealed build"] = expect_error(lambda: submit(unsealed, "gold_publish", "m", "gold_maintenance")) is None
    finish("20261014T010203Z-0000000d", unsealed)
    good = publish_run(env, GOLD_RUN, "publish_fixture", ["lakehouse/gold_e2e/"], accepted=True)
    checks["an argument from the client is refused at submit"] = isinstance(
        expect_error(lambda: submit(good, "gold_publish", "x", "commit_publish", ["--mode", "real"])), launcher.ClientError
    )
    exported = expect_error(lambda: submit(good, "gold_export", "e")) is None
    env.wait(lambda: any(a["launch_id"] == "e" and a["phase"] == "started" for a in env.state()["attempts"].values()), 20, "the export starts")
    stop_launch(env, client, good, "e")
    published = expect_error(lambda: submit(good, "gold_publish", "c", "publish_candidates")) is None
    checks["a publish_fixture run with its prefix and a sealed build is accepted for the export and the writer job"] = exported and published
    env.wait(lambda: any(a["launch_id"] == "c" and a["phase"] == "started" for a in env.state()["attempts"].values()), 20, "the writer job starts")
    created = [c for c in env.daemon.create_bodies.values() if c["Labels"].get("hai.template") == "gold_publish"]
    checks["the writer job was created with the writer's credential and the run's mode"] = bool(created) and any(
        f"POLARIS_CLIENT_ID={env.secret_values['POLARIS_GOLD_WRITER_CLIENT_ID']}" in c["Env"] and "publish_fixture" in c["Cmd"] for c in created
    )
    finish(GOLD_RUN, good)
    return checks


def gold_publish_sentinel(env: Env) -> dict[str, bool]:
    """The sentinel scan. Every secret value is random; the writer's and the reader's values appear only in the create bodies of their
    own templates, only under POLARIS_CLIENT_SECRET, and never in the registry, the launcher log or another template's specification."""
    with_writer(env)
    hold_forever(env)
    base.catalog_script(env)
    env.start()
    base.catalog_up(env)
    client = env.client()
    run_id = GOLD_RUN
    principal = publish_run(env, run_id, "publish_fixture", ["lakehouse/gold_e2e/"], accepted=True)
    for launch, template, job in (
        ("w1", "gold_publish", "publish_candidates"),
        ("w2", "gold_publish", "catalog_backup"),
        ("r1", "gold_publish", "reconcile_candidates"),
        ("c1", "gold_export", None),
    ):
        env.submit(principal, template, launch, [], {"job": job} if job else {})
        env.wait(started(env, launch), 20, launch)
        stop_launch(env, client, principal, launch)
    writer, reader = env.secret_values["POLARIS_GOLD_WRITER_CLIENT_SECRET"], env.secret_values["POLARIS_READER_CLIENT_SECRET"]
    root, database = env.secret_values["POLARIS_CLIENT_SECRET"], env.secret_values["POSTGRES_PASSWORD"]
    bodies = {
        (c["Labels"].get("hai.template"), c["Labels"].get("hai.launch")): c
        for c in env.daemon.create_bodies.values()
        if c["Labels"].get("hai.template", "").startswith("gold_")
    }

    def where(secret: str) -> set[str]:
        return {f"{template}:{launch}" for (template, launch), body in bodies.items() if secret in json.dumps(body)}

    def only_in_secret_variable(secret: str) -> bool:
        return all(
            [entry for entry in body["Env"] if secret in entry] == [f"POLARIS_CLIENT_SECRET={secret}"] for body in bodies.values() if secret in json.dumps(body)
        )

    texts = json.dumps(env.state()) + (env.registry / "launcher.log").read_text()
    for path in (env.work).rglob("*"):
        if path.is_file():
            texts += path.read_text(errors="replace")
    return {
        "the writer secret is in the writer jobs only": where(writer) == {"gold_publish:w1", "gold_publish:w2"},
        "the reader secret is in the reconcile job only": where(reader) == {"gold_publish:r1"},
        "the root secret and the database password are in no gold specification": not where(root) and not where(database),
        "each secret sits only in POLARIS_CLIENT_SECRET": only_in_secret_variable(writer) and only_in_secret_variable(reader),
        "no secret value is in the registry, the launcher log or the case folders": not any(value in texts for value in (writer, reader, root, database)),
        "the export specification holds no AWS or catalog variable": all(
            not any(entry.startswith(("AWS", "POLARIS")) for entry in body["Env"]) for (template, _), body in bodies.items() if template == "gold_export"
        ),
        "the recorded environment holds commitments, not values": all(
            v.startswith("hmac:")
            for operation in env.state()["operations"].values()
            for k, v in (operation.get("projection", {}).get("env", {}) or {}).items()
            if k == "POLARIS_CLIENT_SECRET"
        ),
    }


def completion_error(record: dict[str, Any]) -> str:
    """The refusal ``completion_of`` gives a run with this record, or an empty string when it accepts the record."""
    try:
        launcher.completion_of(sealed_run(record))
    except launcher.LauncherError as error:
        return str(error)
    return ""


def gold_completion_format(env: Env) -> dict[str, bool]:
    """The sealed-build record as its producer writes it (``file`` is ``sealed/<sha256>.duckdb`` in the run folder) is accepted and resolves to the
    sealed copy; any other path form is refused; a link, a missing file and an absent record are refused."""
    env.start()
    checks: dict[str, bool] = {}
    checks["control: a run with no completion record is refused"] = "not sealed" in launcher_run_error({"run_id": GOLD_RUN})
    sealed = sealed_file(env, GOLD_RUN)
    accepted = completion_error(dict(L1B_COMPLETION))
    checks["the producer's own record is accepted"] = accepted == ""
    invalid_runs: list[tuple[str, dict[str, Any], str]] = [
        ("an unsealed run", {"build_state": "building"}, "not sealed"),
        ("an older generation", {"generation": 2}, "not the one of the live build"),
        ("another live attempt", {"build_attempt": "at-other"}, "not the one of the live build"),
        ("another build binding", {"build_binding": {"attempt": "at-other", "generation": 1}}, "not the one of the live build"),
        ("another run's completion", {"completion": {**L1B_COMPLETION, "run_id": "20261014T010204Z-0a1b2c3d"}}, "not the one of the live build"),
    ]
    for label, changes, reason in invalid_runs:
        checks[f"{label} is refused by the provenance check"] = reason in launcher_run_error({**sealed_run(dict(L1B_COMPLETION)), **changes})
    sha = SEALED_SHA
    other = "d" * 64
    for label, file in {
        "a bare file name": f"{sha}.duckdb",
        "a folder other than sealed": f"other/{sha}.duckdb",
        "a name for another digest": f"sealed/{other}.duckdb",
        "a path that climbs out of the folder": f"sealed/../{sha}.duckdb",
        "a nested folder": f"sealed/nested/{sha}.duckdb",
        "an absolute path": f"/sealed/{sha}.duckdb",
        "a backslash path": f"sealed\\{sha}.duckdb",
        "another suffix": f"sealed/{sha}.db",
    }.items():
        checks[f"{label} is refused"] = completion_error({**L1B_COMPLETION, "file": file}) != ""
    checks["a digest in capital letters is refused"] = completion_error({**L1B_COMPLETION, "sha256": sha.upper(), "file": f"sealed/{sha.upper()}.duckdb"}) != ""
    checks["a record with no file is refused"] = completion_error({key: value for key, value in L1B_COMPLETION.items() if key != "file"}) != ""
    normalized = {}
    try:
        normalized = launcher.completion_of(sealed_run(dict(L1B_COMPLETION)))
        rendered = render_with_record(env, normalized)
        mounted = (str(sealed.resolve()), "/workspace/sealed/sealed.duckdb", True) in rendered["mounts"]
        command = rendered["cmd"] == ["--run", GOLD_RUN, "--sealed-sha256", sha]
    except launcher.LauncherError:
        mounted = command = False
    checks["the export mounts the run's sealed copy read-only as one file and passes its digest"] = mounted and command
    missing = sealed.parent / ("e" * 64 + ".duckdb")
    absent = {**L1B_COMPLETION, "sha256": "e" * 64, "file": f"sealed/{missing.name}"}
    checks["a record whose sealed file is missing is refused at the render"] = bool(render_error(env, absent))
    link = sealed.parent / ("f" * 64 + ".duckdb")
    link.symlink_to(sealed)
    checks["a record whose sealed file is a link is refused at the render"] = bool(
        render_error(env, {**L1B_COMPLETION, "sha256": "f" * 64, "file": f"sealed/{link.name}"})
    )
    return checks


def launcher_run_error(run: dict[str, Any]) -> str:
    try:
        launcher.completion_of(run)
    except launcher.LauncherError as error:
        return str(error)
    return ""


def render_error(env: Env, record: dict[str, Any]) -> str:
    """The refusal a gold_export render gives a completion record as the launcher hands it over after ``completion_of``."""
    try:
        normalized = launcher.completion_of(sealed_run(record))
        render_with_record(env, normalized)
    except launcher.LauncherError as error:
        return str(error)
    return ""


RESULT_NAME = "consumer_result.json"


def consumer_result(sha: str = SEALED_SHA, **changes: Any) -> dict[str, Any]:
    """A consumer result as the export step writes it (the manifest digest is a stand-in)."""
    return {"schema": 1, "run_id": GOLD_RUN, "expected_sha256": sha, "in_container_sha256": sha, "exported": True, "manifest_sha256": "b" * 64, **changes}


def write_result(env: Env, document: dict[str, Any], run_id: str = GOLD_RUN) -> str:
    """Write the run's consumer result; returns its SHA-256."""
    folder = env.temp / "runs" / run_id / "publish"
    folder.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    (folder / RESULT_NAME).write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def error_of(action: Callable[[], Any]) -> str:
    """The text of the launcher or client refusal an action gives, or an empty string when it succeeds."""
    try:
        action()
    except (launcher.LauncherError, launcher.ClientError) as error:
        return str(error)
    return ""


def gold_consumer_acceptance(env: Env) -> dict[str, bool]:
    """858 and D1 section 10 item 6: the launcher accepts the exact consumer result of the run's export attempt by one rule, records it, and admits the
    four publish steps only against that record; a rejected result, a changed result or no acceptance admits none of them."""
    with_writer(env)
    base.catalog_script(env, others_run_forever=False)
    env.start()
    base.catalog_up(env)
    client = env.client()
    checks: dict[str, bool] = {}
    principal = publish_run(env, GOLD_RUN, "publish_fixture", ["lakehouse/gold_e2e/"])
    env.submit(principal, "gold_export", "e1", [], {})
    export = next(a for a in env.state()["attempts"].values() if a["launch_id"] == "e1")
    env.wait_ended(export["id"])

    def submit(launch: str, job: str) -> str:
        return error_of(lambda: env.submit(principal, "gold_publish", launch, [], {"job": job}))

    def accept(attempt: str) -> str:
        return error_of(lambda: client.call("accept_consumer", {"attempt_id": attempt}, principal))

    def acceptance() -> Any:
        return env.state()["runs"][GOLD_RUN].get("consumer_acceptance")

    sha = write_result(env, consumer_result())
    checks["control: the export attempt ended with exit 0 and its result file is in the run's publish folder"] = (
        env.attempt(export["id"]).get("exit_code") == 0 and (env.temp / "runs" / GOLD_RUN / "publish" / RESULT_NAME).is_file()
    )
    checks["before any acceptance: the first publish step is refused"] = "accepted" in submit("s0", "publish_candidates")
    checks["before any acceptance: every other publish step is refused"] = all(
        "accepted" in submit(f"s0{job}", job) for job in ("reconcile_candidates", "catalog_backup", "commit_publish")
    )
    checks["before any acceptance: maintenance does not need one"] = submit("m0", "gold_maintenance") == ""
    stop_all(env, client, principal)

    write_result(env, consumer_result(expected_sha256="d" * 64))
    refusal = accept(export["id"])
    checks["a rejected result is not accepted"] = bool(refusal) and "rejected" in refusal
    checks["a rejected result leaves no acceptance record"] = acceptance() is None
    checks["after the rejection: commit_publish is refused"] = "accepted" in submit("s1", "commit_publish")
    for label, change in (("another run's identity", {"run_id": "20261014T010203Z-0000beef"}), ("a consumer that did not export", {"exported": False})):
        write_result(env, consumer_result(**change))
        checks[f"{label} is not accepted"] = bool(accept(export["id"])) and acceptance() is None

    sha = write_result(env, consumer_result())
    other = next(iter(env.state()["attempts"].values()))
    checks["control: a result the rule accepts is accepted"] = accept(export["id"]) == ""
    record = acceptance() or {}
    checks["the acceptance names the export attempt, the exact result digest and the sealed digest"] = (
        record.get("attempt_id") == export["id"] and record.get("result_sha256") == sha and record.get("sealed_sha256") == SEALED_SHA
    )
    checks["after the acceptance: the four publish steps are admitted"] = all(
        submit(f"a{index}", job) == "" for index, job in enumerate(("publish_candidates", "reconcile_candidates", "catalog_backup", "commit_publish"))
    )
    env.wait(lambda: all(a["phase"] == "ended" for a in env.state()["attempts"].values() if a["launch_id"].startswith("a")), 30, "the admitted steps end")
    created = [c for c in env.daemon.create_bodies.values() if c["Labels"].get("hai.template") == "gold_publish" and c["Labels"].get("hai.launch") == "a3"]
    checks["the commit job is created with the accepted result digest"] = bool(created) and ["--accepted-result-sha256", sha] == created[0]["Cmd"][-2:]

    write_result(env, consumer_result(reused=True))
    checks["a result that changed after the acceptance admits no step"] = "changed" in submit("t1", "commit_publish")

    checks["an attempt of another template is not accepted"] = bool(accept(next(a["id"] for a in env.state()["attempts"].values() if a["launch_id"] == "a0")))
    checks["an attempt id that does not exist is not accepted"] = bool(accept("at-000000000000"))
    del other
    stop_all(env, client, principal)
    client.call("finalize", {}, principal)
    client.call("release", {"record": str(base.record_file(env, GOLD_RUN))}, principal, timeout=60)
    return checks


def stop_all(env: Env, client: Any, principal: dict[str, Any]) -> None:
    """Cancel every open attempt of the run and wait for the ends."""
    for attempt_id, attempt in env.state()["attempts"].items():
        if attempt["phase"] != "ended" and attempt["owner"] == f"run:{GOLD_RUN}":
            client.call("cancel", {"attempt_id": attempt_id}, principal)
            env.wait_ended(attempt_id)


def gold_maintenance_fence(env: Env) -> dict[str, bool]:
    """A maintenance job takes a registry fence at admission and holds it until its attempt ends. The fence fixes the protection file, and
    no reservation or registration changes while it is held: not when the job is queued, not while it runs, so not after its last read."""
    with_writer(env)
    hold_forever(env)
    base.catalog_script(env)
    env.start()
    base.catalog_up(env)
    client = env.client()
    checks: dict[str, bool] = {}
    principal = publish_run(env, GOLD_RUN, "publish_fixture", ["lakehouse/gold_e2e/"])
    folder = env.temp / "runs" / GOLD_RUN / "publish"
    old_set = "20261001T000000Z-00000001"

    def call(method: str, params: dict[str, Any]) -> str:
        return error_of(lambda: client.call(method, params, {"kind": "any"}))

    def document() -> dict[str, Any]:
        path = folder / "protected.json"
        return dict(json.loads(path.read_text())) if path.is_file() else {}

    checks["control: before any maintenance a reservation and a registration are accepted"] = (
        call("reserve", {"reader": "r_pending", "domain": "training", "publish_run": old_set}) == ""
        and call("reserve", {"reader": "r_set", "domain": "training", "publish_run": old_set}) == ""
        and call("register_set", {"reader": "r_set", "snapshot_set": old_set}) == ""
    )
    taken = error_of(lambda: env.submit(principal, "gold_publish", "m1", [], {"job": "gold_maintenance"}))
    attempt = next((a for a in env.state()["attempts"].values() if a["launch_id"] == "m1"), {"id": ""})
    checks["maintenance is admitted"] = taken == "" and bool(attempt["id"])
    doc = document()
    readers = env.state()["readers"]
    checks["the protection file is written at admission, bound to the run and the attempt"] = (
        doc.get("schema") == 1 and (doc.get("fence") or {}).get("run_id") == GOLD_RUN and (doc.get("fence") or {}).get("attempt_id") == attempt["id"]
    )
    items = {item.get("id"): item for item in doc.get("items", [])}
    checks["a pending reservation is listed with the time it was taken"] = items.get("r_pending") == {
        "kind": "pending",
        "id": "r_pending",
        "since": readers["r_pending"]["at"],
    }
    checks["a registration is listed with its run"] = items.get("r_set") == {"kind": "registration", "id": "r_set", "run_id": old_set}
    checks["the file lists exactly the two readers"] = set(items) == {"r_pending", "r_set"}

    refused = call("reserve", {"reader": "r_late", "domain": "training", "publish_run": old_set})
    checks["a reservation is refused while the fence is held, at admission"] = "maintenance" in refused
    env.wait(lambda: env.attempt(attempt["id"])["phase"] == "started", 30, "the maintenance job runs")
    checks["a reservation is refused while the job runs"] = "maintenance" in call("reserve", {"reader": "r_late", "domain": "training", "publish_run": old_set})
    checks["a registration is refused while the job runs"] = "maintenance" in call("register_set", {"reader": "r_pending", "snapshot_set": old_set})
    checks["the file did not change while the job runs"] = document() == doc
    checks["no reader was added by the refused calls"] = set(env.state()["readers"]) == {"r_pending", "r_set"}
    checks["a second maintenance job is refused while the fence is held"] = "fence" in error_of(
        lambda: env.submit(principal, "gold_publish", "m2", [], {"job": "gold_maintenance"})
    )
    checks["a reader may leave while the fence is held"] = call("deregister_set", {"reader": "r_pending"}) == ""

    client.call("cancel", {"attempt_id": attempt["id"]}, principal)
    env.wait_ended(attempt["id"])
    checks["after the job ended a reservation is accepted again"] = call("reserve", {"reader": "r_late", "domain": "training", "publish_run": old_set}) == ""

    call("reserve", {"reader": "r_odd", "domain": "training", "publish_run": "pub_7"})
    call("register_set", {"reader": "r_odd", "snapshot_set": "set_7"})
    odd = error_of(lambda: env.submit(principal, "gold_publish", "m3", [], {"job": "gold_maintenance"}))
    checks["a registration that names no gold run stops the maintenance admission"] = "r_odd" in odd
    call("deregister_set", {"reader": "r_odd"})
    again = error_of(lambda: env.submit(principal, "gold_publish", "m4", [], {"job": "gold_maintenance"}))
    checks["after the end maintenance is admitted again and the file lists the readers of that moment"] = again == "" and {
        item["id"] for item in document().get("items", [])
    } == {"r_set", "r_late"}
    stop_all(env, client, principal)
    client.call("finalize", {}, principal)
    client.call("release", {"record": str(base.record_file(env, GOLD_RUN))}, principal, timeout=60)
    return checks


def started(env: Env, launch: str) -> Callable[[], bool]:
    """A predicate: the launch has an attempt that started."""

    def check() -> bool:
        return any(a["launch_id"] == launch and a["phase"] == "started" for a in env.state()["attempts"].values())

    return check


def stop_launch(env: Env, client: Any, principal: dict[str, Any], launch: str) -> None:
    """Cancel the open attempt of a launch and wait for its end."""
    for attempt_id, attempt in env.state()["attempts"].items():
        if attempt["launch_id"] == launch and attempt["phase"] != "ended":
            client.call("cancel", {"attempt_id": attempt_id}, principal)
            env.wait_ended(attempt_id)


GOLD_CASES: dict[str, GoldCase] = {
    "gold_publish_templates": gold_publish_templates,
    "gold_completion_format": gold_completion_format,
    "gold_export_offline_params": gold_export_offline_params,
    "gold_publish_admission": gold_publish_admission,
    "gold_consumer_acceptance": gold_consumer_acceptance,
    "gold_maintenance_fence": gold_maintenance_fence,
    "gold_publish_sentinel": gold_publish_sentinel,
}


def run(selected: list[str], report: Path) -> list[dict[str, Any]]:
    """Run the selected cases (all by default) against fresh test registries; returns one result per case."""
    results: list[dict[str, Any]] = []
    for name in selected or list(GOLD_CASES):
        env = Env(name, report)
        started = time.monotonic()
        error = ""
        checks: dict[str, bool] = {}
        try:
            checks = GOLD_CASES[name](env)
        except Exception as exc:
            import traceback

            error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
        leftovers = env.close()
        checks["nothing of the case is left over"] = not leftovers
        status = "pass" if checks and all(checks.values()) and not error else "fail"
        results.append(
            {"case": name, "status": status, "checks": checks, "error": error, "leftovers": leftovers, "seconds": round(time.monotonic() - started, 1)}
        )
    return results

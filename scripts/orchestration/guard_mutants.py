"""Guard mutants: copies of the A1 code with one guard removed, for the case-level failing-first logs (review round 1, finding 6).

A mutant is a set of text edits to files of a copy of the source tree: a line removed, a check turned off, a safe call replaced by the naive one, or a
function replaced by a shorter definition. ``apply`` makes the edits in a copy and returns a unified diff for the log. Each edit names the exact text it
replaces and the number of times that text must occur, so a mutant that no longer matches the code fails loudly instead of passing as a no-op. The copy is
a tree that imports and runs like the real one (``scripts/orchestration/run_failing_first.py`` runs a runner in it), so a mutant reaches the broker child
processes and the launcher process without any hook in the code under test.

``GUARDS`` maps a runner and one of its cases to the mutants that must make it fail and the check that must be among the failed ones.
"""

from __future__ import annotations

import ast
import difflib
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

BROKER = "scripts/orchestration/broker.py"
LEDGER = "scripts/orchestration/ledger.py"
SENTINEL = "scripts/orchestration/sentinel.py"
SUPERVISOR = "scripts/orchestration/supervisor.py"
OFFLINE = "scripts/orchestration/offline_checks.py"
IN_IMAGE = "scripts/orchestration/in_image_checks.py"
LAUNCHER = "orchestration/launcher.py"


CLEANUP_REPORT = (
    '                sys.stderr.write("supervisor: the cleanup after the failed start did not finish:\\n"'
    ' + "".join(f"  {item}\\n" for item in outcome["errors"]))'
)


class MutantError(Exception):
    """A mutant does not match the code it was written for."""


@dataclass(frozen=True)
class Edit:
    """Replace ``old`` by ``new`` in a file; ``old`` must occur exactly ``count`` times."""

    path: str
    old: str
    new: str
    count: int = 1


@dataclass(frozen=True)
class Replace:
    """Replace the definition of a function or method (``name`` or ``Class.name``) in a file by ``source``, a definition written without indentation."""

    path: str
    target: str
    source: str


@dataclass(frozen=True)
class Mutant:
    """One removed guard."""

    name: str
    what: str
    changes: tuple[Edit | Replace, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Guard:
    """A mutant that must make a case fail, and the check that must be among the failed ones."""

    mutant: str
    check: str


def block(*lines: str) -> str:
    """Lines of source joined into the text of an edit: each ends with a newline."""
    return "".join(f"{line}\n" for line in lines)


def definition(text: str, target: str) -> tuple[int, int, int]:
    """The first line, the last line (both 1-based, decorators included) and the indentation of a function or method in a source text."""
    scope: list[ast.stmt] = ast.parse(text).body
    *classes, name = target.split(".")
    for class_name in classes:
        scope = next(node for node in scope if isinstance(node, ast.ClassDef) and node.name == class_name).body
    node = next((item for item in scope if isinstance(item, ast.FunctionDef) and item.name == name), None)
    if node is None or node.end_lineno is None:
        raise MutantError(f"no definition {target}")
    first = min([node.lineno, *(decorator.lineno for decorator in node.decorator_list)])
    return first, node.end_lineno, node.col_offset


def change_text(text: str, change: Edit | Replace) -> str:
    """The text after one change."""
    if isinstance(change, Edit):
        found = text.count(change.old)
        if found != change.count:
            raise MutantError(f"{change.path}: expected {change.count} occurrence(s) of {change.old!r}, found {found}")
        return text.replace(change.old, change.new)
    first, last, indent = definition(text, change.target)
    lines = text.splitlines(keepends=True)
    replacement = textwrap.indent(textwrap.dedent(change.source).rstrip("\n") + "\n", " " * indent)
    return "".join([*lines[: first - 1], replacement, *lines[last:]])


def apply(tree: Path, mutant: Mutant, *, write: bool = True) -> str:
    """Make the mutant's changes in a copy of the source tree (or only check that they still match, with ``write=False``); return a unified diff."""
    diffs: list[str] = []
    for path in dict.fromkeys(change.path for change in mutant.changes):
        original = (tree / path).read_text()
        text = original
        for change in mutant.changes:
            if change.path == path:
                text = change_text(text, change)
        if write:
            (tree / path).write_text(text)
        diffs += difflib.unified_diff(original.splitlines(keepends=True), text.splitlines(keepends=True), f"a/{path}", f"b/{path}", n=1)
    return "".join(diffs)


def mutants() -> dict[str, Mutant]:
    """The table of mutants."""
    release_id = 'request_id_of(f"{run_id}:release", "release")'
    release_call = 'result: dict[str, Any] = self.call("release", {"record": record}, self.principal(run_id), ' + release_id + ")"
    acquire_call = '            self.call("acquire", params, request_id=request_id_of(f"{request.run_id}:{request.mode}", "acquire"))'
    reason = "{row['reason']}"
    fresh_request_id = Replace(
        BROKER, "request_id_of", 'def request_id_of(submission_id: str, operation: str = "submit") -> str:\n    return f"brk-{operation}-{os.urandom(8).hex()}"'
    )
    stub = "def {name}({arguments}) -> {result}:\n    return {value}"
    lookup = 'stack["services"].get(service)'
    in_memory = 'self.__dict__.setdefault("seen_attempts", {}).get(service)'
    done = block('        stack["services"][service] = attempt["id"]', "        return attempt")
    done_in_memory = block(
        '        stack["services"][service] = attempt["id"]', '        self.seen_attempts[service] = attempt["id"]', "        return attempt"
    )
    orchestration_attempt = block(
        '        attempt_id = stack["services"].get(service)',
        "        if attempt_id is not None:",
        '            return state["attempts"][attempt_id]',
        "        seconds = 900 if service in ORCHESTRATION_FINITE else 10**7",
        "        attempt = self.new_attempt(",
        "            state,",
        "            ORCHESTRATION_OWNER,",
        '            "orchestration_service",',
        '            f"orch-{service}",',
        "            [],",
        '            {"service": service},',
        '            request="launcher",',
        '            checkout=stack["checkout"],',
        "            seconds=seconds,",
        "        )",
        '        stack["services"][service] = attempt["id"]',
        "        return attempt",
    )
    lineage_attempt = block(
        '        attempt_id = stack["services"].get(service)',
        "        if attempt_id is not None:",
        '            return state["attempts"][attempt_id]',
        "        seconds = 10**7",
        "        attempt = self.new_attempt(",
        "            state,",
        "            LINEAGE_OWNER,",
        '            "lineage_service",',
        '            f"lineage-{service}",',
        "            [],",
        '            {"service": service},',
        '            request="launcher",',
        '            checkout=stack["checkout"],',
        "            seconds=seconds,",
        "        )",
        '        stack["services"][service] = attempt["id"]',
        "        return attempt",
    )
    found = [
        Mutant(
            "docker_call_in_broker",
            "the broker runs a docker program before it forwards",
            (
                Edit(
                    BROKER,
                    "        lines = self.lines_of(request)\n        if self.recorded_as_noop",
                    '        import subprocess; subprocess.run(["docker", "version"], check=False)\n'
                    "        lines = self.lines_of(request)\n        if self.recorded_as_noop",
                ),
            ),
        ),
        Mutant(
            "refusal_recorded_as_success",
            "a launcher refusal is recorded as a finished submission with exit 0",
            (Edit(BROKER, "self.finish(request, UNKNOWN_END, refused=True, reason=reason)", "self.finish(request, 0, refused=True, reason=reason)"),),
        ),
        Mutant(
            "release_with_owner_flag",
            "the broker passes the owner flag when it releases a run",
            (Edit(BROKER, "self.principal(run_id), " + release_id, '{**self.principal(run_id), "owner": True}, ' + release_id),),
        ),
        Mutant(
            "release_refusal_swallowed",
            "the broker answers 'released' when the launcher refuses the release",
            (
                Edit(
                    BROKER,
                    block("        " + release_call),
                    block("        try:", "            " + release_call, "        except launcher.ClientError:", '            result = {"state": "released"}'),
                ),
            ),
        ),
        Mutant(
            "broker_allocates",
            "the broker sends the launcher a reservation of its own when it acquires a run",
            (
                Edit(
                    BROKER,
                    block(acquire_call),
                    block(acquire_call, "            with contextlib.suppress(Exception):", '                self.client.call("reserve", {})'),
                ),
            ),
        ),
        Mutant(
            "stale_status",
            "the broker answers from the launcher's last recorded phase without checking that it serves",
            (Replace(BROKER, "Broker.require_serving", stub.format(name="require_serving", arguments="self", result="None", value="None")),),
        ),
        Mutant(
            "duplicate_forwards",
            "no forward cache, and a fresh launcher request ID and launch ID at every forward",
            (
                Replace(
                    BROKER,
                    "Broker.read_forward",
                    stub.format(name="read_forward", arguments="self, request: SubmissionRequest", result="dict[str, Any] | None", value="None"),
                ),
                fresh_request_id,
                Replace(
                    BROKER, "launch_id_of", stub.format(name="launch_id_of", arguments="submission_id: str", result="str", value='f"sub-{os.urandom(8).hex()}"')
                ),
            ),
        ),
        Mutant("random_request_id", "every forward uses a fresh launcher request ID", (fresh_request_id,)),
        Mutant(
            "spec_check_skipped",
            "a changed template row is forwarded",
            (Edit(BROKER, '        if sha256_text(canonical(payload)) != intent["spec_sha256"]:', "        if False:"),),
        ),
        Mutant(
            "request_schema_off",
            "the request schema accepts everything",
            (Edit(BROKER, 'self.requests_schema = schema_validator("submission_request.schema.json")', "self.requests_schema = Draft202012Validator({})"),),
        ),
        Mutant(
            "unmapped_row_accepted",
            "a row with no launcher template is recorded as a no-op",
            (
                Edit(
                    BROKER,
                    block(
                        '        if not row["no_op"] and row.get("launcher") is None:',
                        f'            raise SubmissionRefused(f"no launcher template runs {{request.template}} in mode {{request.mode}}: {reason}")',
                    ),
                    block('        if not row["no_op"] and row.get("launcher") is None:', '            row = {**row, "no_op": True}'),
                ),
            ),
        ),
        Mutant(
            "class_unchecked",
            "the network class is not checked",
            (Edit(BROKER, 'if row["class"] not in (OFFLINE, *getattr(launcher, "NETWORK_CLASSES_ENFORCED", ())):', "if False:", count=2),),
        ),
        Mutant(
            "templates_schema_off",
            "the broker template file is not validated against its schema",
            (Edit(BROKER, "    errors = sorted(Draft202012Validator(schema).iter_errors(document), key=str)\n", "    errors = []\n"),),
        ),
        Mutant("auth_off", "the listener accepts any token", (Edit(BROKER, "if not secrets.compare_digest(supplied, expected):", "if False:"),)),
        Mutant("body_limit_off", "the listener reads a body of any size", (Edit(BROKER, "if length > MAX_BODY_BYTES:", "if False:"),)),
        Mutant(
            "redaction_off",
            "the broker's redaction does nothing",
            (Replace(BROKER, "Broker.redact", stub.format(name="redact", arguments="self, text: str", result="str", value="text")),),
        ),
        Mutant("bind_any_host", "the listener binds any host", (Edit(BROKER, 'if host not in ("127.0.0.1", "::1"):', "if False:"),)),
        Mutant(
            "forward_before_intent",
            "the broker forwards first and writes the intent afterwards",
            (
                Replace(
                    BROKER,
                    "Broker.submit",
                    block(
                        "def submit(self, request: SubmissionRequest) -> Handle:",
                        "    handle = Handle(request.submission_id, request.container_name)",
                        "    with self.ledger.locked(request.run_id):",
                        "        row = self.row_for(request)",
                        "        payload = self.payload_of(request, row)",
                        "        principal = self.ensure_run(request)",
                        '        answer = self.call("submit", payload, principal, request_id_of(request.submission_id))',
                        "        self.write_intent(request, row, payload)",
                        '        forward = {"attempt_id": answer["attempt_id"], "n": answer["n"], "deadline_utc": answer["deadline_utc"]}',
                        '        write_private(self.forward_file(request), json.dumps(forward, sort_keys=True) + "\\n")',
                        "        self.advance_locked(request)",
                        "    return handle",
                    ),
                ),
            ),
        ),
        Mutant(
            "ledger_skips_bad_lines",
            "a torn or unparseable ledger line is skipped",
            (
                Edit(LEDGER, 'if text and not text.endswith("\\n"):', "if False:"),
                Edit(
                    LEDGER,
                    block("            except ValueError:", '                raise LedgerError(f"{path.name}:{number}: not JSON") from None'),
                    block("            except ValueError:", "                continue"),
                ),
            ),
        ),
        Mutant(
            "ledger_skips_schema",
            "a ledger line is not checked against its schema",
            (
                Edit(
                    LEDGER,
                    block(
                        "            errors = sorted(self._schema.iter_errors(line), key=str)",
                        "            if errors:",
                        '                raise LedgerError(f"{path.name}:{number}: {errors[0].message}")',
                    ),
                    block("            pass"),
                ),
            ),
        ),
        Mutant(
            "ledger_skips_order",
            "an event that cannot follow the one before it is accepted",
            (
                Edit(
                    LEDGER,
                    block(
                        '            if line["event"] not in NEXT_EVENTS.get(previous, set()):',
                        "                raise LedgerError(f\"{path.name}:{number}: {line['event']} cannot follow {previous}\")",
                    ),
                    block("            pass"),
                ),
            ),
        ),
        Mutant(
            "broker_writes_container_file",
            "the broker keeps a container file of its own",
            (
                Edit(
                    BROKER,
                    block('        container_id = attempt.get("container_id")'),
                    block(
                        '        container_id = attempt.get("container_id")', '        (self.paths.state / "broker.container").write_text(str(container_id))'
                    ),
                ),
            ),
        ),
        Mutant(
            "environment_in_record",
            "the broker writes its environment into every record",
            (
                Edit(
                    BROKER, block('            "written_at": now(),'), block('            "written_at": now(),', '            "environment": dict(os.environ),')
                ),
            ),
        ),
        Mutant(
            "scan_skips_unreadable",
            "an unreadable file is skipped without a trace",
            (
                Edit(
                    SENTINEL,
                    block("            except OSError:", "                result.unreadable.append(label)", "                continue"),
                    block("            except OSError:", "                continue"),
                ),
            ),
        ),
        Mutant(
            "scan_skips_special_files",
            "a FIFO is skipped without a trace",
            (
                Edit(
                    SENTINEL,
                    block("                if not stat.S_ISREG(mode):", "                    result.skipped.append(label)", "                    continue"),
                    block("                if not stat.S_ISREG(mode):", "                    continue"),
                ),
            ),
        ),
        Mutant(
            "scan_ignores_missing_root",
            "a missing root is skipped without a trace",
            (Edit(SENTINEL, block("        result.missing_roots.append(root.name)"), ""),),
        ),
        Mutant(
            "scan_accepts_nothing_read",
            "a scan that read no file passes",
            (Edit(SENTINEL, "        return not (self.unreadable or self.skipped or self.missing_roots) and self.files > 0", "        return True"),),
        ),
        Mutant(
            "scan_skips_links",
            "a link's own text is not searched for the values",
            (
                Edit(
                    SENTINEL,
                    block(
                        "                    if any(needle in os.readlink(path).encode() for needle in needles):",
                        "                        result.hits.append(label)",
                    ),
                    "",
                ),
            ),
        ),
        Mutant(
            "scan_credits_every_link",
            "a link counts as covered whatever it resolves to",
            (Edit(SENTINEL, block("                    if not link_is_covered(path, scanned):"), block("                    if False:")),),
        ),
        Mutant(
            "scan_counts_links_as_files",
            "a link is counted as a file read",
            (
                Edit(
                    SENTINEL,
                    block("                    result.links += 1"),
                    block("                    result.links += 1", "                    result.files += 1"),
                ),
            ),
        ),
        Mutant(
            "log_failure_raises",
            "a failed log write raises into the request handler and the server's error hook",
            (
                Edit(
                    BROKER,
                    block("            except Exception as error:", "                listener.log_failures += 1"),
                    block("            except KeyError as error:", "                listener.log_failures += 1"),
                ),
            ),
        ),
        Mutant(
            "lookup_skipped",
            "the present row decides a submission before the launcher is asked about its original request",
            (Edit(BROKER, block("        accepted = self.accepted_forward(request, principal)"), block("        accepted = None")),),
        ),
        Mutant(
            "lookup_failure_read_as_none",
            "a lookup the launcher cannot answer is read as no attempt",
            (
                Edit(
                    BROKER,
                    block("        accepted = self.accepted_forward(request, principal)"),
                    block(
                        "        try:",
                        "            accepted = self.accepted_forward(request, principal)",
                        "        except LauncherUnavailable:",
                        "            accepted = None",
                    ),
                ),
            ),
        ),
        Mutant(
            "token_existence_as_proof",
            "a token file that is missing or cannot be searched is read as proof that the broker never sent the submission",
            (
                Edit(
                    BROKER,
                    block("        principal = self.ensure_run(request)", "        accepted = self.accepted_forward(request, principal)"),
                    block(
                        '        hidden = not os.path.lexists(self.client.config.token_root / f"{request.run_id}.token")',
                        "        try:",
                        "            principal = self.ensure_run(request)",
                        "        except LauncherUnavailable:",
                        "            if not hidden:",
                        "                raise",
                        "            principal = {}",
                        "        accepted = None if hidden else self.accepted_forward(request, principal)",
                    ),
                ),
            ),
        ),
        Mutant(
            "unreadable_token_read_as_none",
            "a token that cannot be read is read as the launcher's answer that it holds no attempt",
            (
                Edit(
                    BROKER,
                    block("        principal = self.ensure_run(request)", "        accepted = self.accepted_forward(request, principal)"),
                    block(
                        "        try:",
                        "            principal = self.ensure_run(request)",
                        "        except LauncherUnavailable:",
                        "            principal = {}",
                        "        accepted = self.accepted_forward(request, principal) if principal else None",
                    ),
                ),
            ),
        ),
        Mutant(
            "unsearchable_store_reacquires",
            "any failure to read the token store sends the broker to acquire a run, not only a token file that is absent",
            (
                Edit(
                    BROKER,
                    block("        except LauncherUnavailable:", "            if not self.token_absent(request.run_id):", "                raise"),
                    block("        except LauncherUnavailable:", "            pass"),
                ),
            ),
        ),
        Mutant(
            "dispatch_marker_not_written",
            "the broker does not record that it is about to send, so a sent submission cannot be told from one never sent",
            (
                Edit(
                    BROKER,
                    block("            self.record_dispatch(request)"),
                    block("            pass"),
                ),
            ),
        ),
        Mutant(
            "marker_in_forward_subtree",
            "the dispatch marker lies in the forwards folder, a subtree that is independent of the ledger",
            (
                Edit(
                    BROKER,
                    block('        return self.paths.ledger / f"{request.run_id}.{sha256_text(request.submission_id)[:32]}.sent"'),
                    block('        return self.paths.forwards / request.run_id / f"{sha256_text(request.submission_id)[:32]}.sent"'),
                ),
            ),
        ),
        Mutant(
            "marker_absence_as_proof",
            "a dispatch marker that is not found in the ledger folder is read as proof that nothing was sent, so the launcher is not asked",
            (
                Edit(
                    BROKER,
                    block(
                        "        try:", '            reply = self.call("status", {"what": "submission", "id": request_id_of(request.submission_id)}, principal)'
                    ),
                    block(
                        "        if not self.dispatch_file(request).is_file():",
                        "            return None",
                        "        try:",
                        '            reply = self.call("status", {"what": "submission", "id": request_id_of(request.submission_id)}, principal)',
                    ),
                ),
            ),
        ),
        Mutant(
            "first_intent_error_unclassified",
            "a failed publication of the first intent's directories is not an unavailable launcher, so the listener answers 500",
            (
                Edit(
                    BROKER,
                    block(
                        "                try:",
                        "                    self.write_intent(request, row, payload)",
                        "                except LedgerUnavailable as error:",
                        "                    raise LauncherUnavailable(str(error)) from None",
                    ),
                    block("                self.write_intent(request, row, payload)"),
                ),
            ),
        ),
        Mutant(
            "dispatch_folders_not_published",
            "only the marker file and its own folder are flushed; the folders above it that were created for it are not",
            (
                Edit(
                    BROKER,
                    block("            write_private(self.dispatch_file(request), body, self.paths.state)"),
                    block("            write_private(self.dispatch_file(request), body)"),
                ),
            ),
        ),
        Mutant(
            "teardown_error_dropped",
            "a teardown request or wait that fails is not listed under errors",
            (Edit(SUPERVISOR, block('            errors.append(self.failure("stack", error))'), block("            pass")),),
        ),
        Mutant(
            "non_down_phase_accepted",
            "a phase other than down is not an error",
            (
                Edit(
                    SUPERVISOR,
                    block(
                        '            if record.get("phase") != "down":',
                        "                errors.append(f\"stack: the launcher reports phase {record.get('phase')}, not down; run down again\")",
                    ),
                    "",
                ),
            ),
        ),
        Mutant(
            "wait_error_escapes",
            "the wait for the stack raises the launcher's error past teardown",
            (
                Edit(
                    SUPERVISOR,
                    block('            record = self.wait_for_stack(("down",), 600)'),
                    block('            record = self.client.wait_for("orchestration", "", "phase", ("down",), {"kind": "any"}, 600)'),
                ),
            ),
        ),
        Mutant(
            "start_cleanup_unreported",
            "a failed start drops the errors of its cleanup",
            (
                Edit(
                    SUPERVISOR,
                    block(
                        '            if outcome.get("errors"):',
                        CLEANUP_REPORT,
                    ),
                    "",
                ),
            ),
        ),
        Mutant(
            "session_files_world_readable",
            "the session files are readable by everyone",
            (Edit(BROKER, "    os.replace(temporary, path)\n", "    os.replace(temporary, path)\n    os.chmod(path, 0o644)\n"),),
        ),
        Mutant(
            "no_cleanup_on_failure",
            "a failed start leaves the listener, the token and the stack to the owner",
            (
                Edit(
                    SUPERVISOR,
                    block(
                        "        except BaseException:",
                        "            outcome = self.teardown()",
                        '            if outcome.get("errors"):',
                        CLEANUP_REPORT,
                        "            raise",
                    ),
                    block("        except BaseException:", "            raise"),
                ),
            ),
        ),
        Mutant(
            "airflow_image_unchecked",
            "the supervisor does not look for the built Airflow image",
            (Replace(SUPERVISOR, "Supervisor.check_images", stub.format(name="check_images", arguments="self", result="dict[str, Any]", value="{}")),),
        ),
        Mutant(
            "postgres_not_pulled",
            "the supervisor does not pull the PostgreSQL image",
            (
                Edit(
                    SUPERVISOR,
                    block('        pulled = self.call("pull", {"reference": self.platform["metadata_database"]["image"]}, timeout=PULL_SECONDS)'),
                    block("        pulled = {}"),
                ),
            ),
        ),
        Mutant(
            "second_start_allowed",
            "a second session starts while one runs",
            (
                Edit(
                    SUPERVISOR,
                    block(
                        '        if current.get("phase") != "down":',
                        "            raise SupervisorError(f\"the orchestration stack is {current.get('phase')}; run down first\")",
                    ),
                    block("        pass"),
                ),
            ),
        ),
        Mutant(
            "teardown_stops_catalog",
            "a session's teardown also stops the catalog",
            (
                Edit(
                    SUPERVISOR,
                    block("        try:", '            self.call("orchestration_down")'),
                    block('        self.call("catalog_down")', "        try:", '            self.call("orchestration_down")'),
                ),
            ),
        ),
        Mutant(
            "compose_check_off",
            "the Compose file check finds nothing",
            (
                Replace(
                    OFFLINE,
                    "compose_problems",
                    stub.format(name="compose_problems", arguments="text: str, platform: dict[str, Any]", result="list[str]", value="[]"),
                ),
            ),
        ),
        Mutant(
            "dockerfile_check_off",
            "the Dockerfile check finds nothing",
            (
                Replace(
                    OFFLINE,
                    "dockerfile_problems",
                    stub.format(name="dockerfile_problems", arguments="text: str, platform: dict[str, Any]", result="list[str]", value="[]"),
                ),
            ),
        ),
        Mutant(
            "constraints_check_off",
            "the constraints file check finds nothing",
            (
                Replace(
                    OFFLINE,
                    "constraints_problems",
                    stub.format(name="constraints_problems", arguments="data: bytes, platform: dict[str, Any]", result="list[str]", value="[]"),
                ),
            ),
        ),
        Mutant(
            "dag_import_check_off",
            "the DAG import check finds nothing",
            (
                Replace(
                    OFFLINE, "dag_import_problems", stub.format(name="dag_import_problems", arguments="name: str, source: str", result="list[str]", value="[]")
                ),
            ),
        ),
        Mutant(
            "graph_validator_off",
            "the transform graph validator finds nothing",
            (Edit(OFFLINE, "        rules = sorted({violation.rule for violation in validate_transform_graph(graph)})", "        rules: list[str] = []"),),
        ),
        Mutant(
            "distribution_prefix_exempt",
            "every name that starts with apache-airflow is exempt from the distribution check",
            (
                Edit(
                    IN_IMAGE,
                    "if name not in pins and name not in tools and name not in release)",
                    'if name not in pins and name not in tools and not name.startswith("apache-airflow"))',
                ),
            ),
        ),
        Mutant(
            "api_published_on_all_interfaces",
            "the API server publishes its port on every interface",
            (Edit(LAUNCHER, "ports.append((host_ip, int(host_port), int(container_port)))", 'ports.append(("0.0.0.0", int(host_port), int(container_port)))'),),
        ),
        Mutant(
            "capabilities_kept",
            "no service drops a capability",
            (Edit(LAUNCHER, '"cap_drop": [str(item) for item in raw.get("cap_drop") or []],', '"cap_drop": [],'),),
        ),
        Mutant(
            "migration_exit_ignored",
            "a migration that exits non-zero lets the stack go on",
            (
                Edit(
                    LAUNCHER,
                    'if service in ORCHESTRATION_FINITE and attempt["reason"] == "exited" and attempt.get("exit_code") == 0:',
                    "if service in ORCHESTRATION_FINITE:",
                ),
            ),
        ),
        Mutant(
            "health_not_awaited",
            "the stack goes on without waiting for a service to be healthy",
            (
                Edit(
                    LAUNCHER,
                    block(
                        '        if service in ORCHESTRATION_FINITE or attempt["phase"] != "started":',
                        "            return",
                        '        if stack["health"].get(service) == "healthy":',
                    ),
                    block('        if service in ORCHESTRATION_FINITE or attempt["phase"] != "started":', "            return", "        if True:"),
                ),
            ),
        ),
        Mutant(
            "lineage_health_not_awaited",
            "the lineage stack goes on without waiting for a service to be healthy",
            (
                Edit(
                    LAUNCHER,
                    block('        if attempt["phase"] != "started":', "            return", '        if stack["health"].get(service) == "healthy":'),
                    block('        if attempt["phase"] != "started":', "            return", "        if True:"),
                ),
            ),
        ),
        Mutant(
            "up_during_teardown_allowed",
            "orchestration_up is accepted while the stack is being torn down",
            (
                Edit(
                    LAUNCHER,
                    block(
                        '        if stack["phase"] == "tearing_down":',
                        '            raise LauncherError("the orchestration stack is being torn down", "refused")',
                    ),
                    block("        pass"),
                ),
            ),
        ),
        Mutant(
            "service_attempt_not_reused",
            "a service's attempt is found only in the launcher's memory, so a restarted launcher creates it again",
            (Edit(LAUNCHER, orchestration_attempt, orchestration_attempt.replace(lookup, in_memory).replace(done, done_in_memory)),),
        ),
        Mutant(
            "lineage_attempt_not_reused",
            "a lineage service's attempt is found only in the launcher's memory, so a restarted launcher creates it again",
            (Edit(LAUNCHER, lineage_attempt, lineage_attempt.replace(lookup, in_memory).replace(done, done_in_memory)),),
        ),
        Mutant(
            "catalog_sees_stack_network",
            "the catalog's absence check counts the stack's network",
            (Edit(LAUNCHER, 'if (n.get("Labels") or {}).get(LABEL_STACK) != ORCHESTRATION_STACK]', "]"),),
        ),
        Mutant(
            "state_folder_unchecked",
            "the state folder is not held to the allowed folders",
            (
                Edit(
                    LAUNCHER,
                    block(
                        '        roots = [checkout / "data" / "orchestration", self.main_checkout / "data" / "orchestration", *self.cfg.allowed_roots]',
                        "        if not inside(candidate, roots):",
                        '            raise LauncherError(f"state {text!r} is outside the allowed folders", "args")',
                    ),
                    block(
                        '        roots = [checkout / "data" / "orchestration", self.main_checkout / "data" / "orchestration", *self.cfg.allowed_roots]',
                        "        pass",
                    ),
                ),
            ),
        ),
        Mutant(
            "lineage_state_folder_unchecked",
            "the lineage state folder is not held to the allowed folders",
            (
                Edit(
                    LAUNCHER,
                    block(
                        '        roots = [checkout / "data" / "lineage", self.main_checkout / "data" / "lineage", *self.cfg.allowed_roots]',
                        "        if not inside(candidate, roots):",
                        '            raise LauncherError(f"state {text!r} is outside the allowed folders", "args")',
                    ),
                    block('        roots = [checkout / "data" / "lineage", self.main_checkout / "data" / "lineage", *self.cfg.allowed_roots]', "        pass"),
                ),
            ),
        ),
        Mutant(
            "teardown_removes_other_projects",
            "the stack's teardown also removes the containers of every Compose project",
            (
                Edit(
                    LAUNCHER,
                    block(
                        "        stack = self.orchestration_record(ctx.state)",
                        '        if stack["phase"] in ("down", "tearing_down"):',
                        '            return {"phase": stack["phase"]}',
                        '        stack.update({"phase": "tearing_down", "after": "down", "stop_step": "network"})',
                        '        return {"phase": "tearing_down"}',
                    ),
                    block(
                        "        stack = self.orchestration_record(ctx.state)",
                        '        if stack["phase"] in ("down", "tearing_down"):',
                        '            return {"phase": stack["phase"]}',
                        '        stack.update({"phase": "tearing_down", "after": "down", "stop_step": "network"})',
                        '        for other in self.docker.containers(label="com.docker.compose.project"):',
                        '            self.docker.remove(str(other["Id"]))',
                        '        return {"phase": "tearing_down"}',
                    ),
                ),
            ),
        ),
        Mutant(
            "lineage_teardown_removes_other_projects",
            "the lineage stack's teardown also removes the containers of every Compose project",
            (
                Edit(
                    LAUNCHER,
                    block(
                        "        stack = self.lineage_record(ctx.state)",
                        '        if stack["phase"] in ("down", "tearing_down"):',
                        '            return {"phase": stack["phase"]}',
                        '        stack.update({"phase": "tearing_down", "after": "down", "stop_step": "network"})',
                        '        return {"phase": "tearing_down"}',
                    ),
                    block(
                        "        stack = self.lineage_record(ctx.state)",
                        '        if stack["phase"] in ("down", "tearing_down"):',
                        '            return {"phase": stack["phase"]}',
                        '        stack.update({"phase": "tearing_down", "after": "down", "stop_step": "network"})',
                        '        for other in self.docker.containers(label="com.docker.compose.project"):',
                        '            self.docker.remove(str(other["Id"]))',
                        '        return {"phase": "tearing_down"}',
                    ),
                ),
            ),
        ),
        Mutant(
            "lineage_api_not_joined",
            "the lineage API is created on the frontend network and never joined to the backend network",
            (
                Edit(
                    LAUNCHER,
                    block('        if service == "api":', '            rendered.joins[f"hai-{registry_id[:8]}-lineage-backend"] = ["lineage-api"]'),
                    block('        if service == "api":', "            pass"),
                ),
                Edit(LAUNCHER, 'or created_on | set(rendered.joins) != {f"hai-{registry_id[:8]}-lineage-{role}" for role in expected}', "or False"),
            ),
        ),
        Mutant(
            "lineage_join_refusal_ignored",
            "a join the daemon refuses is ignored and the container is started",
            (
                Edit(
                    LAUNCHER,
                    block("                    self.docker.connect_network(network, container_id, aliases)"),
                    block(
                        "                    with contextlib.suppress(Exception):",
                        "                        self.docker.connect_network(network, container_id, aliases)",
                    ),
                ),
            ),
        ),
        Mutant(
            "lineage_join_repeated_on_retry",
            "a retried start joins the networks again although the container shows them",
            (Edit(LAUNCHER, "                if network not in present:", "                if True:"),),
        ),
        Mutant(
            "pull_without_digest_rule",
            "pull accepts a reference that is not pinned by digest",
            (Edit(LAUNCHER, 'if "@sha256:" not in reference:', "if False:"),),
        ),
        Mutant(
            "check_job_on_a_network",
            "the in-image check runs on a network",
            (
                Edit(
                    LAUNCHER,
                    block('            self.compose.memory("airflow-check", values),', '            "none",'),
                    block('            self.compose.memory("airflow-check", values),', '            "bridge",'),
                ),
            ),
        ),
    ]
    return {mutant.name: mutant for mutant in found}


MUTANTS: dict[str, Mutant] = mutants()

GUARDS: dict[str, dict[str, tuple[Guard, ...]]] = {
    "broker": {
        "no_direct_docker": (Guard("docker_call_in_broker", "no docker program was started by the broker child"),),
        "released_run_refused": (Guard("refusal_recorded_as_success", "the second submission failed"),),
        "failed_retained_refused": (Guard("release_with_owner_flag", "the broker cannot release a failed-retained run"),),
        "recovering_run_refused": (Guard("refusal_recorded_as_success", "the submission failed"),),
        "release_while_in_flight": (Guard("refusal_recorded_as_success", "the reconciliation forwarded it and the launcher refused"),),
        "release_refused_while_running": (Guard("release_refusal_swallowed", "release is refused while the attempt runs"),),
        "broker_holds_no_reservation": (Guard("broker_allocates", "it never sent reserve, allocate, register or recover"),),
        "outage_during_job": (Guard("stale_status", "the broker said the launcher was unavailable during the outage"),),
        "duplicate_submission": (Guard("duplicate_forwards", "one launcher attempt exists for the submission"),),
        "crash_after_forward": (Guard("random_request_id", "the ledger is intent then started"),),
        "crash_after_intent": (Guard("spec_check_skipped", "a row changed since the intent is failed closed"),),
        "override_fields_refused": (Guard("request_schema_off", "command in the body is refused"),),
        "unmapped_template_refused": (Guard("unmapped_row_accepted", "refused because no launcher row exists"),),
        "non_offline_class_refused": (Guard("class_unchecked", "a submission in another class is refused with the class named"),),
        "launcher_refuses_changed_args": (Guard("refusal_recorded_as_success", "the launcher's argument grammar refused the extra option"),),
        "broker_templates_schema": (Guard("templates_schema_off", "a launcher row with an image key is refused"),),
        "listener_auth": (Guard("auth_off", "a wrong token is refused with 401"),),
        "listener_limits": (Guard("body_limit_off", "a body over 4 KiB is refused with 413"),),
        "token_never_logged": (Guard("redaction_off", "no error body contains the session token"),),
        "listener_loopback_only": (Guard("bind_any_host", "a non-loopback bind is refused for each host tried"),),
        "ledger_fsync_order": (Guard("forward_before_intent", "the intent was on disk when acquire was sent"),),
        "ledger_torn_line": (Guard("ledger_skips_bad_lines", "status is refused with the ledger named"),),
        "ledger_bad_schema_line": (Guard("ledger_skips_schema", "status is refused with the ledger named"),),
        "ledger_out_of_order": (Guard("ledger_skips_order", "status is refused with the ledger named"),),
        "ledger_has_no_container_state": (Guard("broker_writes_container_file", "the broker keeps no container file of its own"),),
        "sentinel_scan": (Guard("environment_in_record", "no planted value is in the broker log, ledger, records, saved logs, forward cache or registry"),),
        "sentinel_scan_failed_job": (
            Guard("environment_in_record", "no planted value is in the broker log, ledger, records, saved logs, forward cache or registry"),
        ),
        "sentinel_scan_coverage": (
            Guard("scan_skips_unreadable", "an unreadable file fails the scan and is named"),
            Guard("scan_skips_special_files", "a FIFO fails the scan, is named and is not opened"),
            Guard("scan_ignores_missing_root", "a missing root fails the scan and is named"),
            Guard("scan_accepts_nothing_read", "a scan that read no file fails"),
            Guard("scan_skips_links", "a link is not followed, its own text is searched and a dangling link makes the scan incomplete"),
        ),
        "sentinel_scan_links": (
            Guard("scan_credits_every_link", "a link to a marker-bearing file outside the roots makes the scan incomplete"),
            Guard("scan_counts_links_as_files", "a link adds no file, byte or per-root count"),
            Guard("scan_skips_links", "a link's own text is searched for the values"),
        ),
        "log_write_outage": (Guard("log_failure_raises", "nothing reaches stderr with the session token"),),
        "cancel_after_inventory_drift": (
            Guard("lookup_skipped", "the restart leaves the submission unfinished"),
            Guard("lookup_failure_read_as_none", "the restart reports the submission pending"),
        ),
        "cancel_after_marker_ancestor_loss": (
            Guard("marker_in_forward_subtree", "run folder lost, changed arguments: the dispatch marker is written beside the ledger"),
            Guard("marker_absence_as_proof", "ledger folder removed during the read, changed arguments: the ledger is not ended by the restart"),
            Guard("marker_absence_as_proof", "ledger folder replaced during the read, removed mapping: the attempt received the cancel"),
        ),
        "cancel_after_marker_write_replacement": (
            Guard("marker_absence_as_proof", "changed arguments: the ledger is not ended by the restart"),
            Guard("marker_absence_as_proof", "removed mapping: the launcher is asked once, by the original request ID"),
        ),
        "never_sent_only_by_lookup": (
            Guard("lookup_skipped", "a run with no token: acquire, then the lookup, then the submit"),
            Guard("marker_absence_as_proof", "never sent, none, changed arguments: the lookup says not found"),
            Guard(
                "lookup_failure_read_as_none",
                "never sent, the launcher cannot answer the lookup, changed arguments: the restart reports the submission pending",
            ),
            Guard("token_existence_as_proof", "never sent, missing token file, removed mapping: the restart reports the submission pending"),
            Guard("unreadable_token_read_as_none", "never sent, unsearchable token folder, changed arguments: a status and a cancel are answered 503"),
        ),
        "first_intent_publication_failure": (Guard("first_intent_error_unclassified", "the first submit is answered 503, not 500"),),
        "dispatch_publication_failure": (
            Guard("dispatch_folders_not_published", "a folder above the marker's folder cannot be made durable: nothing is sent to the launcher as a submit"),
            Guard("dispatch_marker_not_written", "the marker's folder cannot be made durable: nothing is sent to the launcher as a submit"),
        ),
        "cancel_after_token_store_loss": (
            Guard("token_existence_as_proof", "the ledger is not ended by the restart"),
            Guard("token_existence_as_proof", "never sent, missing token file, changed arguments: nothing is ended while the token cannot be read"),
            Guard("unreadable_token_read_as_none", "the restart reports the submission pending"),
            Guard("unsearchable_store_reacquires", "never sent, unchanged row, unsearchable token folder: no acquire or submit is sent"),
        ),
    },
    "supervisor": {
        "supervisor_full_session": (Guard("session_files_world_readable", "the token and every metadata file are owner-only"),),
        "supervisor_partial_start": (Guard("no_cleanup_on_failure", "the session token is deleted"),),
        "supervisor_failure_after_start": (Guard("no_cleanup_on_failure", "everything was taken down"),),
        "supervisor_image_missing": (Guard("airflow_image_unchecked", "the start was refused"),),
        "supervisor_pulls_missing_postgres": (Guard("postgres_not_pulled", "one pull was made"),),
        "supervisor_second_start_refused": (Guard("second_start_allowed", "the second start was refused"),),
        "supervisor_leaves_the_catalog": (Guard("teardown_stops_catalog", "the catalog's containers are the same before and after"),),
        "supervisor_teardown_unresolved": (
            Guard("teardown_error_dropped", "the result lists the stack step under errors"),
            Guard("non_down_phase_accepted", "a wait that returns another phase makes the command return nonzero"),
            Guard("wait_error_escapes", "a launcher error raised by the wait does not escape teardown"),
            Guard("start_cleanup_unreported", "a failed start reports the cleanup that did not finish"),
        ),
    },
    "stack": {
        "stack_up_down": (
            Guard("api_published_on_all_interfaces", "the API server publishes only on the loopback"),
            Guard("capabilities_kept", "Airflow runs as the caller's UID with group 0, no capability, no new privileges"),
        ),
        "stack_partial_start": (Guard("migration_exit_ignored", "the stack is down"),),
        "stack_unhealthy_service": (Guard("health_not_awaited", "the stack is down"),),
        "stack_teardown_barrier": (Guard("up_during_teardown_allowed", "orchestration_up is refused while the stack is being torn down"),),
        "stack_resume_after_launcher_kill": (Guard("service_attempt_not_reused", "the API server was created once"),),
        "stack_independent_of_catalog": (Guard("catalog_sees_stack_network", "the catalog teardown barrier is cleared"),),
        "stack_no_docker_restart": (Guard("teardown_removes_other_projects", "a container of another project still runs"),),
        "stack_request_validation": (Guard("state_folder_unchecked", "a state folder outside the allowed folders is refused"),),
        "pull_rules": (Guard("pull_without_digest_rule", "a tag-only reference is refused"),),
        "orchestration_check_template": (Guard("check_job_on_a_network", "it runs on no network"),),
        "lineage_two_networks": (
            Guard("lineage_api_not_joined", "exactly one join is sent, for the API and the backend network, with the lineage-api alias"),
            Guard("lineage_teardown_removes_other_projects", "a container of another project still runs after the lineage teardown"),
        ),
        "lineage_join_refused": (Guard("lineage_join_refusal_ignored", "one join was tried and the API was never started"),),
        "lineage_join_resumed_after_launcher_kill": (
            Guard("lineage_join_repeated_on_retry", "exactly one join was sent in all"),
            Guard("lineage_attempt_not_reused", "the API was created once"),
        ),
        "lineage_unhealthy_service": (Guard("lineage_health_not_awaited", "the lineage stack is down"),),
        "lineage_request_validation": (Guard("lineage_state_folder_unchecked", "a state folder outside the allowed folders is refused"),),
    },
    "offline": {
        "compose": (Guard("compose_check_off", "mutation: compose with another PostgreSQL digest refused"),),
        "dockerfile": (Guard("dockerfile_check_off", "mutation: Dockerfile with a pip install refused"),),
        "constraints": (Guard("constraints_check_off", "mutation: a changed constraints line refused"),),
        "dag_imports": (Guard("dag_import_check_off", "mutation: DAG importing subprocess refused"),),
        "graph": (Guard("graph_validator_off", "graph (plain): cycle"),),
        "distribution": (Guard("distribution_prefix_exempt", "distribution check: an unpinned apache-airflow-* distribution is reported"),),
    },
}

BEFORE_FIX: dict[str, tuple[str, ...]] = {
    "broker": ("malformed_content_length", "cancel_after_lost_submit_reply", "noop_crash_recovery"),
    "supervisor": ("supervisor_listener_start_failure", "supervisor_teardown_independent"),
    "stack": ("stack_memory_floor", "orchestration_dump_template"),
}

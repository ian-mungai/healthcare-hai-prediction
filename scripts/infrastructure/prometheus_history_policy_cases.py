"""Cases for the two task roles of Prometheus history in S3: the shipper creates blocks once, the restore only reads.

The shipper (``infra/iam/owner_policies/prometheus_history_ship.json``) may create an object under ``prometheus/blocks/`` when the request carries the
create-only header and list that prefix, and nothing else. The restore (``prometheus_history_restore.json``) may read and list the same prefix and never
write. Each case is one request with the answer the policy must give, judged by the repository's policy evaluator. The planted defects are copies of the
policies with one mistake each (write access in the restore role, a prefix wider than ``prometheus/blocks/``, a create without its header, a read or
delete in the shipper); the cases must catch every one, so a case that cannot fail is found here.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, build_request, decide

SHIP_POLICY = "infra/iam/owner_policies/prometheus_history_ship.json"
RESTORE_POLICY = "infra/iam/owner_policies/prometheus_history_restore.json"
BUCKET = "example-project-ci-bucket"
BLOCK = "01HZZZZZZZZZZZZZZZZZZZZZZZ"
ONCE = {"s3:if-none-match": "*"}
OTHER_KEYS = (
    "prometheus/other/x",
    "prometheus/x",
    "prometheus",
    "prometheus/blocks",
    "prometheusx/blocks/x",
    "other/prometheus/blocks/x",
    "mlflow/x",
    "promote/x",
    "lakehouse/gold/x.parquet",
)
WRITE_ACTIONS = ("PutObject", "PutObjectAcl", "PutObjectTagging", "DeleteObject", "DeleteObjectVersion", "AbortMultipartUpload", "RestoreObject")
READ_ACTIONS = ("GetObject", "GetObjectVersion", "GetObjectTagging", "GetObjectAttributes", "ListMultipartUploadParts")
Documents = dict[str, dict[str, Any]]
Mutation = Callable[[Documents], None]


def documents(root: Path) -> Documents:
    """Both policies with the bucket name filled in."""
    return {
        name: json.loads((root / path).read_text(encoding="utf-8").replace("${DATA_BUCKET_NAME}", BUCKET))
        for name, path in (("shipper", SHIP_POLICY), ("restore", RESTORE_POLICY))
    }


def allowed(policy: dict[str, Any], request: Request) -> bool:
    return decide([policy], request).allowed


def object_request(action: str, key: str, header: bool, bucket: str = BUCKET) -> Request:
    return build_request(f"s3:{action}", f"arn:aws:s3:::{bucket}/{key}", extra=ONCE if header else None)


def list_request(prefix: str | None, bucket: str = BUCKET) -> Request:
    return build_request("s3:ListBucket", f"arn:aws:s3:::{bucket}", extra=None if prefix is None else {"s3:prefix": (prefix,)})


def request_cases(role: str) -> list[tuple[str, Request, bool]]:
    """Every request with the verdict the named role must get: ``shipper`` or ``restore``."""
    shipper = role == "shipper"
    blocks = [f"prometheus/blocks/{BLOCK}/{name}" for name in ("meta.json", "index", "tombstones", "manifest.json", "chunks/000001")]
    cases: list[tuple[str, Request, bool]] = []
    for key in blocks:
        cases.append((f"{role}: create {key} once", object_request("PutObject", key, True), shipper))
        cases.append((f"{role}: PutObject {key} without the create-only header", object_request("PutObject", key, False), False))
        cases.append((f"{role}: GetObject {key}", object_request("GetObject", key, False), not shipper))
        cases.append((f"{role}: GetObject {key} with the create-only header", object_request("GetObject", key, True), not shipper))
        cases.append((f"{role}: GetObject {key} in another bucket", object_request("GetObject", key, False, f"{BUCKET}-other"), False))
        cases.append((f"{role}: PutObject {key} in another bucket", object_request("PutObject", key, True, f"{BUCKET}-other"), False))
        cases.append(
            (
                f"{role}: PutObject {key} with another if-none-match value",
                build_request("s3:PutObject", f"arn:aws:s3:::{BUCKET}/{key}", extra={"s3:if-none-match": "abc"}),
                False,
            )
        )
    for key in OTHER_KEYS:
        for action in ("PutObject", "GetObject"):
            for header in (True, False):
                cases.append((f"{role}: {action} outside prometheus/blocks/ {key} header={header} refused", object_request(action, key, header), False))
    for action in WRITE_ACTIONS:
        if action != "PutObject":
            for header in (True, False):
                cases.append((f"{role}: {action} on a block header={header} refused", object_request(action, blocks[0], header), False))
    for action in READ_ACTIONS:
        if action != "GetObject":
            cases.append((f"{role}: {action} on a block refused", object_request(action, blocks[0], False), False))
    for prefix in ("prometheus/blocks/", f"prometheus/blocks/{BLOCK}/index"):
        cases.append((f"{role}: list under {prefix}", list_request(prefix), True))
    for wrong in (None, "", "prometheus/", "prometheus", "prometheus/blocks", "prometheusx/blocks/", "lakehouse/", "mlflow/", "other/prometheus/blocks/"):
        cases.append((f"{role}: list with prefix {wrong!r} refused", list_request(wrong), False))
    cases.append((f"{role}: list in another bucket refused", list_request("prometheus/blocks/", f"{BUCKET}-other"), False))
    cases.append((f"{role}: s3:* on a block refused", object_request("*", blocks[0], True), False))
    return cases


def verdicts(docs: Documents) -> dict[str, bool]:
    """Whether the policies give every request the answer it must get."""
    return {label: allowed(docs[label.split(":", 1)[0]], request) == expected for role in docs for label, request, expected in request_cases(role)}


def mistakes(docs: Documents) -> list[str]:
    return [label for label, passed in verdicts(docs).items() if not passed]


def statement(docs: Documents, role: str, sid: str) -> dict[str, Any]:
    return next(item for item in docs[role]["Statement"] if item["Sid"] == sid)


def sets(role: str, sid: str, field: str, value: Any) -> Mutation:
    def apply(docs: Documents) -> None:
        statement(docs, role, sid)[field] = value

    return apply


def adds_action(role: str, sid: str, *actions: str) -> Mutation:
    def apply(docs: Documents) -> None:
        item = statement(docs, role, sid)
        item["Action"] = [*([item["Action"]] if isinstance(item["Action"], str) else item["Action"]), *actions]

    return apply


def drops_condition(role: str, sid: str) -> Mutation:
    def apply(docs: Documents) -> None:
        del statement(docs, role, sid)["Condition"]

    return apply


def adds_statement(role: str, new: dict[str, Any]) -> Mutation:
    def apply(docs: Documents) -> None:
        docs[role]["Statement"].append(new)

    return apply


def widened_list(role: str, prefix: str) -> Mutation:
    return sets(role, "ListPrometheusBlocks", "Condition", {"StringLike": {"s3:prefix": [prefix]}})


DEFECTS: dict[str, tuple[Mutation, str]] = {
    "the restore role may write": (adds_action("restore", "ReadPrometheusBlocks", "s3:PutObject"), "restore: create"),
    "the restore role may delete": (adds_action("restore", "ReadPrometheusBlocks", "s3:DeleteObject"), "restore: DeleteObject"),
    "the restore role may abort uploads": (adds_action("restore", "ReadPrometheusBlocks", "s3:AbortMultipartUpload"), "restore: AbortMultipartUpload"),
    "the restore role has a second statement that writes": (
        adds_statement("restore", {"Sid": "Extra", "Effect": "Allow", "Action": "s3:PutObject", "Resource": f"arn:aws:s3:::{BUCKET}/prometheus/blocks/*"}),
        "restore: create",
    ),
    "the restore read reaches the whole bucket": (
        sets("restore", "ReadPrometheusBlocks", "Resource", f"arn:aws:s3:::{BUCKET}/*"),
        "restore: GetObject outside",
    ),
    "the restore read reaches a prefix that only starts with prometheus": (
        sets("restore", "ReadPrometheusBlocks", "Resource", f"arn:aws:s3:::{BUCKET}/prometheus*"),
        "restore: GetObject outside",
    ),
    "the restore read reaches prometheus/ as a whole": (
        sets("restore", "ReadPrometheusBlocks", "Resource", f"arn:aws:s3:::{BUCKET}/prometheus/*"),
        "restore: GetObject outside",
    ),
    "the restore listing reaches prometheus/ as a whole": (widened_list("restore", "prometheus/*"), "restore: list with prefix 'prometheus/' refused"),
    "the restore listing reaches every prefix": (widened_list("restore", "*"), "restore: list with prefix"),
    "the restore listing loses its prefix condition": (drops_condition("restore", "ListPrometheusBlocks"), "restore: list with prefix"),
    "the shipper create loses its create-only header": (drops_condition("shipper", "CreatePrometheusBlocks"), "shipper: PutObject"),
    "the shipper create reaches the whole bucket": (
        sets("shipper", "CreatePrometheusBlocks", "Resource", f"arn:aws:s3:::{BUCKET}/*"),
        "shipper: PutObject outside",
    ),
    "the shipper create reaches a prefix that only starts with prometheus": (
        sets("shipper", "CreatePrometheusBlocks", "Resource", f"arn:aws:s3:::{BUCKET}/prometheus*"),
        "shipper: PutObject outside",
    ),
    "the shipper create reaches prometheus/ as a whole": (
        sets("shipper", "CreatePrometheusBlocks", "Resource", f"arn:aws:s3:::{BUCKET}/prometheus/*"),
        "shipper: PutObject outside",
    ),
    "the shipper may read": (adds_action("shipper", "CreatePrometheusBlocks", "s3:GetObject"), "shipper: GetObject"),
    "the shipper may delete": (adds_action("shipper", "CreatePrometheusBlocks", "s3:DeleteObject"), "shipper: DeleteObject"),
    "the shipper may abort uploads": (adds_action("shipper", "CreatePrometheusBlocks", "s3:AbortMultipartUpload"), "shipper: AbortMultipartUpload"),
    "the shipper may list every prefix": (widened_list("shipper", "*"), "shipper: list with prefix"),
    "the shipper may do every S3 action on blocks": (sets("shipper", "CreatePrometheusBlocks", "Action", "s3:*"), "shipper: GetObject"),
}


def the_committed_policies_pass_every_request(root: Path) -> dict[str, bool]:
    docs = documents(root)
    wrong = mistakes(docs)
    return {"every request gets the answer it must get": wrong == [], **{f"wrong: {label}": False for label in wrong}}


def every_planted_defect_is_caught(root: Path) -> dict[str, bool]:
    found: dict[str, bool] = {}
    for name, (mutate, expected) in DEFECTS.items():
        planted = copy.deepcopy(documents(root))
        mutate(planted)
        wrong = mistakes(planted)
        found[f"{name}: a case fails"] = bool(wrong)
        found[f"{name}: the case that names it fails ({expected})"] = any(label.startswith(expected) for label in wrong)
    return found


def the_policies_hold_only_the_expected_statements(root: Path) -> dict[str, bool]:
    docs = documents(root)
    return {
        "the shipper holds one create and one list statement": [item["Sid"] for item in docs["shipper"]["Statement"]]
        == ["CreatePrometheusBlocks", "ListPrometheusBlocks"],
        "the restore holds one read and one list statement": [item["Sid"] for item in docs["restore"]["Statement"]]
        == ["ReadPrometheusBlocks", "ListPrometheusBlocks"],
        "every statement allows": all(item["Effect"] == "Allow" for policy in docs.values() for item in policy["Statement"]),
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "iam_prometheus_history_policies_pass_every_request": the_committed_policies_pass_every_request,
    "iam_prometheus_history_every_planted_defect_is_caught": every_planted_defect_is_caught,
    "iam_prometheus_history_policies_hold_only_the_expected_statements": the_policies_hold_only_the_expected_statements,
}

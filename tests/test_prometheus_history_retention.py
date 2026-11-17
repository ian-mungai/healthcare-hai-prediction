"""The S3 lifecycle of the shipped Prometheus blocks stays matched to the local retention, and the prefixes of the roles, the lifecycle and the code agree."""

import json
import re
from pathlib import Path

from monitoring import prometheus_history as history
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
MARGIN_DAYS = 2


def found(match: re.Match[str] | None, message: str) -> re.Match[str]:
    """The match, or a failed test that says what was missing."""
    if match is None:
        raise AssertionError(message)
    return match


def default_of(path: str, name: str) -> int:
    """The numeric default of a Terraform variable, read from its block."""
    text = (ROOT / path).read_text(encoding="utf-8")
    block = found(re.search(rf'variable "{name}" \{{(.*?)\n\}}\n', text, re.DOTALL), f"{path} declares {name}")
    return int(found(re.search(r"default\s*=\s*(\d+)", block.group(1)), f"{name} has a numeric default").group(1))


def test_the_s3_expiry_covers_the_local_retention_plus_the_margin() -> None:
    expiry = default_of("infra/variables.tf", "prometheus_history_expire_days")
    retention = default_of("infra/services/variables.tf", "prometheus_retention_days")
    check(
        expiry >= retention + MARGIN_DAYS,
        f"S3 keeps blocks {expiry} days after upload and Prometheus keeps {retention} days of samples: the margin is {MARGIN_DAYS} days",
    )


def test_the_largest_local_retention_still_has_a_valid_s3_expiry() -> None:
    upper = found(
        re.search(r"prometheus_history_expire_days <= (\d+)", (ROOT / "infra/variables.tf").read_text(encoding="utf-8")), "the S3 expiry has an upper bound"
    )
    local = found(
        re.search(r"prometheus_retention_days <= (\d+)", (ROOT / "infra/services/variables.tf").read_text(encoding="utf-8")),
        "the local retention has an upper bound",
    )
    check(int(upper.group(1)) >= int(local.group(1)) + MARGIN_DAYS, "the S3 expiry may be set to the longest local retention plus the margin")


def test_the_prefix_of_the_code_is_the_prefix_of_every_policy_and_rule() -> None:
    prefix = history.PREFIX
    for name in ("prometheus_history_ship.json", "prometheus_history_restore.json"):
        policy = json.loads((ROOT / "infra/iam/owner_policies" / name).read_text(encoding="utf-8").replace("${DATA_BUCKET_NAME}", "bucket"))
        scopes = [statement["Resource"] for statement in policy["Statement"] if statement["Action"] != "s3:ListBucket"]
        check(scopes == [f"arn:aws:s3:::bucket/{prefix}*"], f"{name} reaches the code's prefix {prefix} and nothing wider: {scopes}")
    main = (ROOT / "infra/main.tf").read_text(encoding="utf-8")
    check(f'prefix = "{prefix.split("/", 1)[0]}/"' in main, "the lifecycle rules name the top folder of the code's prefix")

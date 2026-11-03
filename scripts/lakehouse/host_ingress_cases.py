"""Stand-in AWS replies and host ingress variants for the checks of the Mac-to-AWS catalog route host (``cutover_route.describe_host`` and its callers).

The route host check is permanent code, so its cases live here and the temporary bronze cutover runner imports them (never the other way round).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any


def describe_runner(instance: dict[str, Any] | None, groups: list[dict[str, Any]] | None = None) -> Callable[..., SimpleNamespace]:
    """Stand-in `describe-instances` and `describe-security-groups` replies; a None instance or None groups make that call fail."""

    def runner(command: list[str], **_: Any) -> SimpleNamespace:
        if "describe-security-groups" in command:
            if groups is None:
                return SimpleNamespace(returncode=255, stdout="", stderr="An error occurred (UnauthorizedOperation)")
            return SimpleNamespace(returncode=0, stdout=json.dumps({"SecurityGroups": groups}), stderr="")
        if instance is None:
            return SimpleNamespace(returncode=255, stdout="", stderr="An error occurred (UnauthorizedOperation)")
        return SimpleNamespace(returncode=0, stdout=json.dumps({"Reservations": [{"OwnerId": INGRESS_OWNER, "Instances": [instance]}]}), stderr="")

    return runner


INGRESS_OWNER = "111122223333"


def ingress_rule(**sources: Any) -> dict[str, Any]:
    """One inbound rule on the catalog port; by default its only source is the project's task security group."""
    rule = {"IpProtocol": "tcp", "FromPort": 8181, "ToPort": 8181, "IpRanges": [], "Ipv6Ranges": [], "PrefixListIds": []}
    rule["UserIdGroupPairs"] = [{"GroupId": "sg-0task", "UserId": INGRESS_OWNER}]
    return {**rule, **sources}


def source_group(project_tag: str, group: str = "sg-0task", **changes: Any) -> dict[str, Any]:
    """A source security group: by default the project's, in the host's VPC and account, with the project tag."""
    found = {"GroupId": group, "OwnerId": INGRESS_OWNER, "VpcId": "vpc-0project", "Tags": [{"Key": "Project", "Value": project_tag}], "IpPermissions": []}
    return {**found, **changes}


def ingress_host(tags: list[dict[str, str]], extra_interface_group: bool = False) -> dict[str, Any]:
    """A running host with a public address (the host takes one by design: no NAT gateway) and the host security group."""
    interface_groups = [{"GroupId": "sg-0host"}] + ([{"GroupId": "sg-0extra"}] if extra_interface_group else [])
    return {
        "State": {"Name": "running"},
        "VpcId": "vpc-0project",
        "PublicIpAddress": "192.0.2.1",
        "SecurityGroups": [{"GroupId": "sg-0host"}],
        "NetworkInterfaces": [{"Association": {"PublicIp": "192.0.2.1"}, "Groups": interface_groups}],
        "Tags": tags,
    }


def ingress_variants(project_tag: str) -> list[tuple[str, dict[str, Any] | None, list[dict[str, Any]] | None, bool]]:
    """(label, instance, security groups, accepted): accepted only when every inbound source is a project security group of the host's VPC."""
    host = ingress_host([{"Key": "Project", "Value": project_tag}])
    task = source_group(project_tag)

    def groups(*rules: dict[str, Any], extra: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        return [{"GroupId": "sg-0host", "IpPermissions": list(rules) or [ingress_rule()]}, task, *(extra or [])]

    unrelated = [{"GroupId": "sg-0other", "UserId": INGRESS_OWNER}]
    return [
        ("a public address with only project security group sources passes", host, groups(), True),
        ("an IPv4 world range is refused", host, groups(ingress_rule(IpRanges=[{"CidrIp": "0.0.0.0/0"}])), False),
        ("a narrow IPv4 range is refused", host, groups(ingress_rule(IpRanges=[{"CidrIp": "198.51.100.7/32"}])), False),
        ("an IPv6 range is refused", host, groups(ingress_rule(Ipv6Ranges=[{"CidrIpv6": "::/0"}])), False),
        ("a prefix list source is refused", host, groups(ingress_rule(PrefixListIds=[{"PrefixListId": "pl-0abc"}])), False),
        (
            "a security group of another account is refused",
            host,
            groups(
                ingress_rule(UserIdGroupPairs=[{"GroupId": "sg-0other", "UserId": "444455556666"}]),
                extra=[source_group(project_tag, "sg-0other", OwnerId="444455556666")],
            ),
            False,
        ),
        (
            "a same-account group of another application is refused",
            host,
            groups(ingress_rule(UserIdGroupPairs=unrelated), extra=[source_group("other_application", "sg-0other")]),
            False,
        ),
        (
            "a project group in another VPC is refused",
            host,
            groups(ingress_rule(UserIdGroupPairs=unrelated), extra=[source_group(project_tag, "sg-0other", VpcId="vpc-0elsewhere")]),
            False,
        ),
        ("a source group missing from the reply is refused", host, groups(ingress_rule(UserIdGroupPairs=unrelated)), False),
        (
            "an open group on an interface only is refused",
            ingress_host([{"Key": "Project", "Value": project_tag}], extra_interface_group=True),
            groups(extra=[{"GroupId": "sg-0extra", "IpPermissions": [ingress_rule(IpRanges=[{"CidrIp": "0.0.0.0/0"}])]}]),
            False,
        ),
        (
            "a host security group missing from the reply is refused",
            ingress_host([{"Key": "Project", "Value": project_tag}], extra_interface_group=True),
            groups(),
            False,
        ),
        ("a host with no security group is refused", {**host, "SecurityGroups": [], "NetworkInterfaces": [{}]}, [], False),
        ("a failed security group call is refused", host, None, False),
        ("a stopped host is refused", {**host, "State": {"Name": "stopped"}}, groups(), False),
    ]

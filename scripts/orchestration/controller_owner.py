"""Run the existing owner commands after an ECS Exec root session drops to the controller identity.

Mutations require a real terminal and exact typed confirmation. The wrapper opens no additional listener.
"""

from __future__ import annotations

import argparse
import json
import os
import pwd
import sys
from pathlib import Path

from orchestration import launcher
from scripts.lakehouse import run_lock
from scripts.orchestration.controller_runtime import load_config

COMMANDS = frozenset({"status", "protected-sets", "recover", "release", "deregister-reader"})


def verify_identity(config: launcher.Config) -> None:
    """Require the configured UID, primary group zero and no supplementary groups before socket access."""
    uid = config.expected_uid
    if uid is None or uid <= 0 or os.getuid() != uid or os.geteuid() != uid or os.getgid() != 0 or os.getegid() != 0 or os.getgroups():
        raise launcher.LauncherError("owner wrapper did not obtain the controller identity with cleared groups", "owner_identity")


def drop_identity(config: launcher.Config) -> None:
    """Clear groups and drop an ECS Exec root process to the packaged controller UID and group zero."""
    if config.local_backend != "disabled" or config.expected_uid is None or config.expected_uid <= 0:
        raise launcher.LauncherError("owner wrapper requires the non-root controller configuration", "owner_identity")
    try:
        pwd.getpwuid(config.expected_uid)
    except KeyError:
        raise launcher.LauncherError("controller UID has no passwd entry", "passwd") from None
    if os.geteuid() != 0:
        raise launcher.LauncherError("owner wrapper requires the approved ECS Exec root session", "owner_identity")
    try:
        os.setgroups([])
        os.setgid(0)
        os.setuid(config.expected_uid)
    except OSError:
        raise launcher.LauncherError("owner UID drop failed, verify SETUID and SETGID capabilities", "owner_identity") from None
    verify_identity(config)


def execute(config: launcher.Config, arguments: list[str]) -> int:
    """Confirm and execute one existing owner CLI command with the same explicit controller roots."""
    args = run_lock.parser().parse_args(arguments)
    if args.command not in COMMANDS or args.test_registry is not None:
        raise launcher.LauncherError("owner wrapper accepts only status, protected-sets, recover, release and deregister-reader", "args")
    drop_identity(config)
    if args.command not in ("status", "protected-sets"):
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            raise launcher.LauncherError("owner mutation requires an interactive terminal", "owner_confirmation")
        if args.command == "release" and not args.owner:
            raise launcher.LauncherError("owner release requires --owner", "owner_confirmation")
        if args.command == "deregister-reader" and not args.owner_confirmed:
            raise launcher.LauncherError("stale-reader removal requires --owner-confirmed", "owner_confirmation")
        identity = args.reader if args.command == "deregister-reader" else args.run
        expected = f"{args.command} {identity}"
        try:
            answer = input(f"Type {expected!r} to confirm: ")
        except EOFError:
            raise launcher.LauncherError("owner confirmation was not received", "owner_confirmation") from None
        if answer != expected:
            raise launcher.LauncherError("owner confirmation does not match", "owner_confirmation")
    client = launcher.LauncherClient(config, start=False)
    result = run_lock.launcher_dispatch(args, config.registry, client=client)
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Read nonsecret controller roots, drop identity and run one closed owner command."""
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--config", type=Path, required=True)
    cli.add_argument("command", nargs=argparse.REMAINDER)
    args = cli.parse_args(argv)
    try:
        return execute(load_config(args.config), args.command)
    except (launcher.LauncherError, launcher.ClientError, run_lock.Refused, run_lock.RegistryError, OSError) as error:
        sys.stderr.write(f"controller_owner: {error}\n")
        return launcher.REFUSED_EXIT


if __name__ == "__main__":
    sys.exit(main())

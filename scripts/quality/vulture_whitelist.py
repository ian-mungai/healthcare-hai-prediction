"""Names the unused-code check (vulture) cannot see being used. Each entry is used, or kept, for the reason beside it.

vulture reads this file with every other tracked Python file, so a name used here counts as used everywhere. Add an
entry only after tracing the use; a name that is truly dead is deleted instead. An entry for a removed name fails Ruff,
MyPy and the removed-names check. This file is never imported.
"""

from __future__ import annotations

import tarfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from scripts.acquisition import shards
from scripts.acquisition.discover_history import Links
from scripts.acquisition.hcai_util_contract import _Markup
from scripts.acquisition.tests.conftest import synthetic_collection_routes
from scripts.acquisition.tests.test_capture_mode import stand_in_modes
from scripts.acquisition.tests.test_person_lane import raw_route
from scripts.acquisition.tests.test_publisher_host_run import isolated_environment
from scripts.acquisition.tests.test_reviewed_opener import Publisher
from scripts.acquisition.tests.test_task_keys import no_hosts_file_between_cases
from scripts.acquisition.transport import Download, PublicHTTPSHandler
from scripts.infrastructure.secret_store_stand_in import StandInHandler
from scripts.lakehouse.layer_acceptance import BoundedStream
from tests import test_startup_check_jobs, test_startup_check_planted
from tests.test_bronze_completion_records import record_mode
from tests.test_bronze_writer_records import writer_environment
from tests.test_pin_tables import fake_pyspark
from tests.test_startup_check_cleanup_slow import pytestmark


def uses(download: Download, member: tarfile.TarInfo, server: ThreadingHTTPServer, bridge_handler: Any) -> tuple[object, ...]:
    """Name each entry once; vulture counts these names as used."""
    return (
        StandInHandler.log_message,  # http.server calls it to log each request; the stand-in keeps its own call log.
        StandInHandler.do_POST,  # http.server calls it for each POST request.
        Links.handle_data,  # HTMLParser calls it for the text inside each anchor.
        _Markup.handle_data,  # HTMLParser calls it for workbook XML text.
        _Markup.handle_startendtag,  # HTMLParser calls it for self-closing workbook XML tags.
        synthetic_collection_routes,  # pytest runs autouse fixtures without a reference.
        no_hosts_file_between_cases,  # pytest runs autouse fixtures without a reference (test_task_keys, test_task_owners, test_task_state).
        fake_pyspark,  # pytest runs autouse fixtures without a reference.
        raw_route,  # pytest runs it by name: autouse in test_task_ledgers and test_verify_publisher_run, usefixtures in test_person_lane.
        PublicHTTPSHandler.https_open,  # urllib's OpenerDirector calls <protocol>_open by name, here and in the test handlers.
        Publisher.handler_order,  # urllib's OpenerDirector reads it to sort the handlers.
        shards.pytest_addoption,  # pytest calls the plugin hooks of scripts/acquisition/shards.py by name.
        shards.pytest_configure,  # pytest calls the plugin hooks of scripts/acquisition/shards.py by name.
        shards.ShardPlugin.pytest_collection_modifyitems,  # pytest calls the registered plugin's hooks by name.
        shards.ShardPlugin.pytest_runtest_logreport,  # pytest calls the registered plugin's hooks by name.
        shards.ShardPlugin.pytest_sessionfinish,  # pytest calls the registered plugin's hooks by name.
        download.requested_url,  # transport.download writes it to the transport metadata through dataclasses.asdict.
        member.linkname,  # tarfile reads it when the quality E2E builds its symbolic-link archive.
        stand_in_modes,  # pytest runs it by name: usefixtures("stand_in_modes") in four tests of test_capture_mode.
        isolated_environment,  # pytest runs the autouse fixture of test_publisher_host_run without a reference.
        record_mode,  # pytest runs the autouse fixture of test_bronze_completion_records without a reference.
        writer_environment,  # pytest runs the autouse fixture of test_bronze_writer_records without a reference.
        test_startup_check_jobs.private_environment,  # pytest runs the autouse fixture of test_startup_check_jobs without a reference.
        test_startup_check_planted.private_environment,  # pytest runs the autouse fixture of test_startup_check_planted without a reference.
        pytestmark,  # pytest reads the module variable of test_startup_check_cleanup_slow by name to skip its minute-long case.
        BoundedStream.seekable,  # tarfile calls it on the staged code archive stream: the RawIOBase default (False) would stop the read.
        # scripts.quality may not import scripts.orchestration (.importlinter), so the Handler and Bridge of aws_http_bridge.py are named through their bases.
        BaseHTTPRequestHandler.protocol_version,  # http.server reads it of the bridge's Handler to answer its clients with HTTP/1.1.
        bridge_handler.do_GET,  # http.server dispatches a GET request to the bridge's Handler by method name.
        bridge_handler.do_PUT,  # http.server dispatches a PUT request to the bridge's Handler by method name.
        server.daemon_threads,  # socketserver reads it when the bridge starts a request thread, and on the server that ecs_lineage_cases sets it on.
    )


_ = uses

"""Build a Spark session wired to the project's Iceberg catalog; runs inside the hai-lakehouse Spark container."""

from __future__ import annotations

import os
import re
from pathlib import Path

from pyspark.sql import SparkSession

CATALOG = "hai_lakehouse"
HEAP = re.compile(r"[1-9][0-9]*g")


def job_memory() -> str:
    """Return the heap the host computed for this job from the running containers; there is no default [488].

    ``scripts/lakehouse/catalog.py`` computes it with ``scripts/lakehouse/memory_budget.py`` before the container starts
    (memory is shared by every project's containers and never hardcoded).
    """
    value = os.environ.get("SPARK_JOB_MEMORY", "")
    if not HEAP.fullmatch(value):
        raise RuntimeError(
            f"SPARK_JOB_MEMORY is {value!r}, not a whole number of GiB such as 20g; start jobs with "
            ".venv/bin/python -m scripts.lakehouse.catalog job <name>, which computes it"
        )
    return value


def spark_session(app_name: str, warehouse: Path | None = None, memory: str | None = None) -> SparkSession:
    """Return a local Spark session whose default catalog is ``hai_lakehouse``.

    Without ``warehouse`` the catalog is the local Polaris service. Polaris does not vend storage credentials
    (``stsUnavailable``), so Iceberg's S3 client uses the project's AWS profile from the read-only mount, and the
    client secret comes from the container environment and is never logged. With ``warehouse`` the catalog is a
    throwaway file-based Iceberg catalog in that folder, used by the synthetic end-to-end tests. ``memory`` sets the
    local driver's memory, which also holds the executors in local mode. If omitted, use the computed launch heap.
    """
    prefix = f"spark.sql.catalog.{CATALOG}"
    builder = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{job_threads()}]")
        .config("spark.driver.memory", job_memory() if memory is None else memory)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if warehouse is not None:
        builder = builder.config(f"{prefix}.type", "hadoop").config(f"{prefix}.warehouse", warehouse.as_uri())
    else:
        credential = f"{os.environ['POLARIS_CLIENT_ID']}:{os.environ['POLARIS_CLIENT_SECRET']}"
        builder = (
            builder.config(f"{prefix}.type", "rest")
            .config(f"{prefix}.uri", os.environ["POLARIS_URI"])
            .config(f"{prefix}.warehouse", CATALOG)
            .config(f"{prefix}.credential", credential)
            .config(f"{prefix}.scope", "PRINCIPAL_ROLE:ALL")
            .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"{prefix}.client.region", os.environ["AWS_REGION"])
        )
    return builder.getOrCreate()


def job_threads() -> int:
    """Require the positive CPU count that the host computed before launch [531] [532]."""
    value = os.environ.get("JOB_THREADS", "")
    if not value.isdecimal() or int(value) < 1:
        raise RuntimeError("JOB_THREADS must be a positive integer; start jobs through scripts.lakehouse.catalog")
    return int(value)

"""Read-only: per bronze table, inputs against distinct SHA-256 as discover selects them today."""

import collections
import json
import os
import sys

sys.path.insert(0, os.getcwd())
bronze = __import__("scripts.lakehouse.bronze", fromlist=["bronze"])
deployment = __import__("scripts.lakehouse.catalog", fromlist=["catalog"]).deployment

settings = deployment()
os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
os.environ.setdefault("AWS_REGION", settings["aws_region"])
config = bronze.load_table_map()
names = [t["table"] for t in config["tables"]]
inputs, unselected = bronze.discover(bronze.S3Storage(), settings["data_bucket_name"], config, names, bronze.load_retired())
per = collections.defaultdict(list)
for item in inputs:
    per[item["table"]].append(item)
out = {}
for table, items in sorted(per.items()):
    shas = {i["sha256"] for i in items}
    if len(items) > len(shas):
        out[table] = {
            "objects": len(items),
            "distinct": len(shas),
            "extra_bytes": sum(i["byte_count"] for i in items) - sum({i["sha256"]: i["byte_count"] for i in items}.values()),
        }
sys.stdout.write(json.dumps({"tables_with_copies": out, "identical_copies_skipped": sum(1 for u in unselected if u.get("identical_copy"))}, indent=1) + "\n")

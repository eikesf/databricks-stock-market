"""Create the three environment catalogs, their schemas, and the landing volumes.
 
Idempotent by construction (`IF NOT EXISTS` throughout), so it is safe to re-run and safe to run
from CI. Bootstrap scripts that can only be run once are how environments drift: the second person
to join the project runs a slightly different sequence by hand.
 
Why SQL rather than the Unity Catalog REST API
-----------------------------------------------
`POST /api/2.1/unity-catalog/catalogs` (and `databricks catalogs create`) fails on an account with
Default Storage enabled:
 
    Metastore storage root URL does not exist. Default Storage is enabled in your account.
 
The REST path requires an explicit `storage_root`, which a Free Edition account does not have and
cannot create. The SQL DDL path resolves Default Storage automatically. This is not documented
prominently and costs an afternoon if you assume the API and the SQL surface are equivalent.
 
Usage:
    python3 scripts/bootstrap_catalogs.py                 # all environments
    python3 scripts/bootstrap_catalogs.py --env dev
    python3 scripts/bootstrap_catalogs.py --dry-run
    python3 scripts/bootstrap_catalogs.py --drop dng_probe # explicit, single, named
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Environments are catalogs, not workspaces - Free edition allows one metastore.

ENVIRONMENTS = ("dev", "test", "prod")
CATALOG_PREFIX = "esf"

SCHEMAS: dict[str, str] = {
    "bronze": "Raw ingest. Append-only, source-faithful, no business logic. Never queried by BI.",
    "silver": "conformed entities. Quality-enforced, deduplicated, SCD2 where history matters.",
    "gold": "Dimensional model and governed KPIs. The only layer consumers should read.",
    "ops": (
        "platform observability: data-quality metrics, quarentine, pipeline events, "
        "join coverage, streaming/batch reconciliation. Per-environment on purpose - a shared "
        "ops schema lets dev runs pollute prod dashboards."
    ),
}

VOLUMES: dict[str, tuple[str, str]] = {
    # volume name -> (schema, comment)
    "landing": (
        "bronze",
        "Event landing zone consumed by Auto Loader. Written by the synthetic amplifier.",
    ),
    "checkpoints": (
        "ops",
        "Structured Streaming checkpoints. Deleting one silently reprocesses from scratch.",
    ),
}
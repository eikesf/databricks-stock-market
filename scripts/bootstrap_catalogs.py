#!/usr/bin/env python3
"""Create the three environment catalogs, their schemas, and their volumes.

Idempotent by construction (`IF NOT EXISTS` throughout), so it is safe to re-run and safe to run
from CI. Bootstrap scripts that can only be run once are how environments drift: the second person
to join the project runs a slightly different sequence by hand.

Why SQL rather than the Unity Catalog REST API
-----------------------------------------------
`POST /api/2.1/unity-catalog/catalogs` (and `databricks catalogs create`, and a bundle
`resources.catalogs` entry, which calls the same endpoint) fails on an account with Default
Storage enabled:

    400 INVALID_STATE: Metastore storage root URL does not exist. Default Storage is enabled in
    your account. You can use the UI to create a new catalog using Default Storage, or please
    provide a storage location for the catalog.

The REST path requires an explicit `storage_root`, which a Free Edition account does not have and
cannot create. The SQL DDL path resolves Default Storage automatically. This is not documented
prominently and costs an afternoon if you assume the API and the SQL surface are equivalent.
See docs/decision-log.md (DL-003, DL-004, DL-005).

Usage:
    python scripts/bootstrap_catalogs.py                    # all environments
    python scripts/bootstrap_catalogs.py --env dev
    python scripts/bootstrap_catalogs.py --dry-run          # authenticate, print the SQL, execute nothing

Authentication is the Databricks SDK's default chain (~/.databrickscfg profile, or
DATABRICKS_CONFIG_PROFILE to pick one). Nothing here reads or stores a token.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Environments are catalogs, not workspaces: Free Edition allows one metastore.
# See docs/adr/ADR-0001-environment-isolation.md for why this is a design and not a workaround.
ENVIRONMENTS = ("dev", "test", "prod")
CATALOG_PREFIX = "databricks_stock_market"
PROJECT_TAG = "stock-market"

SCHEMAS: dict[str, str] = {
    "bronze": (
        "Raw ingest. Auto Loader streaming tables over the landing volume: append-only, "
        "source-faithful, no business logic. Never queried by BI."
    ),
    "silver": (
        "Conformed entities: typed, deduplicated on (ticker, date) or (ticker, extraction_date), "
        "SCD Type 2 for company metadata. Only rows that passed every error-severity quality rule."
    ),
    "gold": (
        "Star schema and analytical views (fact_prices, dim_companies, fact_company_metrics, "
        "v_companies_performance, v_fact_prices_converted). The only layer consumers should read."
    ),
    "ops": (
        "Audit and observability: quality quarantine tables (rows rejected by an error rule, with "
        "the rules they failed), row conservation and other audit datasets, and pipeline "
        "observability. Per environment on purpose, so dev runs cannot pollute prod audit data."
    ),
}

VOLUMES: dict[str, tuple[str, str]] = {
    # volume name -> (schema, comment)
    "landing": (
        "bronze",
        "Landing zone written by the yfinance extractors (subfolders prices/ and metadata/) and "
        "read by Auto Loader. Files stay here; Auto Loader checkpoints track what was ingested.",
    ),
    "checkpoints": (
        "ops",
        "Auto Loader schema locations and streaming checkpoints. Deleting one silently "
        "reprocesses its source from scratch.",
    ),
}


def catalog_name(env: str) -> str:
    """Return the catalog for an environment.

    Args:
        env: One of ENVIRONMENTS.

    Returns:
        The environment catalog name.
    """
    return f"{CATALOG_PREFIX}_{env}"


def sql_string(value: str) -> str:
    """Quote a value as a SQL string literal, doubling embedded single quotes.

    Args:
        value: Raw text, e.g. a comment.

    Returns:
        The quoted literal.
    """
    return "'" + value.replace("'", "''") + "'"


@dataclass
class Runner:
    """Executes SQL through the Statement Execution API, or prints it in dry-run mode."""

    client: WorkspaceClient
    warehouse_id: str
    dry_run: bool = False

    def sql(self, statement: str) -> None:
        """Run one statement and fail loudly if it does not succeed.

        Args:
            statement: SQL to execute.

        Raises:
            RuntimeError: If the statement does not reach the SUCCEEDED state.
        """
        if self.dry_run:
            print(f"  [dry-run] {statement}")
            return
        result = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=statement,
            wait_timeout="50s",
        )
        state = result.status.state if result.status else None
        if state != StatementState.SUCCEEDED:
            message = result.status.error.message if result.status and result.status.error else "?"
            raise RuntimeError(f"FAILED ({state}): {statement}\n  {message}")
        print(f"  ok  {statement.splitlines()[0][:96]}")


def resolve_warehouse(client: WorkspaceClient) -> str:
    """Pick the first available SQL warehouse.

    Free Edition allows exactly one, so 'first' is unambiguous here. On a paid tier this would be
    a required parameter rather than a guess: silently picking a warehouse is how a bootstrap ends
    up billed to the wrong team.

    Args:
        client: Authenticated workspace client.

    Returns:
        The warehouse id.

    Raises:
        RuntimeError: If the workspace has no SQL warehouse.
    """
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse available. Start one in the workspace UI first.")
    if len(warehouses) > 1:
        names = ", ".join(w.name or "?" for w in warehouses)
        print(f"WARNING: {len(warehouses)} warehouses found ({names}); using the first.")
    chosen = warehouses[0]
    print(f"warehouse: {chosen.name} ({chosen.cluster_size}, {chosen.state})")
    if chosen.id is None:
        raise RuntimeError(f"Warehouse {chosen.name!r} has no id.")
    return chosen.id


def bootstrap_statements(env: str) -> list[str]:
    """Build every DDL statement for one environment, in execution order.

    Kept separate from execution so the exact SQL is unit-testable without a workspace.

    Args:
        env: One of ENVIRONMENTS.

    Returns:
        The statements: catalog, schemas, volumes, then catalog tags.
    """
    catalog = catalog_name(env)
    catalog_comment = (
        f"Stock market pipeline, {env} environment. Managed by scripts/bootstrap_catalogs.py; "
        "do not create objects here by hand."
    )
    statements = [f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT {sql_string(catalog_comment)}"]
    statements += [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT {sql_string(comment)}"
        for schema, comment in SCHEMAS.items()
    ]
    statements += [
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} COMMENT {sql_string(comment)}"
        for volume, (schema, comment) in VOLUMES.items()
    ]
    # Tags make the environment queryable from system tables, which is what makes a
    # "which objects belong to prod?" question answerable without a naming convention.
    statements.append(
        f"ALTER CATALOG {catalog} SET TAGS ('environment' = {sql_string(env)}, 'project' = {sql_string(PROJECT_TAG)})"
    )
    return statements


def bootstrap_environment(runner: Runner, env: str) -> None:
    """Create one environment's catalog, schemas, volumes and tags.

    Args:
        runner: Statement runner.
        env: One of ENVIRONMENTS.
    """
    print(f"\n=== {catalog_name(env)} ===")
    for statement in bootstrap_statements(env):
        runner.sql(statement)


def verify(client: WorkspaceClient, envs: list[str]) -> int:
    """Check that every expected catalog, schema and volume exists.

    Args:
        client: Authenticated workspace client.
        envs: Environments to verify.

    Returns:
        The number of problems found; 0 means everything exists.
    """
    print("\n=== verification ===")
    existing = {c.name for c in client.catalogs.list()}
    problems = 0
    for env in envs:
        catalog = catalog_name(env)
        if catalog not in existing:
            print(f"  MISSING catalog {catalog}")
            problems += 1
            continue

        schemas = {s.name for s in client.schemas.list(catalog_name=catalog)}
        missing_schemas = sorted(set(SCHEMAS) - schemas)
        if missing_schemas:
            print(f"  {catalog}: missing schemas {missing_schemas}")
            problems += 1

        missing_volumes = []
        for volume, (schema, _) in VOLUMES.items():
            if schema not in schemas:
                missing_volumes.append(f"{schema}.{volume}")
                continue
            volumes = {v.name for v in client.volumes.list(catalog_name=catalog, schema_name=schema)}
            if volume not in volumes:
                missing_volumes.append(f"{schema}.{volume}")
        if missing_volumes:
            print(f"  {catalog}: missing volumes {missing_volumes}")
            problems += 1

        if not missing_schemas and not missing_volumes:
            print(f"  {catalog}: {len(SCHEMAS)} schemas and {len(VOLUMES)} volumes ok")
    return problems


def main() -> int:
    """Parse arguments, then bootstrap and verify.

    Returns:
        Process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=ENVIRONMENTS, help="bootstrap a single environment")
    parser.add_argument("--dry-run", action="store_true", help="print statements, execute nothing")
    args = parser.parse_args()

    # A dry run still authenticates and resolves the warehouse, so it proves the credentials and
    # the warehouse work before anything is created. It just does not execute the statements.
    client = WorkspaceClient()
    runner = Runner(client=client, warehouse_id=resolve_warehouse(client), dry_run=args.dry_run)

    envs = [args.env] if args.env else list(ENVIRONMENTS)
    for env in envs:
        bootstrap_environment(runner, env)

    if args.dry_run:
        return 0
    return 1 if verify(client, envs) else 0


if __name__ == "__main__":
    sys.exit(main())

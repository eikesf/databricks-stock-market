# ADR-0001 — Catalog-per-environment on a single metastore

**Status:** Accepted · **Date:** 2026-10-14 · **Reversal cost:** High

Adapted from ADR-0002 ("Catalog-per-environment on a single metastore") in Daniel Rocha's
[`databricks-retail-lakehouse`](https://github.com/danielnrocha/databricks-retail-lakehouse), which
faced the same Free Edition constraints. The structure and most of the reasoning are his; the
specifics (the stock market domains, `ticker_limit`, the bootstrap path, the `ops` contents) are
this project's.

---

## Context

The pipeline (yfinance → Landing → Bronze → Silver → Gold, domains prices, metadata and metrics) is
moving from Airflow, ClickHouse and path-based Delta on Docker to Databricks Free Edition with
Unity Catalog, Databricks Asset Bundles and Lakeflow Declarative Pipelines. It needs three
environments, `dev`, `test` and `prod`, with real separation: a broken dev run must not be able to
write prod data, and what is deployed to prod must be the artifact that passed test.

Free Edition sets the boundary conditions, and they are not preferences:

- **One workspace and one metastore per account**, no account console, no service principals.
- **One SQL warehouse**, and a **daily compute quota shared by everything**. Exceeding it shuts
  down all compute for the rest of the day, prod included.
- **One active Lakeflow pipeline per pipeline type per account**, so dev and prod pipeline runs
  cannot overlap.
- **Catalogs cannot be created through the Unity Catalog REST API** on an account with Default
  Storage (`400 INVALID_STATE: Metastore storage root URL does not exist`). This is the endpoint a
  bundle `resources.catalogs` entry calls, so catalogs cannot be bundle resources here
  (DL-003, DL-004).

## Options considered

### A. Workspace-per-environment

Three workspaces with catalogs bound to them. The strongest isolation: a dev identity cannot even
authenticate against prod.

**Rejected:** impossible on Free Edition (one workspace). Even where available it multiplies the
surface to keep in sync, and workspace drift is a classic source of "works in dev, fails in prod".

### B. Schema-per-environment inside one catalog

`stock_market.bronze_dev`, `stock_market.bronze_prod`, and so on.

**Rejected.** The catalog is Unity Catalog's natural grant boundary; encoding the environment in
schema names turns access control into a naming convention. It also puts two axes (environment ×
layer) into one name segment, so every table name is string-built, which is exactly how a dev
notebook ends up writing to prod after someone edits a variable.

### C. Catalog-per-environment on the shared metastore ✅

`databricks_stock_market_dev`, `databricks_stock_market_test`, `databricks_stock_market_prod`, each
with the same `bronze` / `silver` / `gold` / `ops` schemas. The bundle target injects the catalog;
code never names one.

**Chosen.**

## Decision

Three catalogs, created by `scripts/bootstrap_catalogs.py` with SQL DDL before a target's first
deploy (`make bootstrap ENV=<env>`), identical in layout:

```
databricks_stock_market_dev    databricks_stock_market_test    databricks_stock_market_prod
├── bronze                     ├── bronze                      ├── bronze      volume: landing (prices/, metadata/)
├── silver                     ├── silver                      ├── silver
├── gold                       ├── gold                        ├── gold
└── ops                        └── ops                         └── ops         volume: checkpoints; quarantine, row conservation
```

Each catalog is tagged `environment=<env>` and `project=stock-market`, so "which objects belong to
prod?" is answerable from system tables rather than from a naming convention.

The catalog name reaches code by exactly one route:

```
bundle target (databricks.yml) → var.catalog → pipeline/job configuration → spark.conf → code
```

No module in `src/` reads an environment variable to discover its catalog, and no module contains
a literal catalog name. `tests/unit/test_deploy_provenance.py` checks the bundle end: every target
declares the catalog matching its name, `var.catalog` has no default, `test` and `prod` are
production mode, and no pipeline pins `development`. The code end gets a test that fails on a
literal catalog name in `src/` when the code is refactored to read the catalog (DL-007), because it
is the rule that erodes under deadline pressure.

**`ops` is per environment.** It holds the quality quarantine tables (rows rejected by an
error-severity rule, with the rules they failed), `ops.row_conservation` and the other audit
datasets, and pipeline observability. A shared `ops` schema would let dev runs pollute prod audit
data and dashboards.

**`ticker_limit` is the dev cost lever.** Dev runs with `ticker_limit: 10` (test and prod: 0, no
limit); the code that honours it comes with the producer refactor. It shrinks the *input*, not a row sample: a sampled price series breaks every window-based
metric, while 10 complete series exercise the same code as 500. It protects the shared daily quota
(and therefore prod) and keeps the extractors well under yfinance rate limits, which are per IP and
would otherwise be spent by dev before prod's scheduled run.

## Consequences

**Positive**

- Grants are simple and auditable: `GRANT USE CATALOG ON CATALOG databricks_stock_market_prod TO <group>`.
- Promotion is a bundle target switch, not a code change, so what is tested is what deploys.
- It is the pattern Databricks documents for single-metastore organisations: a legitimate design
  that Free Edition happens to force, not a workaround passed off as one.
- Re-running the bootstrap is a no-op, so a new environment (or a rebuilt one) is one command.

**Negative — stated plainly**

- Isolation is *logical*, not *physical*. Anyone with full metastore privileges reaches every
  environment. On a paid tier this would be closed with catalog-workspace bindings and a service
  principal per environment; neither exists on Free Edition.
- Compute is shared. A runaway dev job spends prod's quota. `ticker_limit` reduces the risk; it
  does not remove it.
- One active pipeline per type means dev and prod pipeline updates must be serialised. That is
  scheduled around (prod at fixed times, dev manual), not engineered around.
- Catalog creation is outside the bundle (DL-004), so a target's first deploy has a manual
  prerequisite (`make bootstrap ENV=<env>`). `bundle destroy` cannot drop a catalog, which is a
  benefit, but it also means the bundle does not describe the whole environment.
- No service principal means CI cannot deploy without a long-lived personal token (#9).

## Reversal cost: High

Changing the isolation model once tables exist means moving data between catalogs (a full copy;
there is no lineage-preserving catalog rename), rewriting every grant, and invalidating every
dashboard, metric view and query that uses three-part names. That is why this is the first ADR.

## Revisit trigger

Move to workspace-per-environment if any of these becomes true:

- The project moves to a paid tier **and** more than one person deploys.
- A requirement demands physical separation of prod data.
- Quota contention between dev and prod causes a missed prod run more than once.

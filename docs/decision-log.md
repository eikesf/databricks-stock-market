# Decision Log — Stock Market Pipeline (Databricks migration)

This file records every technical decision taken in this project: what forced it, what was chosen, what was rejected and why, and what it costs. Each entry also has a content angle so it can be turned into a LinkedIn post or poll later. Newest entries go at the top.

| ID | Date | Decision | Status |
|---|---|---|---|
| DL-009 | 2026-10-23 | Schemas and volumes created by the bootstrap script, not as bundle resources; `landing` schema replaced by `bronze.landing` volume, `ops` added | Accepted |
| DL-008 | 2026-10-14 | Soda Core replaced by native Lakeflow expectations; quarantine routed to `ops` | Accepted (implementation pending plan approval) |
| DL-007 | 2026-10-14 | Code receives the catalog via pipeline/job configuration; path constants replaced by UC table names | Accepted (implementation deferred to the producer/pipeline refactor) |
| DL-006 | 2026-10-14 | Bronze ingestion moves to Auto Loader; archive/replay retired | Accepted (implementation pending plan approval) |
| DL-005 | 2026-10-14 | Bootstrap script in Python (Databricks SDK) executing SQL DDL | Accepted |
| DL-004 | 2026-10-14 | Catalogs created via SQL DDL, not as bundle resources | Accepted |
| DL-003 | 2026-10-14 | Attempted to manage catalogs as `resources.catalogs` in the bundle | Superseded by DL-004 |
| DL-002 | 2026-10-14 | Databricks Asset Bundles with dev/test/prod targets, one catalog per environment | Accepted |
| DL-001 | before 2026-10-14 | Full migration to Databricks Free Edition, ClickHouse replaced by Delta tables | Accepted |

---

## DL-009 — Schemas and volumes created by the bootstrap script, not as bundle resources

**Date:** 2026-10-23 · **Status:** Accepted

**Context.** Issues #2 and #3 planned the schemas (`landing`, `bronze`, `silver`, `gold`) and a `landing.raw` volume as bundle resources in `resources/schemas.yml` and `resources/volumes.yml`. DL-004 moved catalog creation out of the bundle and left schemas open: they might work as `resources.schemas` because they inherit the catalog's storage, but that was never tested on Free Edition. Meanwhile ADR-0001 settled the per-environment layout as `bronze` / `silver` / `gold` / `ops`, and DL-006 placed the landing files where Auto Loader reads them.

**Decision.** Every Unity Catalog container (catalog, schemas, volumes) is created by `scripts/bootstrap_catalogs.py` with idempotent SQL DDL. The bundle creates only compute and code objects (pipelines, jobs) and references containers through `${var.catalog}`. Layout:

- No `landing` schema. Landing files go to the `bronze.landing` volume, subfolders `prices/` and `metadata/` (`/Volumes/<catalog>/bronze/landing/{prices,metadata}/`). Raw files are Bronze's input, so they live in Bronze's schema.
- An `ops` schema per environment for quarantine and audit tables (DL-008, ADR-0001), plus an `ops.checkpoints` volume.

**Spike (2026-10-23).** A throwaway `resources.schemas` entry (`dab_spike`) deployed to dev succeeded on Free Edition (`Created schemas.dab_spike`), unlike `resources.catalogs` (DL-004): a schema inherits its catalog's Default Storage. In development mode the bundle created it as `dev_eikefranco_dab_spike`, so bundle-managed schemas would not have the same names in dev as in test and prod. The spike was destroyed afterwards. Bundle-managed schemas are therefore possible; this entry rejects them by choice, not by necessity.

**Alternatives considered.** Schemas and volumes as bundle resources (the original plan). Rejected for three reasons. It splits one environment's containers across two tools, so recreating an environment means running the script and a deploy in the right order. Bundle-managed containers are removed by `bundle destroy`, while the catalog they live in is not, which makes the data lifecycle inconsistent. And development mode adds the user prefix to schema names (confirmed by the spike: `dev_eikefranco_<name>`), which would give the dev catalog differently named schemas from test and prod and break the rule that all three are structurally identical (ADR-0001). Moving the existing schemas into the bundle would also require `databricks bundle deployment bind` for each one in test and prod, because a bundle does not adopt objects that already exist.

**Consequences.** The bootstrap must run before the first deploy of each target (already true for the catalog). A new schema or volume is a change to the script plus a re-run, not a deploy. Grants, if ever needed, go into the script too. If a future need makes bundle-managed schemas attractive (for example, grants declared next to the jobs that need them), supersede this entry; the spike shows the mechanism works, so the open questions are only the dev naming and binding the existing schemas.

**Content angle.** "Infrastructure that holds data and infrastructure that runs code have different lifecycles. Put them in different tools." Poll idea: do you manage Unity Catalog schemas in your deployment bundle or outside it?

---

## DL-008 — Soda Core replaced by native Lakeflow expectations; quarantine routed to `ops`

**Date:** 2026-10-14 · **Status:** Accepted (implementation pending plan approval)

**Context.** Data quality today has two layers: row-level rules in `src/streaming/quality_rules.py` that split each Silver batch into valid and `*_rejected` tables before the write, and Soda Core contracts (`soda/contracts/*.yml`) that re-check Silver and Gold after the write. Since the row-level gate was added, the Soda checks mostly re-assert bounds the gate already enforced. Soda also needs its own connections (a Spark session for Silver, ClickHouse's MySQL port for Gold), and both of those targets disappear in the migration.

**Decision.** Use native Lakeflow expectations, keeping `quality_rules.py` as the source of truth for *what* is checked and changing only the mechanism:
- `error` rules route the whole row to a quarantine table in the `ops` schema (`ops.prices_quarantine`, `ops.metadata_quarantine`, `ops.metrics_quarantine`) carrying the failed rule names, the rule expressions, `failed_at` and a `ruleset_version`. No row is ever silently dropped; `expect_or_drop` is not used.
- `warn` rules (today's Soda WARN checks) become `dp.expect_all`: counted in the event log, rows pass.
- A null `ticker` or domain date (today's `RequiredColumnMissingError`) becomes `dp.expect_or_fail`.
- Row conservation per domain becomes an `ops.row_conservation` dataset with `dp.expect_or_fail` asserting `bronze_rows = duplicates_collapsed + passed + quarantined` (the plan proposes an extra term for non-trading-day rows; see the plan).
- Non-trading-day rows with all-null OHLC stay a plain filter: they are source shape, not a quality failure.
- Rules start inline in the pipeline source tree; they move to an `ops.dq_rules` table only if a pipeline cannot import a shared rules module, which is verified rather than assumed.

The full rule-by-rule mapping (every rule in `quality_rules.py` and every Soda check) is in `docs/plans/lakeflow-migration.md` §2.

**Alternatives considered.**
- *Keep Soda.* A second system re-checking what the pipeline already enforces, with its own connections and contract files to keep in sync with the rules module. Its post-write checks can only fail a run after bad data is already written.
- *DQX (Databricks Labs).* Not a core product. Its main draw is the profiler, which fits ranges to a sample, so unreviewed candidates can quarantine valid rows. This project's rules are domain-authored, so DQX adds little beyond another dependency.
- *`expect_or_drop`.* Silent loss: the row disappears and only a count remains. Rejected; quarantine keeps the row and the reason.

**Consequences.** One quality mechanism instead of two, visible in the pipeline UI and event log. Expectations exist only inside Lakeflow pipelines, so data quality is now coupled to that choice: moving off Lakeflow means re-implementing the gate. Set-level checks that are not row expectations (uniqueness, completeness percentages, freshness, referential coverage) move to an `ops.dataset_health` view with expectations on it. Several Soda checks change severity (freshness from fail to warn, exchange/currency allow-lists from fail to warn); each is flagged for decision in the plan. Lakeflow treats a rule that evaluates to NULL as passing, unlike some of today's rules, so presence rules must be written to evaluate FALSE on NULL and are pinned by tests.

**Content angle.** "I deleted my data quality tool and my data quality got stricter." Or a poll: where do your quality checks live — in the pipeline, after the pipeline, or in a separate tool?

---

## DL-007 — Code receives the catalog via pipeline/job configuration; path constants replaced by UC table names

**Date:** 2026-10-14 · **Status:** Accepted (implementation deferred to the producer/pipeline refactor)

**Context.** `src/producer/config.py` builds every location from a `DATA_DIR` environment variable (`data/landing/prices`, `data/bronze/...`) and created the directories on import. On Databricks there is no local data directory: tables are Unity Catalog names in a per-environment catalog (DL-002), and files live in volumes. Any route by which code *discovers* its catalog (an environment variable, a default, a literal) is a route by which a dev run can write to prod.

**Decision.** The catalog reaches code by exactly one route: bundle target → `var.catalog` → pipeline/job configuration → `spark.conf` (or a task argument) → code. No literal catalog name and no environment-variable lookup in `src/`. The infrastructure step only puts the bundle end in place (`var.catalog` with no default, one catalog per target, checked by `tests/unit/test_deploy_provenance.py`). The code end (replacing the `DATA_DIR` path constants with Unity Catalog names, extractors writing to the landing volume, a dev ticker cap, and a test that fails on a literal catalog name in `src/`) is done when the producer and the layers are refactored to the Databricks pattern, not before.

**Alternatives considered.** An environment variable (`CATALOG=...`) was rejected: it is invisible in the bundle, has to be set separately per job, and a missing value tends to get a default, which is a guess about where to write. A default catalog in code was rejected for the same reason. Building names in each module was rejected in favour of one helper so the layout cannot drift between modules.

**Consequences.** Code cannot run without being told its catalog, which is the point; local runs pass it explicitly. For plain-Python tasks such as the extractors the route ends in a task argument rather than `spark.conf`, because they have no Spark session; the rule that matters (no literal, no environment variable) still holds. Until the refactor, `src/` still uses the local `DATA_DIR` paths.

**Content angle.** "The most dangerous line in a multi-environment pipeline is a default value." Short post on why the catalog has no default.

---

## DL-006 — Bronze ingestion moves to Auto Loader; archive/replay retired

**Date:** 2026-10-14 · **Status:** Accepted (implementation pending plan approval)

**Context.** Bronze ingestion today is hand-built: `ingest_landing_to_bronze` reads one landing file per execution date, appends it to a path-based Delta table, and moves the file to `data/archive/` so it is not ingested twice. Because the Delta tables sat on a macOS Docker bind mount that does not guarantee durable writes, corrupted parquet files appeared, which led to a self-healing layer (`check_and_heal_corrupt_data_file`, checkpoint repair, `RESTORE` to the version before the damaged file) and an archive replay (`recover_bronze_from_archive`, `find_archive_dates`) to rebuild Bronze after a rollback. That is several hundred lines guarding against a storage problem that does not exist on Databricks managed storage.

**Decision.** Bronze becomes two Auto Loader streaming tables (`bronze.prices_raw`, `bronze.metadata_raw`) inside a Lakeflow Declarative Pipeline, reading from the landing volume `/Volumes/<catalog>/bronze/landing/{prices,metadata}/`. Auto Loader's checkpoint records which files were ingested, so files stay in landing and are never moved; the archive folder, the replay logic, `infer_execution_date` and the Delta self-heal helpers in `src/streaming/utils.py` are retired. In a Lakeflow pipeline the schema and checkpoint locations are managed by the pipeline itself (per the Auto Loader documentation), so none are configured by hand. Implementation and the exact deletion order are in `docs/plans/lakeflow-migration.md`.

**Alternatives considered.** Porting the archive/replay logic to volumes was rejected: it re-implements, with less testing, what the Auto Loader checkpoint already guarantees. `COPY INTO` from a scheduled job was rejected because it is not part of the pipeline graph, so lineage and expectations would stop at Bronze. Keeping the self-heal layer "just in case" was rejected: it targets a failure mode (bind-mount write loss) that managed storage does not have, and unexercised recovery code is a liability.

**Consequences.** Exactly-once file ingestion comes from the platform instead of from code. Rebuilding Bronze is a full refresh that re-reads every landing file, which only works because files are never moved or deleted; landing retention therefore becomes a decision of its own. Deleting the pipeline's state (or the pipeline) means the next run re-ingests everything; Silver's deduplication makes that safe but not free. Bronze rows gain `_source_file` lineage from Auto Loader's file metadata.

**Content angle.** "300 lines of self-healing code, deleted. The best recovery logic is the one your platform makes unnecessary." Good for a before/after post.

---

## DL-005 — Bootstrap script in Python (Databricks SDK) executing SQL DDL

**Date:** 2026-10-14 · **Status:** Accepted

**Context.** DL-004 moved catalog creation out of the bundle, so something has to create the catalogs (and their schemas) before the first deploy of each target. That something should be code in the repo, not clicks in the UI, so the environments are reproducible.

**Decision.** A `scripts/bootstrap_catalogs.py` that uses the Databricks Python SDK (`WorkspaceClient` + Statement Execution API against the SQL warehouse) to run idempotent SQL DDL (`CREATE CATALOG IF NOT EXISTS`, `CREATE SCHEMA IF NOT EXISTS`). Python is the runner, SQL is the payload. The script loops over the environments, supports `--env` and `--dry-run`, and verifies at the end that every catalog and schema exists.

Implemented as `make bootstrap ENV=<env>` / `make bootstrap-dry ENV=<env>`. Beyond the original proposal it also creates the `landing` and `checkpoints` volumes, tags each catalog `environment=<env>` and `project=stock-market`, and verifies volumes as well as schemas. `--dry-run` authenticates and resolves the warehouse like a real run, so it proves the credentials and the warehouse before anything is created; it only skips executing the statements. There is no `--drop`: removing a catalog is a deliberate one-off in the SQL editor, not something a bootstrap script should offer.

**Alternatives considered.** A plain `.sql` file was rejected because it has no runner of its own: it can only be pasted into the SQL editor, it cannot be called from the CLI or CI, and the catalog name would be hardcoded once per environment, which is exactly the drift the bundle variables exist to prevent. A workspace notebook using `spark.sql` was rejected as the primary path because it needs compute to start and lives in the workspace rather than running from the terminal or CI, which matters on Free Edition where compute quota is shared across all environments. The Unity Catalog REST API (`catalogs.create`) is ruled out by DL-004.

**Consequences.** One command recreates every environment from scratch. The script must stay idempotent so it is safe to re-run. It needs a running SQL warehouse; Free Edition allows one, so picking it is unambiguous.

**Content angle.** "Python or SQL for infrastructure bootstrap? Both. SQL says what the environment is, Python decides when and where it runs." Poll idea: how do you create your Unity Catalog catalogs — UI, SQL, Terraform, or bundles?

---

## DL-004 — Catalogs created via SQL DDL, not as bundle resources

**Date:** 2026-10-14 · **Status:** Accepted

**Context.** The first `deploy` on the dev target failed with `400 INVALID_STATE: Metastore storage root URL does not exist. Default Storage is enabled in your account.` The bundle creates `resources.catalogs` through `POST /api/2.1/unity-catalog/catalogs`. On an account with Default Storage (Free Edition), that endpoint requires an explicit `storage_root`, which a Free Edition account does not have and cannot create. Omitting `storage_root` does not fall back to Default Storage on this path.

**Decision.** Remove `resources/catalogs.yml` from the bundle. Create the catalogs with SQL DDL, which resolves Default Storage automatically. The bundle only references the catalog through `${var.catalog}` and assumes it exists.

**Alternatives considered.** Passing a `storage_root` was not possible on Free Edition. Creating catalogs by hand in the UI works but cannot be scripted or reviewed, so environments would drift.

**Consequences.** Catalog creation becomes a bootstrap step that runs before the first deploy of each target (see DL-005). Because the catalog is no longer a bundle resource, `bundle destroy` cannot drop it, so the data protection that `lifecycle.prevent_destroy` was meant to provide now comes from the design itself. Schemas may still work as `resources.schemas` since they inherit the catalog's storage; not yet verified on Free Edition. If they fail the same way, they move into the bootstrap script too.

**Content angle.** "The Unity Catalog REST API and SQL are not the same surface. On Default Storage, one fails and the other works, and the error message sends you to the UI, the only option you can't automate." Strong hook for a short troubleshooting post.

---

## DL-003 — Attempted to manage catalogs as `resources.catalogs` in the bundle

**Date:** 2026-10-14 · **Status:** Superseded by DL-004

**Context.** The goal was for one `deploy` per target to create everything that target needs, including its catalog, so dev, test and prod are fully declared in code.

**Decision (original).** A `resources/catalogs.yml` declaring one catalog resource named `${var.catalog}`, with no `storage_root` (assuming Free Edition would use Default Storage), `lifecycle.prevent_destroy: true` to stop `bundle destroy` from dropping a catalog full of tables, and `bundle.engine: direct` set in `databricks.yml` to enable catalog resources.

**Why it was dropped.** The deploy failed on the dev target with the 400 error described in DL-004. The design intent was right; the API underneath it does not support Default Storage.

**Lesson.** Check which API a declarative resource actually calls before assuming it behaves like the SQL equivalent. Declarative tooling inherits the limits of the endpoint it wraps.

**Content angle.** A "what I tried first and why it failed" post. Shows debugging reasoning, which reads more senior than a clean success story.

---

## DL-002 — Databricks Asset Bundles with dev/test/prod targets, one catalog per environment

**Date:** 2026-10-14 · **Status:** Accepted

**Context.** The pipeline needs separate dev, test and prod environments. Free Edition gives one workspace and one metastore, so separate workspaces per environment are not an option.

**Decision.** Use Databricks Asset Bundles with three targets. Each target sets `var.catalog` (for example `databricks_stock_market_dev`), and that variable is the only route by which code learns where to write. No literal catalog name appears in the source code.

**Alternatives considered.** Workspace-per-environment is impossible on Free Edition. Schema-per-environment inside one catalog was rejected because isolation and grants are cleaner at the catalog level, and it mirrors the catalog-per-environment pattern used on single-metastore setups.

**Consequences.** Isolation is logical, not physical: anyone with full metastore privileges can reach every environment. Compute is shared, so a heavy dev run consumes the same daily quota as prod. Promotion between environments becomes a target switch instead of a code change.

**Content angle.** "Three environments on a free tier: what's real isolation and what's just naming."

---

## DL-001 — Full migration to Databricks Free Edition, ClickHouse replaced by Delta tables

**Date:** decided before 2026-10-14 (logged retroactively) · **Status:** Accepted

**Context.** The original pipeline served the Gold layer from ClickHouse, an external service alongside the processing stack.

**Decision.** Move the whole medallion architecture (Bronze, Silver, Gold) into Databricks and replace ClickHouse with Delta tables, so the project runs on one platform with no external services.

**Consequences.** One platform to operate, govern and explain in interviews. ClickHouse's query speed for serving is traded for simplicity and Unity Catalog governance over every layer.

**Content angle.** "Why I removed a database from my own pipeline."

---

## Template for new entries

```
## DL-00X — <decision in one line>

**Date:** YYYY-MM-DD · **Status:** Proposed | Accepted | Superseded by DL-00Y

**Context.** What forced the decision. Paste the exact error if there was one.

**Decision.** What was chosen, concretely (files, commands, config).

**Alternatives considered.** What was rejected and the specific reason for each.

**Consequences.** What this costs or limits, stated plainly, and what to verify later.

**Content angle.** The hook for a LinkedIn post or poll.
```

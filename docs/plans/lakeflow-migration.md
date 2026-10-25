# Plan: Lakeflow Declarative Pipeline conversion

**Status:** Draft, awaiting approval. No pipeline code has been written and no module has been
deleted. · **Date:** 2026-10-14 · **Related:** DL-006, DL-007, DL-008 in `docs/decision-log.md`,
`docs/adr/ADR-0001-environment-isolation.md`, epic #55 (milestones M1–M5).

Items marked **[verify]** could not be confirmed from documentation or on this machine and need a
check on the Free Edition workspace before (or while) the code is written. Items marked
**[decide]** need your call before implementation.

**Scope note.** Parts of this plan assume a producer refactor that was drafted and then taken out
of the infrastructure step: `src/stock_market/catalog.py` as a Unity Catalog name builder
(`CatalogNames`), the old path constants moved to `src/streaming/legacy_paths.py`, extractors
taking `--landing-path` / `--max-tickers`, `var.landing_path` / `var.max_tickers` in the bundle,
and `tests/unit/test_no_hardcoded_catalog.py`. `CatalogNames` now exists (issue #5), covering the
Bronze tables and the landing volume, and its hardcoded-catalog check lives in
`tests/unit/test_catalog.py`. The rest of that list is still pending (the bundle still has
`ticker_limit`). It becomes the first step of the conversion, and will be redesigned alongside
the move from Airflow to Lakeflow jobs.

---

## 1. Pipeline layout

### Recommendation: one pipeline per target, Bronze → Silver → Ops → Gold in the same graph

```
landing volume                bronze                silver                       ops                           gold (materialized views)
/Volumes/<c>/bronze/landing
  prices/   ──Auto Loader──▶  prices_raw  ──conform──▶ prices (AUTO CDC SCD1)    prices_quarantine            fact_prices
                                          └─gate───▶                               row_conservation             v_fact_prices_converted
  metadata/ ──Auto Loader──▶  metadata_raw ─conform─▶ dim_companies (AUTO CDC    metadata_quarantine          dim_companies, v_companies_active
                                           │          SCD2)                       metrics_quarantine           fact_company_metrics
                                           └─────────▶ company_metrics (AUTO CDC  dataset_health               v_companies_performance
                                                       SCD1)
```

One serverless pipeline (`stock_market_lakehouse`) holds every domain and every layer, triggered
(not continuous). Three Lakeflow Jobs replace the three Airflow DAGs; each job is
`extract task (Python) → pipeline refresh task`:

| Job | Schedule (prod only) | Extract task | Replaces |
|---|---|---|---|
| `daily_prices` | Mon–Fri 22:00 UTC | `src.producer.generator` | `dag_stock_prices` (#37) |
| `weekly_metrics` | weekly | `src.producer.metadata_generator` | `dag_stock_metrics` (#39) |
| `monthly_metadata` | monthly | `src.producer.metadata_generator` | `dag_stock_metadata` (#38) |

`dag_delta_maintenance` is not replaced by a job: managed tables get predictive optimization (#42).

**Why one pipeline rather than one per domain or a separate Gold job**

- **Free Edition allows one active pipeline per type per account.** Three pipelines would contend
  for that slot and fail rather than queue; one pipeline per target means the only contention left
  is dev vs prod, which is scheduled around (prod runs at fixed times; dev is manual).
- **Streaming tables make an "everything" refresh cheap.** Auto Loader only processes files it has
  not seen, so the daily prices refresh reads one new prices file and zero metadata files; the
  metadata/metrics flows are no-ops that day. The Gold materialized views recompute, but they are
  small (hundreds of tickers).
- **Gold in the same graph** keeps lineage end to end, lets Gold carry expectations, and avoids a
  second compute start. A separate Gold job would need either a second pipeline (blocked by the
  one-active-pipeline limit) or SQL warehouse tasks (the single warehouse is also what the
  bootstrap and BI use).
- **Cost of this choice:** the metadata and metrics extract run on different cadences but share
  one landing folder (`metadata/`), and both silver tracks read `metadata_raw`. That is how the
  current code works too (one metadata extraction feeds both tracks), so behaviour is unchanged.
  **[decide]** whether the weekly job should keep re-extracting the full company profile (today's
  behaviour) or whether metrics get their own landing subfolder later.

**[verify]** Whether a job can trigger a refresh of only some datasets (`refresh_selection`) from a
`pipeline_task`. Not needed for the design above, but it would let the weekly job skip the prices
Gold views.

### Bronze (M1, #15, #16)

Two Auto Loader streaming tables over the landing volume subfolders, written by the existing
extractors (once refactored to write to `var.landing_path`, see the scope note):

- `bronze.prices_raw` from `landing/prices/*.parquet`
- `bronze.metadata_raw` from `landing/metadata/*.parquet`

Each adds `_source_file` and `_source_file_modified_at` from the `_metadata` column and
`_ingested_at = current_timestamp()`. No business logic, no casts beyond what Auto Loader infers.
Schema evolution mode `addNewColumns` (fail-and-restart on a new column) rather than `rescue`,
so a yfinance schema change is loud. **[verify]** that `_metadata.file_path` is exposed for
Parquet sources inside a pipeline on serverless.

### Silver (M2, #17–#26)

Per domain, the order is conform → gate → deduplicate:

1. **Conform** (a pure function `DataFrame -> DataFrame` per domain, importable without a pipeline
   so it can be unit tested): the casts, ticker normalisation (`upper(trim())`, `USDBRL=X → USDBRL`),
   exchange standardisation (`SAO→B3`, `NYQ→NYSE`, `NMS/NGM/NCM→NASDAQ`) and, for prices, the
   **non-trading-day filter** (all four OHLC null). The filter stays a plain `.filter()`; it is
   source shape, not a quality failure.
2. **Gate**: a `_violations()` column holding `array<struct<rule_name, rule_expression>>` of every
   `error` rule the row fails (rules in name order, `(expr) IS FALSE` so NULL is not a violation).
   Two temporary views per domain: `<domain>_valid` (size = 0, with `@dp.expect_all(WARN_RULES)`)
   and `<domain>_rejected` (size > 0, plus `rule_name`, `rule_expression`, `failed_rules`,
   `failed_at`, `ruleset_version`).
3. **Deduplicate idempotently** with `dp.create_auto_cdc_flow` into the silver table and, separately,
   into the quarantine table:
   - `silver.prices`: SCD1, keys `(ticker, date)`, `sequence_by = struct(_ingested_at, _source_file)`.
   - `silver.company_metrics`: SCD1, keys `(ticker, extraction_date)`.
   - `silver.dim_companies`: **SCD2**, keys `(ticker)`, `sequence_by = extraction_date`,
     `track_history_column_list` = the descriptive columns (short_name, sector, industry, country,
     isin, full_time_employees, exchange, currency). `__START_AT`/`__END_AT` replace the hand-built
     `start_date`/`end_date`/`is_active`; Gold projects them back to those names.
   - Quarantine tables `ops.<domain>_quarantine`: SCD1 on the same keys, so a replayed bad row does
     not inflate the quarantine.

   Quarantining happens **before** the SCD2 flow, so a ticker with a bad extraction keeps its
   current active record (today's behaviour, `test_silver_metadata_quarantined_ticker_keeps_existing_active_record`).

**[verify]** Exact parameter names of `dp.create_auto_cdc_flow` (`track_history_column_list`,
`stored_as_scd_type`) and `dp.create_streaming_table` (`expect_all_or_fail`, `schema`,
`cluster_by`) on the current serverless channel. The Python reference page I could read lists the
functions but not their signatures.

### Ops

- `ops.prices_quarantine`, `ops.metadata_quarantine`, `ops.metrics_quarantine` (above).
- `ops.row_conservation`, one row per domain, materialized view with `@dp.expect_or_fail`:

  ```
  bronze_rows = non_trading_rows + duplicates_collapsed + passed_rows + quarantined_rows
  ```

  **[decide]** The brief's identity is `bronze_rows = duplicates_collapsed + passed + quarantined`.
  For prices that cannot hold, because the non-trading-day filter removes rows on purpose before
  the gate. I propose adding the `non_trading_rows` term (zero for metadata and metrics) rather
  than hiding the filtered rows inside `duplicates_collapsed`. A second fail expectation states the
  split from the other direction: `distinct_keys = passed_keys + quarantined_only_keys`.

  **[decide]** A key can legitimately be in both halves when a date is re-extracted: first
  version bad (quarantined), later version good (passes). With SCD1 on both tables the key then
  sits in silver *and* in quarantine, and a naive `distinct_keys = passed + quarantined` fails.
  Proposal: `ops.row_conservation` counts a key as quarantined only if its *latest* version was
  rejected; the quarantine table keeps the full history for audit. Alternative: delete the key from
  quarantine when a later good version arrives (AUTO CDC with `apply_as_deletes`), which makes the
  quarantine "currently bad rows" only. The first keeps more evidence; I recommend it.
- `ops.dataset_health`, materialized view, one row per dataset per update, for the **set-level**
  checks that are not row expectations (uniqueness, completeness %, freshness, referential
  coverage, SCD2 overlap). Each becomes a column, and expectations on this view carry the severity
  from the mapping table (`expect_all` for warn, `expect_all_or_fail` for fail). This is where
  today's Soda `duplicate_count`, `missing_percent`, freshness and "exists in dim_companies"
  checks land.

### Gold (M3, #27–#33)

Materialized views over silver, in the same pipeline:

| Gold dataset | Source | Notes |
|---|---|---|
| `gold.fact_prices` | `silver.prices` | Excludes `USDBRL` (it feeds the conversion only). |
| `gold.dim_companies` | `silver.dim_companies` | Projects `__START_AT/__END_AT` to `start_date`, `end_date`, `is_active`. |
| `gold.v_companies_active` | `gold.dim_companies` | `is_active` filter (#29). |
| `gold.fact_company_metrics` | `silver.company_metrics` | |
| `gold.v_companies_performance` | dim + latest metrics | Ratios ported from `src/db_init/init.sql` (#30). |
| `gold.v_fact_prices_converted` | `fact_prices` + `USDBRL` series | **ClickHouse `ASOF LEFT JOIN` has no direct Databricks SQL equivalent.** Port as a forward-filled daily rate (window `last_value(... ignore nulls)` over a date spine, or a range join on `rate_date <= date` keeping the max). **[decide]** which; both are testable offline. (#31) |

UC metric views (#32, #33) are out of scope for this plan; they sit on top of these views.

---

## 2. Rule mapping

**Legend.** `Q` = quarantine (`error`: row routed to `ops.<domain>_quarantine`); `W` = `dp.expect_all`
(warn: counted in the event log, row passes); `F` = `dp.expect_or_fail` / `expect_all_or_fail`
(fails the update); `X` = dropped, with the reason. Expressions are SQL that must be TRUE for an
acceptable row; NULL is not a violation. `Gold-NN` = column is NOT NULL in the old ClickHouse DDL.

### 2a. Rules in `src/streaming/quality_rules.py`

| # | Domain | Rule (current name) | Expression (TRUE = acceptable) | Soda mirror | Map | Note |
|---|---|---|---|---|---|---|
| 1 | prices | `require_columns(ticker)` | `ticker IS NOT NULL` | `missing_count(ticker) = 0` (silver, gold) | F | Today's `RequiredColumnMissingError`. On the conformed source view. |
| 2 | prices | `require_columns(date)` | `date IS NOT NULL` | `missing_count(date) = 0` | F | |
| 3 | prices | `open_not_null` | `open IS NOT NULL` | gold `missing_count(open)` | Q | Gold-NN. Rationale changes from "ClickHouse would coerce to 0" to "a traded row without a price cannot be charted or converted". |
| 4 | prices | `high_not_null` | `high IS NOT NULL` | gold `missing_count(high)` | Q | Gold-NN. |
| 5 | prices | `low_not_null` | `low IS NOT NULL` | gold `missing_count(low)` | Q | Gold-NN. |
| 6 | prices | `close_not_null` | `close IS NOT NULL` | gold `missing_count(close)` | Q | Gold-NN. |
| 7 | prices | `adj_close_not_null` | `adj_close IS NOT NULL` | gold `missing_count(adj_close)` | Q | Gold-NN. |
| 8 | prices | `volume_not_null` | `volume IS NOT NULL` | gold `missing_count(volume)` | Q | Gold-NN. Extractor fills NaN volume with 0, so this is a tripwire. |
| 9 | prices | `dividends_not_null` | `dividends IS NOT NULL` | gold `missing_count(dividends)` | Q | Gold-NN. |
| 10 | prices | `stock_splits_not_null` | `stock_splits IS NOT NULL` | gold `missing_count(stock_splits)` | Q | Gold-NN. |
| 11 | prices | `ingestion_timestamp_not_null` | — | gold `missing_count(ingestion_timestamp)` | X | Set by the pipeline (`_ingested_at = current_timestamp()`), never null. |
| 12 | prices | `open_range` | `open >= 0` | silver `min(open) >= 0` | Q | |
| 13 | prices | `high_range` | `high >= 0` | silver `min(high) >= 0` | Q | |
| 14 | prices | `low_range` | `low >= 0` | silver `min(low) >= 0` | Q | |
| 15 | prices | `close_range` | `close >= 0` | silver `min(close) >= 0` | Q | |
| 16 | prices | `adj_close_range` | `adj_close >= 0` | silver `min(adj_close) >= 0` | Q | |
| 17 | prices | `volume_range` | `volume >= 0` | silver `min(volume) >= 0` | Q | |
| 18 | prices | `dividends_range` | `dividends >= 0` | silver `min(dividends) >= 0.0`; gold "dividends and stock splits non-negative" | Q | |
| 19 | prices | `stock_splits_range` | `stock_splits >= 0` | silver `min(stock_splits) >= 0.0`; gold same | Q | |
| 20 | prices | `ticker_format` | `ticker RLIKE '^[A-Z0-9.\\-]+$'` | silver + gold "invalid ticker format" | Q | Also catches a failed `USDBRL=X` rename (`=` is not allowed). |
| 21 | prices | `high_not_highest` | `NOT (volume > 0 AND open > 0 AND (high < low OR high < open OR high < close))` | silver warn, gold fail "high ≥ low, open, close" | Q | Already promoted from Soda WARN to quarantine (`QUARANTINE_OHLC_INCONSISTENCY = True`). |
| 22 | prices | `low_not_lowest` | `NOT (volume > 0 AND open > 0 AND (low > open OR low > close))` | silver warn, gold fail "low ≤ open, close" | Q | Same. |
| 23 | metrics | `require_columns(ticker)` | `ticker IS NOT NULL` | `missing_count(ticker) = 0` | F | |
| 24 | metrics | `require_columns(extraction_date)` | `extraction_date IS NOT NULL` | `missing_count(extraction_date) = 0` | F | |
| 25 | metrics | `ingestion_timestamp_not_null` | — | `missing_count(ingestion_timestamp)` | X | Pipeline-set. |
| 26 | metrics | `market_cap_range` | `market_cap >= 0` | silver `min(market_cap) >= 0`; gold "non-negative" | Q | |
| 27 | metrics | `shares_outstanding_range` | `shares_outstanding >= 0` | silver min; gold non-negative | Q | |
| 28 | metrics | `total_debt_range` | `total_debt >= 0` | silver min; gold non-negative | Q | |
| 29 | metrics | `total_cash_range` | `total_cash >= 0` | silver min; gold non-negative | Q | |
| 30 | metrics | `dividend_yield_range` | `dividend_yield >= 0` | silver `min(dividend_yield) >= 0` | Q | Upper bound: see 2b row G-M4. |
| 31 | metrics | `price_to_sales_range` | `price_to_sales >= 0` | silver min | Q | |
| 32 | metrics | `current_ratio_range` | `current_ratio >= 0` | silver min; gold non-negative; perf "market metrics positive" | Q | |
| 33 | metrics | `asset_turnover_range` | `asset_turnover >= 0` | silver min; gold non-negative; perf "market metrics positive" | Q | |
| 34 | metrics | `trailing_pe_range` | `trailing_pe >= 0` | silver min; gold non-negative | Q | |
| 35 | metrics | `ebitda_range` | `ebitda > -1000000000000` | silver `min(ebitda) > -1e12` | Q | |
| 36 | metrics | `net_income_to_common_range` | `net_income_to_common > -1000000000000` | silver min | Q | |
| 37 | metrics | `book_value_range` | `book_value > -10000` | silver min | Q | |
| 38 | metrics | `trailing_eps_range` | `trailing_eps > -1000` | silver min | Q | |
| 39 | metrics | `peg_ratio_range` | `peg_ratio > -100` | silver `min(peg_ratio) > -100.0`; **gold `peg_ratio < 0` fails** | Q | **[decide]** Conflict: gold Soda fails on any negative PEG; silver allows down to -100. Proposal: keep the silver bound as Q (negative PEG is legitimate for shrinking earnings) and drop the gold check. |
| 40 | metrics | `price_to_book_range` | `price_to_book > -1000` | silver min | Q | |
| 41 | metrics | `debt_to_equity_range` | `debt_to_equity > -1000 AND debt_to_equity < 500000` | silver min/max | Q | |
| 42 | metrics | `operating_margins_range` | `operating_margins > -200 AND operating_margins <= 2.5` | silver min/max | Q | Narrower gold band → W, row G-M5. |
| 43 | metrics | `ebitda_margins_range` | `ebitda_margins > -200 AND ebitda_margins <= 2.5` | silver min/max | Q | Same. |
| 44 | metrics | `profit_margins_range` | `profit_margins > -200 AND profit_margins <= 5.5` | silver min/max | Q | Same. |
| 45 | metrics | `gross_margins_range` | `gross_margins > -200 AND gross_margins <= 2.5` | silver min/max | Q | Same. |
| 46 | metrics | `roa_range` | `roa > -50 AND roa <= 50` | silver min/max | Q | |
| 47 | metrics | `roe_range` | `roe > -100 AND roe <= 100` | silver min/max | Q | |
| 48 | metrics | `enterprise_to_ebitda_range` | `enterprise_to_ebitda > -5000 AND enterprise_to_ebitda < 5000` | silver min/max | Q | |
| 49 | metrics | `enterprise_to_ebit_range` | `enterprise_to_ebit > -5000 AND enterprise_to_ebit < 5000` | silver min/max | Q | |
| 50 | metrics | `ticker_format` | `ticker RLIKE '^[A-Z0-9.\\-]+$'` | silver + gold ticker format | Q | |
| 51 | metadata | `require_columns(ticker)` | `ticker IS NOT NULL` | `missing_count(ticker) = 0` | F | |
| 52 | metadata | `require_columns(extraction_date)` | `extraction_date IS NOT NULL` | `missing_count(extraction_date) = 0` | F | |
| 53 | metadata | `industry_not_null` | `industry IS NOT NULL` | `missing_count(industry) = 0` | Q | `N/A` stays allowed (current behaviour). |
| 54 | metadata | `country_not_null` | `country IS NOT NULL` | — | Q | |
| 55 | metadata | `isin_not_null` | `isin IS NOT NULL` | — | Q | |
| 56 | metadata | `ingestion_timestamp_not_null` | — | `missing_count(ingestion_timestamp)` | X | Pipeline-set. |
| 57 | metadata | `short_name_populated` | `short_name IS NOT NULL AND short_name <> 'N/A'` | `missing_count(short_name) = 0` | Q | Written without relying on NULL semantics: a NULL must breach here, so the expression is NOT NULL-safe by design. |
| 58 | metadata | `sector_populated` | `sector IS NOT NULL AND sector <> 'N/A'` | `missing_count(sector) = 0` | Q | Same. |
| 59 | metadata | `exchange_populated` | `exchange IS NOT NULL AND exchange <> 'N/A'` | `missing_count(exchange) = 0` | Q | Same. |
| 60 | metadata | `currency_populated` | `currency IS NOT NULL AND currency <> 'N/A'` | `missing_count(currency) = 0` | Q | Same. |
| 61 | metadata | `full_time_employees_range` | `full_time_employees >= 0` | `min(full_time_employees) >= 0` | Q | |
| 62 | metadata | `isin_format` | `isin RLIKE '^(N/A\|[A-Z]{2}[A-Z0-9]{9}[0-9])$'` | silver + gold ISIN format | Q | |
| 63 | metadata | `ticker_format` | `ticker RLIKE '^[A-Z0-9.\\-]+$'` | silver + gold ticker format | Q | |

**NULL semantics change, stated plainly.** Today a rule's breach condition decides NULL handling
per rule (`range_rule` ignores NULL, `populated_rule` treats NULL as a breach). In the new form an
expression that evaluates to NULL is not a violation (`(expr) IS FALSE`), matching Lakeflow
expectation semantics. Rules 3–10 and 53–60 must therefore be written so a NULL makes them FALSE
(as above), and each gets an offline test with a NULL fixture row so the difference is pinned.

### 2b. Soda checks not covered by a row in 2a

Grouped rows (`schema`, `missing_count`, `min`/`max`) list every check they cover; nothing is
omitted.

| # | Contract | Soda check | Map | Where / expression | Note |
|---|---|---|---|---|---|
| S-P1 | silver_prices | `schema` required columns; `schema` column types | X | Declared `schema` on the streaming table | A missing column or wrong type fails table creation/update. |
| S-P2 | silver_prices | `row_count > 0` | F | `ops.row_conservation`: `passed_rows > 0` | |
| S-P3 | silver_prices | `duplicate_count(ticker, date) = 0` | F | `ops.dataset_health`: `duplicate_keys = 0` | AUTO CDC SCD1 makes this true by construction; kept as a tripwire. |
| S-P4 | silver_prices | `min(open/high/low/close/adj_close/volume/dividends/stock_splits)` | — | Rows 12–19 | Mirrored. |
| S-P5 | silver_prices | `USDBRL=X` renamed | X | Row 20 | `=` fails the ticker format, so a missed rename is quarantined. |
| S-P6 | silver_prices | `date` not in future (`date > current_date + 1`) | Q | `date <= date_add(current_date(), 1)` | New Q rule; was a failing Soda check after the write. |
| S-P7 | silver_prices | `ingestion_timestamp >= date - 1 day` | X | — | `_ingested_at` is the pipeline's clock; with S-P6 in place this cannot fail. |
| S-M1 | silver_metadata | `schema` × 2 | X | Declared schema | |
| S-M2 | silver_metadata | `row_count > 0` | F | `ops.row_conservation` | |
| S-M3 | silver_metadata | `missing_count` for ticker, short_name, sector, industry, exchange, currency, extraction_date | — | Rows 51–53, 57–60 | Mirrored. |
| S-M4 | silver_metadata | `missing_count(ingestion_timestamp)`, `missing_count(start_date)`, `missing_count(is_active)` | X | — | Pipeline-set / generated by AUTO CDC SCD2. |
| S-M5 | silver_metadata | `duplicate_count(ticker, extraction_date) = 0` | F | `ops.dataset_health` | Tripwire. |
| S-M6 | silver_metadata | ticker format, ISIN format, `min(full_time_employees) >= 0` | — | Rows 61–63 | Mirrored. |
| S-M7 | silver_metadata | `invalid_count(exchange)` valid `[B3, BTS, NASDAQ, NYSE]` | W | `exchange IN ('B3','BTS','NASDAQ','NYSE')` | **[decide]** Soda fails; `quality_rules.metadata_rules` deliberately has no allow-list so an unmapped but legitimate venue flows through. Proposal: warn, which respects the rules module and still surfaces a new venue. |
| S-M8 | silver_metadata | `invalid_count(currency)` valid `[USD, BRL]` | W | `currency IN ('USD','BRL')` | Same reasoning. The conversion view fails loudly (G-C3) if a third currency reaches it. |
| S-M9 | silver_metadata | `invalid_count(is_active)` | X | — | Derived from `__END_AT` in Gold. |
| S-M10 | silver_metadata | `missing_percent(full_time_employees) < 15` | W | `ops.dataset_health`: `employees_missing_pct < 15` | Set-level. |
| S-M11 | silver_metadata | Active ⇒ `end_date` NULL; inactive ⇒ `end_date` set; `end_date >= start_date` | F | `gold.dim_companies`: `is_active = (end_date IS NULL)`, `end_date IS NULL OR end_date >= start_date` | True by construction under AUTO CDC SCD2; tripwires. |
| S-M12 | silver_metadata | At most one active record per ticker; active periods do not overlap | F | `ops.dataset_health`: `multi_active_tickers = 0`, `overlapping_periods = 0` | Set-level tripwires. |
| S-M13 | silver_metadata | `extraction_date` not in future | Q | `extraction_date <= current_date()` | New Q rule. |
| S-M14 | silver_metadata | `start_date` not in future; `ingestion_timestamp >= extraction_date` | X | — | `start_date` = `__START_AT` = `extraction_date` (covered by S-M13); ingestion is pipeline-set. |
| S-R1 | silver_metrics | `schema` × 2 | X | Declared schema | |
| S-R2 | silver_metrics | `row_count > 0` | F | `ops.row_conservation` | |
| S-R3 | silver_metrics | `missing_count` ticker, extraction_date | — | Rows 23–24 | |
| S-R4 | silver_metrics | `missing_count(ingestion_timestamp)`; ingestion after extraction; ingestion not in future | X | — | Pipeline-set. |
| S-R5 | silver_metrics | `duplicate_count(ticker, extraction_date) = 0` | F | `ops.dataset_health` | Tripwire. |
| S-R6 | silver_metrics | ticker format; all 30 `min`/`max` bounds | — | Rows 26–50 | Mirrored one-for-one (the rules module was written from these). |
| S-R7 | silver_metrics | `extraction_date` not in future | Q | `extraction_date <= current_date()` | New Q rule. |
| S-R8 | silver_metrics | `missing_percent` of market_cap < 5, shares_outstanding < 10, ebitda < 25, total_cash < 10, operating_margins < 25, profit_margins < 25, trailing_pe < 35 | W | `ops.dataset_health`, one column per check | Set-level; a jump means a yfinance payload change. |
| G-P1 | gold_prices | `schema` × 2 (ClickHouse types) | X | Gold MV schema | ClickHouse-specific types no longer exist. |
| G-P2 | gold_prices | `row_count > 0` | F | `gold.fact_prices` expectation via `ops.dataset_health` | |
| G-P3 | gold_prices | `missing_count` × 11 | — | Rows 1–11 | Gold is a projection of gated silver. |
| G-P4 | gold_prices | `duplicate_count(ticker, date)` | F | `ops.dataset_health` | |
| G-P5 | gold_prices | ticker format; dividends/splits non-negative; high/low consistency | — | Rows 18–22 | |
| G-P6 | gold_prices | prices "strictly positive": any negative price, or `volume > 0` with a zero price | Q | `NOT (volume > 0 AND (open = 0 OR high = 0 OR low = 0 OR close = 0 OR adj_close = 0))` | New Q rule in silver prices (negatives already covered by 12–17). Untraded zero-price days stay valid. |
| G-P7 | gold_prices | trading date not in future (`> today + 1`) | — | S-P6 | |
| G-P8 | gold_prices | ingestion not in future and `>= date - 1 day` | X | — | As S-P7. |
| G-P9 | gold_prices | every ticker exists in `dim_companies` (warn) | W | `ops.dataset_health`: `prices_tickers_without_company = 0` | |
| G-P10 | gold_prices | intraday variation > 5× (warn) | W | silver prices `expect_all`: `NOT (volume > 0 AND open > 0 AND (close / open > 5 OR open / close > 5))` | Row passes. |
| G-P11 | gold_prices | latest date not older than 4 days (fail) | W | `ops.dataset_health`: `max_date >= current_date() - 4` | **[decide]** Proposal: warn, not fail. As a fail expectation it would fail every refresh during a long holiday, and every dev refresh (dev schedules are paused). Job failure alerts (#40, #41) already catch a missed run. |
| G-C1 | gold_companies | `schema` × 2; `row_count > 0` | X / F | Declared schema; `ops.dataset_health` | |
| G-C2 | gold_companies | `missing_count` × 10 | — | Rows 51–60, S-M4 | |
| G-C3 | gold_companies | `duplicate_count(ticker, start_date)` | F | `ops.dataset_health` | |
| G-C4 | gold_companies | ticker format, ISIN format, exchange/currency valid values, `is_active` valid | — | Rows 62–63, S-M7, S-M8, S-M9 | |
| G-C5 | gold_companies | sector / industry placeholder for non-ETF tickers (warn) | W | silver dim `expect_all`: `ticker RLIKE '(11\|4L\|3L)\\.SA$' OR sector NOT IN ('', ' ', 'null', 'NULL', 'N/A', 'Unknown', 'unknown')` (and the same for industry) | Only rows in the active version matter; Gold evaluates on `is_active`. |
| G-C6 | gold_companies | start/extraction date not in future; ingestion checks | — / X | S-M13, S-M14 | |
| G-C7 | gold_companies | SCD2: active ⇒ end NULL, inactive ⇒ end set, end ≥ start, one active per ticker, no overlap, **no gaps** | F | S-M11, S-M12, plus `ops.dataset_health`: `history_gaps = 0` | AUTO CDC sets `__END_AT` to the next `__START_AT`, so gaps are impossible by construction; tripwire. |
| G-C8 | gold_companies | active companies have price records (warn) | W | `ops.dataset_health`: `active_companies_without_prices = 0` | |
| G-C9 | v_companies_active | unique ticker; only active; `end_date` NULL | F | Expectations on `gold.v_companies_active` (row-level) and `ops.dataset_health` (uniqueness) | Tripwires on a filter view. |
| G-M1 | gold_metrics | `schema` × 2; `row_count > 0`; `missing_count` × 3; duplicate key | X / F / — / F | As above | |
| G-M2 | gold_metrics | ticker format; non-negative sizes and ratios | — | Rows 26–34, 50 | Except `peg_ratio < 0`, see row 39. |
| G-M3 | gold_metrics | market cap populated when `shares_outstanding > 0` | Q | `NOT (shares_outstanding > 0 AND (market_cap IS NULL OR market_cap <= 0))` | New Q rule in silver metrics. |
| G-M4 | gold_metrics | `dividend_yield` between 0 and 200 | Q | `dividend_yield <= 200` | New upper bound, added to row 30's rule. |
| G-M5 | gold_metrics | margins between -10 and 1 (warn) | W | silver metrics `expect_all`, one per margin column | Narrower than the Q bounds in 42–45. |
| G-M6 | gold_metrics | extraction not in future; ingestion checks | — / X | S-R7, S-R4 | |
| G-M7 | gold_metrics | tickers exist in `dim_companies` (warn) | W | `ops.dataset_health` | |
| G-M8 | gold_metrics | latest extraction not older than 7 days (fail) | W | `ops.dataset_health` | **[decide]** Same reasoning as G-P11. |
| G-V1 | gold_performance | `schema` × 2; `row_count > 0`; duplicate `(ticker, extraction_date)`; duplicate `ticker` | X / F / F / F | Gold MV / `ops.dataset_health` | |
| G-V2 | gold_performance | `p_ebitda = market_cap / ebitda` when `ebitda > 0`; `net_debt_ebitda` formula; `equity_assets + liabilities_assets = 1.0` | F | Row-level expectations on `gold.v_companies_performance` | Computed by the view itself; tripwires on the port from ClickHouse SQL. |
| G-V3 | gold_performance | market metrics non-negative; margins realistic (warn); extraction not in future; freshness 7 days | — / W / — / W | Rows 26, 32–33; G-M5; S-R7; G-M8 | |
| G-X1 | gold_prices_converted | `schema` × 2; `row_count > 0`; `missing_count` ticker/date/currency/volume; duplicate key | X / F / F / F | Gold MV / `ops.dataset_health` | Missing checks become row-level F tripwires on the view. |
| G-X2 | gold_prices_converted | `currency IN ('USD','BRL')` | F | Row-level on `gold.v_fact_prices_converted` | A third currency cannot be converted; failing is correct. |
| G-X3 | gold_prices_converted | BRL rows equal `fact_prices`; USD rows equal `fact_prices`; BRL = USD × rate; converted OHLC boundaries consistent; prices and rate positive | F | Row-level on the view | Tripwires on the port of the ASOF logic (section 1, Gold). |
| G-X4 | gold_prices_converted | rate within 2.0–10.0 (warn) | W | `usd_brl IS NULL OR usd_brl BETWEEN 2 AND 10` | |
| G-X5 | gold_prices_converted | date not in future | — | S-P6 | |
| G-X6 | gold_prices_converted | rate populated for the last 4 days (fail) | W | `ops.dataset_health`: `recent_rows_without_rate = 0` | **[decide]** Same reasoning as G-P11; also NULL by design before 2026-04-02. |

**Totals.** 63 rules in 2a: 56 → Q, 6 → F, 3 → X (pipeline-set timestamps; counted once per
domain). Soda adds 6 new Q rules (S-P6, S-M13, S-R7, G-P6, G-M3, G-M4) and 7 new W expectations;
every set-level check moves to `ops.dataset_health` or `ops.row_conservation`; all ClickHouse
`schema` checks are dropped in favour of declared table schemas. Five severity changes need your
call (rows 39, S-M7/S-M8, G-P11/G-M8/G-X6).

---

## 3. Deletions and what replaces their guarantees

| Delete | Guarantee it provided | Replacement |
|---|---|---|
| `utils.ingest_landing_to_bronze`, `infer_execution_date`, `resolve_bronze_filename`, `src/streaming/bronze.py`, `bronze_metadata.py` | Each landing file ingested once; archived files skipped | Auto Loader file tracking in the streaming table checkpoint. Files stay in `landing/`. |
| `utils.replay_archive_to_bronze`, `find_archive_dates`, `recover_bronze_from_archive`; the `data/archive/` folder | Bronze can be rebuilt after a rollback | Landing files are never moved, so a full refresh of `bronze.*_raw` re-reads everything. Delta time travel on managed tables covers accidental writes. |
| `utils.heal_corrupt_delta_checkpoints`, `check_and_heal_corrupt_data_file`, `find_version_introducing_file`, `extract_corrupt_parquet_filename`, `is_corruption_error`, `verify_delta_table`; every `except` block calling them | Recovery from parquet damaged by the macOS Docker bind mount | The cause disappears: managed tables on Databricks storage, not a bind mount. Recovery, if ever needed, is `RESTORE TABLE` or a full refresh. |
| `src/streaming/maintenance.py`, `dag_delta_maintenance` | OPTIMIZE + VACUUM weekly | Predictive optimization on UC managed tables. **[verify]** that it is enabled on Free Edition (#42). |
| `src/streaming/silver.py`, `silver_metadata.py` (Spark batch jobs) | Dedup, SCD2, quarantine | Pipeline source files; conform functions keep the transform logic. |
| `src/streaming/quality_rules.py` | Rule definitions + `classify`/`split_valid_rejected` | Rules as SQL expressions (section 2) + `_violations()` routing. Deleted only after the parity test in section 7 passes. |
| `src/streaming/gold.py`, `src/db_init/` (#48) | ClickHouse load, DDL, views | Gold materialized views. |
| `src/quality/soda_validator.py`, `soda/` (#47) | Post-write contract checks | Section 2 expectations, quarantine and `ops.dataset_health`. |
| `src/streaming/spark_session.py`, `src/streaming/utils.py` remainder, `src/streaming/legacy_paths.py`, `src/utils/dag_helpers.py` | Session factory, ClickHouse client, local paths, Airflow ClickHouse hook | Pipeline-provided session; `src/stock_market/catalog.py` (UC names). |
| `src/utils/alerts.py`, `airflow/` (#45) | Email + Discord on task failure | Job email notifications (#40) and a Discord notification destination (#41). |
| `docker/`, `Makefile` Docker targets (#46), `legacy` dependency group (#50) | Local runtime | Serverless jobs and pipelines; `uv` for local tests. |

Order: delete only after the matching Databricks piece has run green on dev and the parity check
(#44) passes, so there is never a window with neither implementation.

---

## 4. Checkpoints and schema locations

**Finding that changes the brief.** The Auto Loader documentation states: "You do not need to
provide a schema or checkpoint location because Lakeflow pipelines automatically manage these
settings." Inside the pipeline, `bronze.prices_raw` and `bronze.metadata_raw` therefore get no
`cloudFiles.schemaLocation` and no `checkpointLocation`, and nothing writes to `ops.checkpoints`.

The `ops.checkpoints` volume is still created by the bootstrap (as the brief asked). **[decide]**
keep it as reserved space for any stream that ever runs outside the pipeline, or remove it from
`scripts/bootstrap_catalogs.py`. If kept, the naming convention for such streams is:

```
/Volumes/<catalog>/ops/checkpoints/<layer>/<dataset>/checkpoint     # e.g. bronze/prices_raw/checkpoint
/Volumes/<catalog>/ops/checkpoints/<layer>/<dataset>/schema         # Auto Loader schema location
```

built only through `CatalogNames.checkpoints_root` in `src/stock_market/catalog.py`, never as a literal.

---

## 5. Bundle resources and how configuration reaches code

`resources/lakehouse_pipeline.yml` (illustrative; field names **[verify]** against
`databricks bundle schema` on the installed CLI before committing):

```yaml
resources:
  pipelines:
    stock_market_lakehouse:
      name: stock_market_lakehouse_${var.environment}
      catalog: ${var.catalog}
      schema: silver                 # default schema; datasets use schema-qualified names
      serverless: true
      continuous: false
      # no `development:` key: the target mode decides (tests/unit/test_deploy_provenance.py)
      configuration:
        stock_market.catalog: ${var.catalog}
        stock_market.max_tickers: ${var.max_tickers}
      root_path: ../src/pipelines
      libraries:
        - glob:
            include: ../src/pipelines/**
```

`resources/jobs.yml`: three jobs, each a `spark_python_task` (or `python_wheel_task`, see #4) for
the extractor with parameters
`["--landing-path", "${var.landing_path}", "--max-tickers", "${var.max_tickers}", "--date", "{{job.start_time.iso_date}}"]`,
then a `pipeline_task` on `${resources.pipelines.stock_market_lakehouse.id}`. Schedules only in
prod (dev mode pauses them anyway; test must never have one).

How values reach code:

| Value | Route | Read by |
|---|---|---|
| `var.catalog` | pipeline `configuration` → `spark.conf` | `config.catalog_from_spark_conf(spark)` at the top of each pipeline source file |
| `var.max_tickers` | pipeline `configuration` → `spark.conf`; job task parameter `--max-tickers` | Extractor CLI caps the input. The pipeline only records it in `ops.row_conservation` so a dev row count can be read in context; it filters nothing. |
| `var.landing_path` | job task parameter `--landing-path` | Extractor CLI. The pipeline derives the same path from the catalog (`CatalogNames.landing_dir`); `test_deploy_provenance.py` asserts the two agree. |

**Deviation, flagged.** For the extractors the route is `var → job task parameter → argv → code`,
not `spark.conf`: a Python script task receives parameters as arguments, and the extractors use no
Spark at all (pandas + yfinance). It still satisfies the rule that matters (no literal, no
environment variable).

**Shared rules module.** The documentation says the pipeline root folder is added to `sys.path`.
Plan: rules live in `src/pipelines/rules.py` (pure Python, SQL strings, no Spark import), imported
by the pipeline source files and by the unit tests. **[verify]** first, with a one-file probe
pipeline on dev, that a serverless pipeline deployed from the bundle can import a sibling module.
If it cannot, the rules move inline into each pipeline source file (per the brief), and the unit
tests load them by parsing the file rather than importing it. `ops.dq_rules` (a UC table) is the
last resort, not the default.

---

## 6. Implementation order (each step deployable on dev on its own)

1. Probe pipeline: sibling import, `_metadata` columns, `spark.conf` configuration key. Delete after.
2. Bronze streaming tables + pipeline + jobs (extract → refresh), prices then metadata. (M1)
3. `src/pipelines/rules.py` + offline tests + parity test against `quality_rules.py`.
4. Silver prices: conform, gate, AUTO CDC, quarantine, `ops.row_conservation`. Then metrics, then
   `dim_companies` SCD2. (M2)
5. `ops.dataset_health`. Gold views. (M3)
6. Alerts on jobs. (M4)
7. Backfill prod, parity check against ClickHouse (#43, #44), then the deletions in section 3. (M5)

---

## 7. Tests

### Break when the deletions in section 3 land, and are deleted with their module

`tests/streaming/test_bronze.py`, `test_bronze_metadata.py`, `test_maintenance.py`, `test_gold.py`,
`test_utils.py`, `tests/dags/test_dag_validation.py`, `tests/utils/test_alerts.py`.

### Rewritten

| Today | Becomes |
|---|---|
| `tests/streaming/test_quality_rules.py` (Column-based rules, `classify`, `split_valid_rejected`, `require_columns`) | `tests/unit/test_rules.py`: SQL-expression rules evaluated offline (below). |
| `tests/streaming/test_silver.py`, `test_silver_metadata.py` (end-to-end over local Delta paths) | `tests/unit/test_conform_*.py`: the conform functions on fixture DataFrames (casts, ticker normalisation, `USDBRL` rename, exchange mapping, non-trading-day filter). Dedup/SCD2 behaviour moves to AUTO CDC and is checked by the `ops` expectations on dev, not re-implemented in a test. |
| `tests/conftest.py` `DATA_DIR` isolation | Dropped with `legacy_paths.py`; the Spark fixture stays. |

### Unchanged

`tests/producer/*` (updated with the producer refactor), `tests/unit/test_no_hardcoded_catalog.py` (added with it),
`tests/unit/test_deploy_provenance.py`, `tests/unit/test_bootstrap_catalogs.py`.

### Offline rule tests with local PySpark

Rules are `QualityRule(name, dataset, expression, severity)` where `expression` is a SQL string.
For each rule the suite asserts, on `spark.createDataFrame` fixture rows:

- a known-good row: `(expr) IS FALSE` is false;
- a known-bad row: `(expr) IS FALSE` is true, and the row lands in the rejected half of the same
  `_violations()` function the pipeline uses;
- a NULL row: not a violation, except for the presence rules (3–10, 53–60), where it is;
- boundary rows for every bound (inclusive vs exclusive, e.g. `ebitda = -1e12` breaches,
  `-1e12 + 1` passes), taken from section 2;
- a structural check that every `error`/`warn` rule name is unique per dataset and every rule in
  section 2 exists (so a rule cannot be dropped silently).

**Parity test (temporary).** Until `quality_rules.py` is deleted, one test runs the old
`classify()` and the new expression rules over the same fixtures and asserts the same rows are
rejected for the same rule names. It is deleted together with `quality_rules.py`.

The pipeline source files themselves are not imported by unit tests (they need a live pipeline
session); everything with logic in it lives in importable modules.

---

## 8. Cannot be verified from here (Free Edition / no workspace access)

- Serverless jobs reaching Wikipedia, BRAPI and Yahoo Finance (issue #6 spike).
- A serverless pipeline importing a sibling module from its root folder (section 5).
- `_metadata` file columns for Parquet in a pipeline; Auto Loader on a managed volume path in a
  Free Edition pipeline.
- Exact `dp.create_auto_cdc_flow` / `dp.create_streaming_table` parameter names on the current
  channel; `glob`/`root_path` pipeline fields in the installed CLI's bundle schema.
- Predictive optimization availability on Free Edition (#42).
- Behaviour when a job triggers the pipeline while another update (e.g. dev) holds the one active
  pipeline slot: fail vs queue.
- `pandas.to_parquet` writing to `/Volumes/...` from a serverless Python task (expected to work via
  the volume FUSE mount; not run).
- Whether the empty `include: resources/*.yml` glob validates before any resource file exists.

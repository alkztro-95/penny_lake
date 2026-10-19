# Penny Lake expense transformations

[bronze_ingest.py](../bronze_ingest.py) reads complete expense exports from the
managed input Volume with Auto Loader. The historical workspace Expenses pipeline
is separate and unchanged.

## Input contract

Each new, uniquely named `.json` file is a multiline JSON object containing an
`exportedAt` field and an `expenses` array. Expense objects contain `id`, `amount`,
`categories`, `category`, `currency`, `date`, and `description`, matching the
existing bronze transformation.

Input JSON files are read from
`/Volumes/${var.catalog}/${var.landing_schema}/${var.transactions_volume}/`.
The state Volume is `/Volumes/${var.catalog}/${var.landing_schema}/${var.autoloader_state_volume}/`.
Auto Loader's explicit schema location is
`<state-volume>/bronze_expenses/schema/`; the streaming checkpoint is kept
separately at `<state-volume>/bronze_expenses/checkpoint/`. Both persist across
job runs. Run the environment setup job before ingestion to create the Volume.
See [Databricks Auto Loader schema inference and evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema).

## Bronze outputs

The ingestion job incrementally appends two Delta tables in the target's bronze
`expenses` schema:

- `bronze_expense_exports`: complete JSON payloads and source metadata, including
  empty exports, required to select the latest complete Silver snapshot.
- `bronze_expenses_raw`: one row per expense, preserving the original flattened
  field names plus the source metadata and export-level ingestion timestamp.

Malformed JSON fails parsing; missing/null `expenses` or `exportedAt` and rescued
data fail batch validation. Export timestamps must parse as timestamps.
An empty expenses array is valid and produces no flattened rows, but is retained
as a complete snapshot so both current-state Silver views become empty.

The checkpoint and schema state are stored in the managed Volume, not in SDP's
internal state. Additive schema changes can require rerunning the job after
Auto Loader updates its schema. The flattened table selects only known fields.
Changed types that produce rescued data require investigation. The original JSON
files remain in the input Volume.

Every export is cumulative. Bronze intentionally retains repeated transaction
IDs across files; it is not a current-state balance or deduplicated reporting
table. Use new filenames instead of overwriting previously ingested files.
Silver selects its current state from these complete snapshots as described below.

## Deploy and run

After environment setup and uploading a representative JSON file:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev --select jobs.expense_refresh,pipelines.penny_lake_silver,pipelines.penny_lake_gold
databricks bundle run expense_refresh -t dev
```

The job has an active file-arrival trigger on the target's
`transactions_volume`. It waits 60 seconds after the last file change before
starting, so files uploaded together are processed in one run. The Bronze
notebook task retries up to two times, with a 30-second delay. Its tasks run
Bronze, Silver, then Gold in dependency order. Use the job for the complete
workflow. The complete development run succeeded with all three tasks:
[run 9936890133387](https://dbc-7c7fae20-e5c4.cloud.databricks.com/jobs/816547114899556/runs/9936890133387?o=7474656472211339).
The existing workspace Expenses pipeline has separate state and is unchanged.

## Silver current state

[silver_categories.sql](transformations/silver_categories.sql) creates
`${silver_catalog}.${schema}.silver_categories` from distinct normalized
`primary_category` values in `bronze_expenses_raw`. It includes categories
found in any ingested snapshot, even if they are no longer used in the latest.
[silver_expenses.sql](transformations/silver_expenses.sql) creates
`${silver_catalog}.${schema}._int_expenses_expectations` as a streaming table
over the append-only `bronze_expenses_raw` source. Its expectations drop invalid
rows; accepted rows are normalized and converted to a streaming enriched table
with the category labels. An AUTO CDC flow writes
`${silver_catalog}.${schema}.silver_expenses` as SCD Type 1, keyed by
`expense_id` and sequenced by `exported_at`. These datasets are defined in the
[Silver pipeline](../../resources/penny_lake_silver.pipeline.yml).

Silver retains the latest accepted version seen for each expense ID; it does
not retain SCD Type 2 history. Since Bronze raw is append-only and does not emit
delete events, IDs missing from a later full export remain in Silver. An empty
export also does not clear Silver. Source filename and ingestion metadata stay
in Bronze and are not copied into the expense stream.

Amounts use `DECIMAL(18,4)`, USD/CRC currencies are trimmed/uppercased, and dates
are parsed. The primary category is used only as an intermediate lookup key;
the final `silver_expenses` table exposes `normalized_category`,
`spending_area`, and `essentiality` instead of `primary_category` or the
original `categories` array. Original currency amounts are retained.
`amount_crc` uses the configurable `usd_to_crc_rate` bundle variable (default
500 CRC/USD) for USD expenses and a rate of 1 for CRC expenses.
`crc_conversion_rate`, `conversion_is_estimate`, and `amount_usd_original`
make the approximation explicit. No historical exchange-rate lookup is performed.
`exported_at` determines which version wins when an expense appears in multiple
exports. A full refresh is required to recompute existing amounts after changing
the conversion-rate parameter.

Rows with blank IDs or categories, unparseable amounts or dates, unsupported
currencies, missing export timestamps, or an unrepresentable CRC amount are
dropped by expectations in `_int_expenses_expectations`. Descriptions and
category arrays are optional. Negative or zero amounts remain valid to allow
refunds and adjustments.

The category dimension normalizes the primary category by trimming and
lowercasing its key, then applying title case to the label. The enriched stream
left-joins categories one-to-one, preserving expenses without a category match;
it does not explode category arrays or duplicate amounts. This is formatting
normalization, not a business-defined category rollup. `ai_classify` adds a
spending area and an essentiality label to each distinct category.

AI classification is performed on distinct category strings, not individual
expenses. Because it runs as part of the materialized view refresh, each refresh
can invoke AI inference again and incur additional cost; classifications may
also vary. Review the labels before using them for decisions.

[silver_metrics.py](../silver_metrics.py) contains the requested before/after
metrics cells: Bronze row counts and required-field nulls per input file, then
Silver row counts, distinct IDs, nulls, and total estimated CRC amount.
`bronze_expenses_raw` feeds both the Bronze-side metrics and the category
dimension.
An invalid conversion-rate parameter fails the stream update.

## Gold monthly spending

[gold_monthly_spending.sql](transformations/gold_monthly_spending.sql) creates
`${gold_catalog}.${schema}.gold_monthly_spending`, answering: "How much are we
spending in estimated CRC each month, by spending area and essentiality?" Each
row contains the month, spending area, essentiality, expense count, and total
estimated CRC amount.

The [Gold pipeline](../../resources/penny_lake_gold.pipeline.yml) reads the
current SCD Type 1 Silver expenses. It fails its update if a row has a null
month, an unknown spending area, or an unknown essentiality. Those expectations
are deliberately strict so invalid Gold data is rejected rather than silently
omitted.

The expectation was tested in `dev` with a temporary invalid area, then the test
row was removed and Gold was refreshed successfully. Databricks returned this
literal rejection (update `941d52dd-cd2a-45d7-b4f3-faab8a83205e`):

```text
[EXPECTATION_VIOLATION.VERBOSITY_ALL] Flow 'dev_penny_lake_03_gold.expenses.gold_monthly_spending' failed to meet the expectation. Violated expectations: 'valid_spending_area'. Input data: '{}'. Output record: '{"spending_month":"2026-01-01","spending_area":"INVALID_TEST","essentiality":"Indispensable","expense_count":1,"total_amount_crc":1.0}'. Missing input data: false
```

### Validation

The categories materialized view restricts AI outputs to the declared labels.
The expense expectations use `DROP ROW`, so invalid input rows do not block
valid expenses. Use unique `exportedAt` timestamps for exports; `exported_at` is
the SCD ordering column. To rebuild from all Bronze raw history or apply a
conversion-rate or normalization change to existing expenses, run a full
refresh:

```bash
databricks bundle run penny_lake_silver -t dev --full-refresh-all
```

The `expense_refresh` job runs Bronze, Silver, Gold, and then publishes the
protected reader table. To refresh just Gold after Silver has been updated, run:

```bash
databricks bundle run penny_lake_gold -t dev
```

After a standalone Gold refresh, rerun `expense_refresh` to republish the
protected reader table.

## Governance

The `apply_governance` task runs after Gold and publishes
`${gold_catalog}.${schema}.gold_monthly_spending_reader` as a Delta table. The
source materialized view stays private because Unity Catalog column masks are
applied to tables, not materialized views. The publishing notebook refreshes
the reader table from the Gold materialized view and applies the mask to
`total_amount_crc`.

The mask function lives in
`${governance_catalog}.governance.mask_gold_amount` and consults
`${governance_catalog}.governance.gold_amount_access_map`. The configured
reader principal (`alkztro@hotmail.com`) is mapped to `can_view_amount = false`,
so the masked amount is `NULL`; the function returns amounts only for
principals explicitly mapped with `can_view_amount = true`. The
`catalog_full_access_principal` (`alfredo.castro@ulatina.net` by default) is
mapped to `true`, so that principal can read unmasked amounts from the protected
reader table. Only the reader table is granted to the reader, along with
`USE CATALOG` and `USE SCHEMA` on Gold. The reader receives no access to the
source Gold materialized view, Silver, Bronze, or the mapping table. The full
access principal already receives `ALL_PRIVILEGES` on the environment catalogs.
The task tags sensitive amounts in Bronze, Silver, Gold, and the protected
reader table.

The task completed successfully in development as part of
[job run 204698341133513](https://dbc-7c7fae20-e5c4.cloud.databricks.com/?o=7474656472211339#job/816547114899556/run/204698341133513).
The verified `SHOW GRANTS` results are:

```sql
SHOW GRANTS ON CATALOG ${gold_catalog};
SHOW GRANTS ON SCHEMA ${gold_catalog}.${schema};
SHOW GRANTS ON TABLE ${gold_catalog}.${schema}.gold_monthly_spending_reader;
SHOW GRANTS ON TABLE ${gold_catalog}.${schema}.gold_monthly_spending;
```

| Securable | Principal | Action |
| --- | --- | --- |
| `dev_penny_lake_03_gold` | `alkztro@hotmail.com` | `USE CATALOG` |
| `dev_penny_lake_03_gold.expenses` | `alkztro@hotmail.com` | `USE SCHEMA` |
| `dev_penny_lake_03_gold.expenses.gold_monthly_spending_reader` | `alkztro@hotmail.com` | `SELECT` |
| `dev_penny_lake_03_gold.expenses.gold_monthly_spending` | *(no rows)* | *(no grant)* |

Effective grants were also empty for this principal on the Bronze raw table,
Silver schema, and governance mapping table. The mapping rows are `alkztro@hotmail.com | false` and
`alfredo.castro@ulatina.net | true`. The column mask was exercised as the
workspace owner: the protected table returned 93 groups with 0 non-null amounts
for the reader, while the unmasked Gold object returned 93 groups with 93
non-null amounts. The reader-principal mapping plus its effective grants were
verified, but the reader's own interactive session was not impersonated.
Column-tag metadata was verified in `information_schema.column_tags` for the
Bronze raw amount, the three Silver amount columns, and both Gold amount
columns (including the protected reader table).

To verify the masked result while signed in as the reader, run:

```sql
SELECT total_amount_crc
FROM ${gold_catalog}.${schema}.gold_monthly_spending_reader
LIMIT 5;
```

The same reader mapping, mask, and table-only grant are applied in dev, test,
and prod when `expense_refresh` completes its Gold refresh and governance task.
The test and prod bundle deployments bootstrap their catalogs first; an
`expense_refresh` run with data is still required to create and populate the
masked reader table in those targets.

Retain source JSON files and the Auto Loader state Volume. Do not delete or
recreate the checkpoint casually: it records which source files have already
been processed.

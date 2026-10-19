-- C5: job-run observability for the test expense refresh.
-- Lakeflow system-table records can take up to an hour to appear.
SELECT
  workspace_id,
  job_id,
  run_id,
  period_start_time,
  period_end_time,
  result_state
FROM system.lakeflow.job_run_timeline
WHERE workspace_id = '7474656472211339'
  AND job_id = '311134548631586'
  AND period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY period_start_time DESC
LIMIT 20;

-- At the time of measurement, this was the largest test Delta table by size:
-- bronze_expense_exports: 52,731 bytes; bronze_expenses_raw: 51,043 bytes;
-- silver_expenses: 35,218 bytes.
DESCRIBE DETAIL test_penny_lake_01_bronze.expenses.bronze_expense_exports;

-- exportedAt supports time-based inspection of each source export.
ALTER TABLE test_penny_lake_01_bronze.expenses.bronze_expense_exports
CLUSTER BY (exportedAt);

-- FULL reclusters the existing files after changing clustering keys.
OPTIMIZE test_penny_lake_01_bronze.expenses.bronze_expense_exports FULL;

-- Evidence: clusteringColumns should contain exportedAt.
DESCRIBE DETAIL test_penny_lake_01_bronze.expenses.bronze_expense_exports;

-- Evidence: the latest operation should be OPTIMIZE.
DESCRIBE HISTORY test_penny_lake_01_bronze.expenses.bronze_expense_exports LIMIT 10;

-- C5: observe expense-refresh runs for any deployed target.
-- System-table records can take up to an hour to appear.
WITH latest_jobs AS (
  SELECT workspace_id, job_id, name
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY workspace_id, job_id
    ORDER BY change_time DESC
  ) = 1
),
recent_runs AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    MIN(period_start_time) AS started_at,
    MAX(period_end_time) AS ended_at,
    MAX_BY(result_state, period_end_time) AS result_state
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
  GROUP BY workspace_id, job_id, run_id
)
SELECT
  runs.workspace_id,
  jobs.name AS job_name,
  runs.job_id,
  runs.run_id,
  runs.started_at,
  runs.ended_at,
  runs.result_state
FROM recent_runs AS runs
JOIN latest_jobs AS jobs
  ON runs.workspace_id = jobs.workspace_id
  AND runs.job_id = jobs.job_id
WHERE jobs.name RLIKE '(dev|test|prod)_penny_lake_expense_refresh$'
ORDER BY runs.started_at DESC
LIMIT 20;

-- The Silver SDP declares liquid clustering on silver_expenses. Set the
-- silver_table SQL parameter to the fully qualified table for the target, e.g.
-- prod_penny_lake_02_silver.expenses.silver_expenses, then inspect its layout
-- and pipeline-managed optimization history.
EXECUTE IMMEDIATE CONCAT('DESCRIBE DETAIL ', :silver_table);
EXECUTE IMMEDIATE CONCAT('DESCRIBE HISTORY ', :silver_table, ' LIMIT 10');

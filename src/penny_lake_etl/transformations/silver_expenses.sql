CREATE OR REFRESH STREAMING TABLE _int_expenses_expectations (
  CONSTRAINT valid_expense_id EXPECT (
    expense_id IS NOT NULL AND TRIM(expense_id) <> ''
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (
    original_amount IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_expense_date EXPECT (
    expense_date IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_primary_category EXPECT (
    primary_category IS NOT NULL AND TRIM(primary_category) <> ''
  ) ON VIOLATION DROP ROW,
  CONSTRAINT supported_currency EXPECT (
    original_currency IS NOT NULL AND original_currency IN ('USD', 'CRC')
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_exported_at EXPECT (
    exported_at IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_crc_amount EXPECT (
    amount_crc IS NOT NULL
  ) ON VIOLATION DROP ROW
)
COMMENT 'Validated, normalized, and converted append-only expense events.'
AS
WITH rate_settings AS (
  SELECT CAST(:usd_to_crc_rate AS DECIMAL(18, 6)) AS usd_to_crc_rate
),
rate_validation AS (
  SELECT
    usd_to_crc_rate,
    ASSERT_TRUE(
      usd_to_crc_rate IS NOT NULL
      AND usd_to_crc_rate > 0
      AND usd_to_crc_rate < CAST(1000000000000 AS DECIMAL(18, 0)),
      'usd_to_crc_rate must be a positive DECIMAL(18,6).'
    ) AS validation_passed
  FROM rate_settings
),
normalized_expenses AS (
  SELECT
    TRIM(CAST(expense_id AS STRING)) AS expense_id,
    description,
    TRIM(primary_category) AS primary_category,
    TRY_CAST(amount AS DECIMAL(18, 4)) AS original_amount,
    UPPER(TRIM(currency)) AS original_currency,
    TRY_CAST(expense_date AS DATE) AS expense_date,
    TRY_CAST(exported_at AS TIMESTAMP) AS exported_at
  FROM STREAM(IDENTIFIER(:bronze_expenses_table))
),
converted_expenses AS (
  SELECT
    expense.expense_id,
    expense.description,
    expense.primary_category,
    expense.original_amount,
    expense.original_currency,
    expense.expense_date,
    rate.usd_to_crc_rate AS crc_conversion_rate,
    TRY_CAST(
      expense.original_amount
        * CASE
            WHEN expense.original_currency = 'USD' THEN rate.usd_to_crc_rate
            WHEN expense.original_currency = 'CRC' THEN CAST(1 AS DECIMAL(18, 6))
          END
      AS DECIMAL(18, 4)
    ) AS amount_crc,
    CASE
      WHEN expense.original_currency = 'USD' THEN expense.original_amount
    END AS amount_usd_original,
    expense.original_currency = 'USD' AS conversion_is_estimate,
    expense.exported_at
  FROM normalized_expenses AS expense
  CROSS JOIN rate_validation AS rate
  WHERE rate.validation_passed IS NULL
)
SELECT *
FROM converted_expenses;

CREATE OR REFRESH STREAMING TABLE _int_expenses_enriched
COMMENT 'Validated expense events enriched with category labels.'
AS
SELECT
  expense.expense_id,
  expense.description,
  expense.original_amount,
  expense.original_currency,
  expense.expense_date,
  expense.crc_conversion_rate,
  expense.amount_crc,
  expense.amount_usd_original,
  expense.conversion_is_estimate,
  expense.exported_at,
  category.normalized_category,
  category.spending_area,
  category.essentiality
FROM STREAM(_int_expenses_expectations) AS expense
LEFT JOIN silver_categories AS category
  ON LOWER(TRIM(expense.primary_category)) = category.category_key;

CREATE OR REFRESH STREAMING TABLE silver_expenses
CLUSTER BY (expense_date, spending_area)
COMMENT 'Latest accepted expense version per expense_id (SCD Type 1).'
TBLPROPERTIES ('pipelines.autoOptimize.managed' = 'true');

CREATE FLOW silver_expenses_scd1 AS
AUTO CDC INTO silver_expenses
FROM STREAM(_int_expenses_enriched)
KEYS (expense_id)
SEQUENCE BY exported_at
STORED AS SCD TYPE 1;

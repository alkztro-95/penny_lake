CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_spending (
  CONSTRAINT valid_spending_month EXPECT (
    spending_month IS NOT NULL
  ) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_spending_area EXPECT (
    spending_area IS NOT NULL
    AND spending_area IN (
      'Vivienda',
      'Alimentación',
      'Transporte',
      'Servicios básicos',
      'Salud',
      'Educación',
      'Finanzas',
      'Ocio',
      'Compras personales',
      'Otros'
    )
  ) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_essentiality EXPECT (
    essentiality IS NOT NULL
    AND essentiality IN ('Indispensable', 'Discrecional', 'Depende')
  ) ON VIOLATION FAIL UPDATE
)
COMMENT 'Monthly CRC spending by spending area and essentiality.'
AS
SELECT
  CAST(DATE_TRUNC('MONTH', expense_date) AS DATE) AS spending_month,
  spending_area,
  essentiality,
  COUNT(*) AS expense_count,
  SUM(amount_crc) AS total_amount_crc
FROM IDENTIFIER(:silver_expenses_table)
GROUP BY
  CAST(DATE_TRUNC('MONTH', expense_date) AS DATE),
  spending_area,
  essentiality;

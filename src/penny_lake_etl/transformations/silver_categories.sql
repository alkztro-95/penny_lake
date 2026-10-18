CREATE OR REFRESH MATERIALIZED VIEW silver_categories (
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
COMMENT 'Distinct Bronze primary categories classified by spending area and essentiality.'
AS
WITH distinct_categories AS (
  SELECT DISTINCT
    LOWER(TRIM(primary_category)) AS category_key,
    INITCAP(LOWER(TRIM(primary_category))) AS normalized_category
  FROM IDENTIFIER(:bronze_expenses_table)
  WHERE primary_category IS NOT NULL
    AND TRIM(primary_category) <> ''
),
classified_categories AS (
  SELECT
    category_key,
    normalized_category,
    AI_CLASSIFY(
      normalized_category,
      '{
        "Vivienda": "Renta, hipoteca, mantenimiento o reparaciones del hogar",
        "Alimentación": "Supermercado, alimentos, restaurantes o comida preparada",
        "Transporte": "Gasolina, taxis, autobús, tren, estacionamiento o mantenimiento de vehículos",
        "Servicios básicos": "Electricidad, agua, gas, internet, teléfono u otros servicios del hogar",
        "Salud": "Consultas médicas, medicamentos, seguros o servicios de salud",
        "Educación": "Colegiaturas, cursos, libros o materiales educativos",
        "Finanzas": "Deudas, intereses, comisiones bancarias o servicios financieros",
        "Ocio": "Entretenimiento, viajes, actividades recreativas o suscripciones de ocio",
        "Compras personales": "Ropa, cuidado personal u otras compras individuales",
        "Otros": "Categorías que no encajan claramente en los rubros anteriores"
      }',
      MAP(
        'version', '2.1',
        'instructions', 'Clasifica el nombre de una categoría de gastos personales en el rubro que mejor corresponda.'
      )
    ) AS area_result,
    AI_CLASSIFY(
      normalized_category,
      '{
        "Indispensable": "Necesidad básica habitual como vivienda, supermercado, salud o servicios esenciales",
        "Discrecional": "Gasto normalmente opcional como ocio, viajes o compras no esenciales",
        "Depende": "No se puede determinar por el nombre; la necesidad depende del uso o contexto"
      }',
      MAP(
        'version', '2.1',
        'instructions', 'Clasifica solo por el nombre de la categoría. Gasolina y taxis pueden ser indispensables o discrecionales según su uso; elige Depende si no se puede saber.'
      )
    ) AS essentiality_result
  FROM distinct_categories
)
SELECT
  category_key,
  normalized_category,
  area_result:response[0].value::STRING AS spending_area,
  essentiality_result:response[0].value::STRING AS essentiality
FROM classified_categories;

{{ config(materialized='view') }}

-- Raw telemetry cleaning and type casting
WITH source_data AS (
    SELECT * FROM {{ source('laboratorio', 'raw_temperatura') }}
)

SELECT
    id AS medicion_id,
    timestamp AS medido_en,
    CAST(valor_crudo AS NUMERIC) AS temperatura_c,
    topico AS sensor_origen
FROM source_data
WHERE valor_crudo IS NOT NULL
  AND CAST(valor_crudo AS NUMERIC) != 0
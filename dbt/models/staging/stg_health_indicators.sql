WITH source_data AS (
    SELECT * FROM {{ ref('stg_health_indicators_raw') }}
),

cleaned AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY CAST(indicator AS TEXT), CAST(location AS TEXT), CAST(period AS TEXT)) AS indicator_id,
        
        TRIM(CAST(indicator AS TEXT)) AS indicator_name,
        TRIM(CAST(indicator_code AS TEXT)) AS indicator_code,
        
        TRIM(CAST(location AS TEXT)) AS location,
        TRIM(CAST(location_code AS TEXT)) AS location_code,
        
        TRIM(CAST(period AS TEXT)) AS period,
        CASE 
            WHEN CAST(period AS TEXT) ~ '^\d{4}$' THEN CAST(CAST(period AS TEXT) AS INTEGER)
            WHEN CAST(period AS TEXT) ~ '^\d{4}-\d{4}$' THEN CAST(SPLIT_PART(CAST(period AS TEXT), '-', 1) AS INTEGER)
            ELSE NULL
        END AS year,
        
        TRIM(LOWER(COALESCE(CAST(sex AS TEXT), 'both'))) AS sex,
        TRIM(LOWER(COALESCE(CAST(residence AS TEXT), 'total'))) AS residence,
        TRIM(LOWER(COALESCE(CAST(wealth_quintile AS TEXT), 'total'))) AS wealth_quintile,
        
        CASE 
            WHEN NULLIF(TRIM(CAST(value_numeric AS TEXT)), '') ~ '^[0-9.]+$' 
                THEN CAST(NULLIF(TRIM(CAST(value_numeric AS TEXT)), '') AS NUMERIC)
            WHEN NULLIF(TRIM(CAST(value AS TEXT)), '') ~ '^[0-9.]+$' 
                THEN CAST(NULLIF(TRIM(CAST(value AS TEXT)), '') AS NUMERIC)
            ELSE NULL
        END AS value,
        
        CASE
            WHEN CAST(value_ci AS TEXT) LIKE '%[%]%' THEN 
                CASE 
                    WHEN NULLIF(TRIM(SPLIT_PART(REPLACE(REPLACE(CAST(value_ci AS TEXT), '[', ''), ']', ''), '-', 1)), '') ~ '^[0-9.]+$'
                    THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(REPLACE(CAST(value_ci AS TEXT), '[', ''), ']', ''), '-', 1)), '') AS NUMERIC)
                    ELSE NULL
                END
            ELSE NULL
        END AS value_lower_ci,
        
        CASE
            WHEN CAST(value_ci AS TEXT) LIKE '%[%]%' THEN 
                CASE 
                    WHEN NULLIF(TRIM(SPLIT_PART(REPLACE(REPLACE(CAST(value_ci AS TEXT), '[', ''), ']', ''), '-', 2)), '') ~ '^[0-9.]+$'
                    THEN CAST(NULLIF(TRIM(SPLIT_PART(REPLACE(REPLACE(CAST(value_ci AS TEXT), '[', ''), ']', ''), '-', 2)), '') AS NUMERIC)
                    ELSE NULL
                END
            ELSE NULL
        END AS value_upper_ci,
        
        CASE 
            WHEN (
                CASE 
                    WHEN NULLIF(TRIM(CAST(value_numeric AS TEXT)), '') ~ '^[0-9.]+$' 
                        THEN CAST(NULLIF(TRIM(CAST(value_numeric AS TEXT)), '') AS NUMERIC)
                    WHEN NULLIF(TRIM(CAST(value AS TEXT)), '') ~ '^[0-9.]+$' 
                        THEN CAST(NULLIF(TRIM(CAST(value AS TEXT)), '') AS NUMERIC)
                    ELSE NULL
                END
            ) IS NULL OR NULLIF(TRIM(CAST(value AS TEXT)), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_value,

        CASE 
            WHEN value_numeric IS NULL AND (
                CASE 
                    WHEN NULLIF(TRIM(CAST(value_numeric AS TEXT)), '') ~ '^[0-9.]+$' 
                        THEN CAST(NULLIF(TRIM(CAST(value_numeric AS TEXT)), '') AS NUMERIC)
                    WHEN NULLIF(TRIM(CAST(value AS TEXT)), '') ~ '^[0-9.]+$' 
                        THEN CAST(NULLIF(TRIM(CAST(value AS TEXT)), '') AS NUMERIC)
                    ELSE NULL
                END
            ) IS NOT NULL THEN TRUE ELSE FALSE END AS has_data_quality_issue,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM source_data
    WHERE CAST(indicator_code AS TEXT) != '#indicator+code'
)

SELECT * FROM cleaned
WHERE location_code = 'KEN'
  AND year IS NOT NULL
  AND indicator_name IS NOT NULL

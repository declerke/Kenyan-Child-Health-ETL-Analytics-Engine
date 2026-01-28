WITH distinct_years AS (
    SELECT DISTINCT year
    FROM {{ ref('stg_health_indicators') }}
    WHERE year IS NOT NULL
),

time_attributes AS (
    SELECT
        year,
        
        FLOOR(year / 10) * 10 AS decade_start,
        FLOOR(year / 10) * 10 + 9 AS decade_end,
        FLOOR(year / 10) * 10 || 's' AS decade_label,
        
        FLOOR(year / 5) * 5 AS five_year_period_start,
        FLOOR(year / 5) * 5 + 4 AS five_year_period_end,
        
        {{ var('current_year') }} - year AS years_ago,
        CASE 
            WHEN year = {{ var('current_year') }} THEN 'Current year'
            WHEN year = {{ var('current_year') }} - 1 THEN 'Last year'
            WHEN year >= {{ var('current_year') }} - 5 THEN 'Recent (5 years)'
            WHEN year >= {{ var('current_year') }} - 10 THEN 'Medium term (10 years)'
            ELSE 'Historical (>10 years)'
        END AS time_category,
        
        CASE WHEN year = {{ var('current_year') }} THEN TRUE ELSE FALSE END AS is_current_year,
        CASE WHEN year >= {{ var('current_year') }} - 5 THEN TRUE ELSE FALSE END AS is_recent_5y,
        CASE WHEN year >= {{ var('current_year') }} - 10 THEN TRUE ELSE FALSE END AS is_recent_10y,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM distinct_years
)

SELECT * FROM time_attributes
ORDER BY year DESC
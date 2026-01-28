WITH child_indicators AS (
    SELECT * FROM {{ ref('int_child_health_indicators') }}
),

with_previous_year AS (
    SELECT
        indicator_id,
        indicator_name,
        indicator_category,
        severity_level,
        year,
        sex,
        residence,
        wealth_quintile,
        indicator_value,
        
        LAG(indicator_value) OVER (
            PARTITION BY indicator_name, sex, residence, wealth_quintile
            ORDER BY year
        ) AS previous_year_value,
        
        LAG(year) OVER (
            PARTITION BY indicator_name, sex, residence, wealth_quintile
            ORDER BY year
        ) AS previous_year,
        
        data_quality_score,
        dbt_updated_at
        
    FROM child_indicators
    WHERE indicator_value IS NOT NULL
),

calculate_changes AS (
    SELECT
        *,
        
        CASE 
            WHEN previous_year_value IS NOT NULL 
            THEN indicator_value - previous_year_value
            ELSE NULL
        END AS absolute_change,
        
        CASE 
            WHEN previous_year_value IS NOT NULL AND previous_year_value != 0
            THEN ((indicator_value - previous_year_value) / previous_year_value) * 100
            ELSE NULL
        END AS percentage_change,
        
        CASE
            WHEN previous_year IS NOT NULL
            THEN year - previous_year
            ELSE NULL
        END AS years_gap,
        
        CASE
            WHEN previous_year_value IS NULL THEN 'No baseline'
            WHEN indicator_value > previous_year_value THEN 'Increasing'
            WHEN indicator_value < previous_year_value THEN 'Decreasing'
            ELSE 'Stable'
        END AS trend_direction
        
    FROM with_previous_year
)

SELECT
    indicator_id,
    indicator_name,
    indicator_category,
    severity_level,
    year,
    previous_year,
    years_gap,
    sex,
    residence,
    wealth_quintile,
    
    indicator_value AS current_value,
    previous_year_value,
    
    absolute_change,
    ROUND(percentage_change, 2) AS percentage_change,
    trend_direction,
    
    CASE 
        WHEN ABS(percentage_change) >= 10 THEN TRUE
        ELSE FALSE
    END AS is_significant_change,
    
    data_quality_score,
    dbt_updated_at
    
FROM calculate_changes
ORDER BY year DESC, indicator_category, ABS(percentage_change) DESC NULLS LAST
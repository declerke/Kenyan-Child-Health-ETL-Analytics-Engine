WITH child_health AS (
    SELECT * FROM {{ ref('int_child_health_indicators') }}
),

yoy_changes AS (
    SELECT * FROM {{ ref('int_yoy_changes') }}
),

demographics AS (
    SELECT * FROM {{ ref('dim_demographics') }}
),

time_dim AS (
    SELECT * FROM {{ ref('dim_time') }}
),

fact_base AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ch.year, ch.indicator_name, ch.sex, ch.residence, ch.wealth_quintile) AS fact_key,
        
        t.year AS time_key,
        d.demographic_key,
        
        ch.indicator_id,
        ch.indicator_name,
        ch.indicator_code,
        ch.indicator_category,
        ch.severity_level,
        
        ch.indicator_value,
        ch.value_lower_ci,
        ch.value_upper_ci,
        ch.ci_width,
        
        yoy.previous_year_value,
        yoy.absolute_change,
        yoy.percentage_change,
        yoy.trend_direction,
        yoy.is_significant_change,
        
        ch.is_missing_value,
        ch.has_data_quality_issue,
        ch.data_quality_score,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM child_health ch
    
    LEFT JOIN demographics d
        ON ch.sex = d.sex
        AND ch.residence = d.residence
        AND ch.wealth_quintile = d.wealth_quintile
    
    LEFT JOIN time_dim t
        ON ch.year = t.year
    
    LEFT JOIN yoy_changes yoy
        ON ch.indicator_id = yoy.indicator_id
        AND ch.year = yoy.year
        AND ch.sex = yoy.sex
        AND ch.residence = yoy.residence
        AND ch.wealth_quintile = yoy.wealth_quintile
    
    WHERE ch.indicator_value IS NOT NULL
)

SELECT 
    fact_key,
    time_key,
    demographic_key,
    indicator_id,
    indicator_name,
    indicator_code,
    indicator_category,
    severity_level,
    
    indicator_value,
    value_lower_ci,
    value_upper_ci,
    ci_width,
    
    previous_year_value,
    absolute_change,
    percentage_change,
    trend_direction,
    is_significant_change,
    
    is_missing_value,
    has_data_quality_issue,
    data_quality_score,
    
    dbt_updated_at
    
FROM fact_base
ORDER BY time_key DESC, indicator_category, indicator_name
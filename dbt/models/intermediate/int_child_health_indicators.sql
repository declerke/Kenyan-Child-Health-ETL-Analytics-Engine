WITH staged_indicators AS (
    SELECT * FROM {{ ref('stg_health_indicators') }}
),

child_health_focus AS (
    SELECT
        indicator_id,
        indicator_name,
        indicator_code,
        year,
        
        CASE
            WHEN LOWER(indicator_name) LIKE '%stunting%' THEN 'Stunting'
            WHEN LOWER(indicator_name) LIKE '%wasting%' THEN 'Wasting'
            WHEN LOWER(indicator_name) LIKE '%underweight%' THEN 'Underweight'
            WHEN LOWER(indicator_name) LIKE '%anaemia%' AND LOWER(indicator_name) LIKE '%child%' THEN 'Anaemia'
            WHEN LOWER(indicator_name) LIKE '%mortality%' AND LOWER(indicator_name) LIKE '%under%' THEN 'Mortality'
            WHEN LOWER(indicator_name) LIKE '%immunization%' OR LOWER(indicator_name) LIKE '%vaccine%' THEN 'Immunization'
            ELSE 'Other'
        END AS indicator_category,
        
        CASE
            WHEN LOWER(indicator_name) LIKE '%severe%' THEN 'Severe'
            WHEN LOWER(indicator_name) LIKE '%moderate%' THEN 'Moderate'
            ELSE 'Overall'
        END AS severity_level,
        
        sex,
        residence,
        wealth_quintile,
        
        value AS indicator_value,
        value_lower_ci,
        value_upper_ci,
        
        CASE 
            WHEN value_upper_ci IS NOT NULL AND value_lower_ci IS NOT NULL 
            THEN value_upper_ci - value_lower_ci
            ELSE NULL
        END AS ci_width,
        
        is_missing_value,
        has_data_quality_issue,
        
        dbt_updated_at
        
    FROM staged_indicators
    
    WHERE 
        LOWER(indicator_name) LIKE '%child%'
        OR LOWER(indicator_name) LIKE '%stunting%'
        OR LOWER(indicator_name) LIKE '%wasting%'
        OR LOWER(indicator_name) LIKE '%underweight%'
        OR LOWER(indicator_name) LIKE '%under-5%'
        OR LOWER(indicator_name) LIKE '%under 5%'
        OR LOWER(indicator_name) LIKE '%anaemia%'
        OR (LOWER(indicator_name) LIKE '%mortality%' AND LOWER(indicator_name) LIKE '%under%')
)

SELECT 
    *,
    CASE
        WHEN is_missing_value THEN 0
        WHEN has_data_quality_issue THEN 50
        WHEN ci_width IS NULL THEN 70
        WHEN ci_width > 20 THEN 80
        ELSE 100
    END AS data_quality_score
    
FROM child_health_focus
WHERE indicator_category != 'Other'
ORDER BY year DESC, indicator_category

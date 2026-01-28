WITH fact_table AS (
    SELECT * FROM {{ ref('fact_child_health') }}
),

time_dim AS (
    SELECT * FROM {{ ref('dim_time') }}
),

demographics AS (
    SELECT * FROM {{ ref('dim_demographics') }}
),

latest_year_data AS (
    SELECT
        f.indicator_category,
        f.indicator_name,
        d.sex_label,
        d.residence_label,
        d.wealth_quintile_label,
        f.indicator_value,
        f.percentage_change,
        f.trend_direction,
        t.year,
        t.is_current_year,
        d.is_low_wealth,
        d.is_rural
    FROM fact_table f
    JOIN time_dim t ON f.time_key = t.year
    JOIN demographics d ON f.demographic_key = d.demographic_key
    WHERE t.is_recent_5y = TRUE
),

category_aggregates AS (
    SELECT
        indicator_category,
        sex_label,
        residence_label,
        wealth_quintile_label,
        
        ROUND(AVG(indicator_value), 2) AS avg_indicator_value,
        ROUND(MIN(indicator_value), 2) AS min_indicator_value,
        ROUND(MAX(indicator_value), 2) AS max_indicator_value,
        
        ROUND(MAX(indicator_value) - MIN(indicator_value), 2) AS value_range,
        
        AVG(percentage_change) AS avg_percentage_change,
        
        COUNT(*) AS measurement_count,
        
        MAX(year) AS most_recent_year,
        
        MAX(is_low_wealth::INT)::BOOLEAN AS includes_low_wealth,
        MAX(is_rural::INT)::BOOLEAN AS includes_rural
        
    FROM latest_year_data
    GROUP BY 
        indicator_category,
        sex_label,
        residence_label,
        wealth_quintile_label
),

equity_gaps AS (
    SELECT
        ld1.indicator_category,
        ld1.year,
        ld1.sex_label,
        
        MAX(CASE WHEN ld1.residence_label = 'Urban' THEN ld1.indicator_value END) AS urban_value,
        
        MAX(CASE WHEN ld1.residence_label = 'Rural' THEN ld1.indicator_value END) AS rural_value,
        
        MAX(CASE WHEN ld1.residence_label = 'Urban' THEN ld1.indicator_value END) - 
        MAX(CASE WHEN ld1.residence_label = 'Rural' THEN ld1.indicator_value END) AS urban_rural_gap
        
    FROM latest_year_data ld1
    WHERE ld1.residence_label IN ('Urban', 'Rural')
      AND ld1.wealth_quintile_label = 'All Wealth Groups'
    GROUP BY 
        ld1.indicator_category,
        ld1.year,
        ld1.sex_label
    HAVING 
        MAX(CASE WHEN ld1.residence_label = 'Urban' THEN ld1.indicator_value END) IS NOT NULL
        AND MAX(CASE WHEN ld1.residence_label = 'Rural' THEN ld1.indicator_value END) IS NOT NULL
)

SELECT
    ca.indicator_category,
    ca.sex_label,
    ca.residence_label,
    ca.wealth_quintile_label,
    ca.avg_indicator_value,
    ca.min_indicator_value,
    ca.max_indicator_value,
    ca.value_range,
    ca.avg_percentage_change,
    ca.measurement_count,
    ca.most_recent_year,
    
    ca.includes_low_wealth,
    ca.includes_rural,
    
    eg.urban_value,
    eg.rural_value,
    eg.urban_rural_gap,
    
    CASE
        WHEN ABS(eg.urban_rural_gap) >= 10 THEN 'Large disparity'
        WHEN ABS(eg.urban_rural_gap) >= 5 THEN 'Moderate disparity'
        WHEN ABS(eg.urban_rural_gap) >= 1 THEN 'Small disparity'
        WHEN eg.urban_rural_gap IS NOT NULL THEN 'Minimal disparity'
        ELSE 'No data'
    END AS disparity_level,
    
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM category_aggregates ca
LEFT JOIN equity_gaps eg
    ON ca.indicator_category = eg.indicator_category
    AND ca.sex_label = eg.sex_label
    AND ca.most_recent_year = eg.year

ORDER BY 
    ca.indicator_category,
    ca.residence_label,
    ca.wealth_quintile_label
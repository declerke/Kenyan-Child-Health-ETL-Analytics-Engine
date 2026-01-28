SELECT 
    t.year,
    f.indicator_value as stunting_percentage,
    f.value_lower_ci as lower_bound,
    f.value_upper_ci as upper_bound
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE f.indicator_category = 'Stunting'
  AND d.is_total_population = TRUE
  AND f.severity_level = 'Overall'
ORDER BY t.year;

SELECT 
    t.year,
    d.sex_label,
    f.indicator_value as mortality_rate
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE f.indicator_category = 'Mortality'
  AND d.is_total_residence = TRUE
  AND d.is_total_wealth = TRUE
ORDER BY t.year, d.sex_label;

SELECT 
    t.year,
    f.indicator_category,
    AVG(f.indicator_value) as avg_value
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE d.is_total_population = TRUE
  AND f.severity_level = 'Overall'
  AND t.year >= 2014
GROUP BY t.year, f.indicator_category
ORDER BY t.year, f.indicator_category;

SELECT 
    f.indicator_category,
    d.residence_label,
    AVG(f.indicator_value) as avg_value
FROM fact_child_health f
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
JOIN dim_time t ON f.time_key = t.year
WHERE d.residence_label IN ('Urban', 'Rural')
  AND d.sex_label = 'Both Sexes'
  AND d.is_total_wealth = TRUE
  AND t.is_recent_5y = TRUE
  AND f.severity_level = 'Overall'
GROUP BY f.indicator_category, d.residence_label
ORDER BY f.indicator_category, d.residence_label;

SELECT 
    f.indicator_category,
    d.wealth_quintile_label,
    AVG(f.indicator_value) as avg_value
FROM fact_child_health f
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
JOIN dim_time t ON f.time_key = t.year
WHERE d.wealth_quintile_label != 'All Wealth Groups'
  AND d.sex_label = 'Both Sexes'
  AND d.is_total_residence = TRUE
  AND t.is_recent_5y = TRUE
  AND f.indicator_category IN ('Stunting', 'Underweight')
GROUP BY f.indicator_category, d.wealth_quintile_label
ORDER BY f.indicator_category, 
         CASE d.wealth_quintile_label
            WHEN 'Poorest (Q1)' THEN 1
            WHEN 'Second Quintile (Q2)' THEN 2
            WHEN 'Middle Quintile (Q3)' THEN 3
            WHEN 'Fourth Quintile (Q4)' THEN 4
            WHEN 'Richest (Q5)' THEN 5
         END;

SELECT 
    f.indicator_category,
    d.sex_label,
    AVG(f.indicator_value) as avg_value
FROM fact_child_health f
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
JOIN dim_time t ON f.time_key = t.year
WHERE d.sex_label IN ('Male', 'Female')
  AND d.is_total_residence = TRUE
  AND d.is_total_wealth = TRUE
  AND t.is_recent_5y = TRUE
GROUP BY f.indicator_category, d.sex_label
ORDER BY f.indicator_category, d.sex_label;

SELECT 
    f.indicator_value as current_stunting_rate,
    f.percentage_change as yoy_change,
    f.trend_direction,
    t.year as measurement_year
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE f.indicator_category = 'Stunting'
  AND d.is_total_population = TRUE
  AND f.severity_level = 'Overall'
ORDER BY t.year DESC
LIMIT 1;

SELECT 
    ROUND(AVG(f.indicator_value), 1) as avg_mortality_rate,
    COUNT(*) as data_points,
    MIN(t.year) as from_year,
    MAX(t.year) as to_year
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE f.indicator_category = 'Mortality'
  AND d.is_total_population = TRUE
  AND t.is_recent_5y = TRUE;

SELECT 
    indicator_category,
    urban_rural_gap as gap_percentage,
    urban_value,
    rural_value,
    disparity_level
FROM mart_kpi_summary
WHERE urban_rural_gap IS NOT NULL
  AND sex_label = 'Both Sexes'
ORDER BY ABS(urban_rural_gap) DESC
LIMIT 1;

SELECT 
    indicator_category,
    COUNT(CASE WHEN trend_direction = 'Decreasing' THEN 1 END) as improving_indicators,
    COUNT(CASE WHEN trend_direction = 'Increasing' THEN 1 END) as worsening_indicators,
    COUNT(CASE WHEN trend_direction = 'Stable' THEN 1 END) as stable_indicators,
    ROUND(AVG(percentage_change), 2) as avg_change_pct
FROM fact_child_health f
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE d.is_total_population = TRUE
  AND f.percentage_change IS NOT NULL
GROUP BY indicator_category
ORDER BY indicator_category;

SELECT 
    indicator_category,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_missing_value THEN 1 ELSE 0 END) as missing_values,
    ROUND(AVG(data_quality_score), 1) as avg_quality_score,
    COUNT(CASE WHEN is_significant_change THEN 1 END) as significant_changes
FROM fact_child_health
GROUP BY indicator_category
ORDER BY indicator_category;

SELECT 
    t.year,
    f.indicator_category,
    d.residence_label,
    f.indicator_value as current_value,
    f.previous_year_value,
    f.absolute_change,
    f.percentage_change,
    f.is_significant_change
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE d.residence_label IN ('Urban', 'Rural')
  AND d.sex_label = 'Both Sexes'
  AND d.is_total_wealth = TRUE
  AND f.percentage_change IS NOT NULL
ORDER BY t.year DESC, f.indicator_category, d.residence_label;

SELECT 
    f.indicator_name,
    t.year,
    d.residence_label,
    f.indicator_value,
    f.percentage_change,
    f.absolute_change
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE f.percentage_change < 0
  AND d.sex_label = 'Both Sexes'
  AND t.is_recent_5y = TRUE
ORDER BY f.percentage_change ASC
LIMIT 5;

SELECT 
    t.year,
    f.indicator_name,
    f.indicator_value as severe_percentage,
    d.residence_label
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
WHERE f.severity_level = 'Severe'
  AND d.sex_label = 'Both Sexes'
  AND d.is_total_wealth = TRUE
ORDER BY t.year DESC, f.indicator_name;

WITH indicator_pivot AS (
    SELECT 
        t.year,
        d.demographic_key,
        MAX(CASE WHEN f.indicator_category = 'Stunting' THEN f.indicator_value END) as stunting,
        MAX(CASE WHEN f.indicator_category = 'Wasting' THEN f.indicator_value END) as wasting,
        MAX(CASE WHEN f.indicator_category = 'Underweight' THEN f.indicator_value END) as underweight,
        MAX(CASE WHEN f.indicator_category = 'Anaemia' THEN f.indicator_value END) as anaemia
    FROM fact_child_health f
    JOIN dim_time t ON f.time_key = t.year
    JOIN dim_demographics d ON f.demographic_key = d.demographic_key
    WHERE f.severity_level = 'Overall'
    GROUP BY t.year, d.demographic_key
)
SELECT * FROM indicator_pivot
WHERE stunting IS NOT NULL 
  AND wasting IS NOT NULL
ORDER BY year DESC;

SELECT 
    t.year,
    COUNT(DISTINCT f.indicator_category) as indicators_count,
    COUNT(DISTINCT CONCAT(d.sex, d.residence, d.wealth_quintile)) as demographic_combinations,
    COUNT(*) as total_measurements,
    ROUND(AVG(f.data_quality_score), 1) as avg_quality
FROM fact_child_health f
JOIN dim_time t ON f.time_key = t.year
JOIN dim_demographics d ON f.demographic_key = d.demographic_key
GROUP BY t.year
ORDER BY t.year DESC;

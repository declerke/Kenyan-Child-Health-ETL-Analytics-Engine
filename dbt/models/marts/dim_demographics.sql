WITH dimension_combinations AS (
    SELECT DISTINCT
        sex,
        residence,
        wealth_quintile
    FROM {{ ref('stg_health_indicators') }}
    WHERE sex IS NOT NULL
      AND residence IS NOT NULL
      AND wealth_quintile IS NOT NULL
),

enriched_dimensions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY sex, residence, wealth_quintile) AS demographic_key,
        
        sex,
        residence,
        wealth_quintile,
        
        CASE sex
            WHEN 'male' THEN 'Male'
            WHEN 'female' THEN 'Female'
            WHEN 'both' THEN 'Both Sexes'
            ELSE INITCAP(sex)
        END AS sex_label,
        
        CASE residence
            WHEN 'urban' THEN 'Urban'
            WHEN 'rural' THEN 'Rural'
            WHEN 'total' THEN 'All Areas'
            ELSE INITCAP(residence)
        END AS residence_label,
        
        CASE wealth_quintile
            WHEN 'q1' THEN 'Poorest (Q1)'
            WHEN 'q2' THEN 'Second Quintile (Q2)'
            WHEN 'q3' THEN 'Middle Quintile (Q3)'
            WHEN 'q4' THEN 'Fourth Quintile (Q4)'
            WHEN 'q5' THEN 'Richest (Q5)'
            WHEN 'total' THEN 'All Wealth Groups'
            ELSE INITCAP(wealth_quintile)
        END AS wealth_quintile_label,
        
        CASE WHEN sex = 'both' THEN TRUE ELSE FALSE END AS is_total_sex,
        CASE WHEN residence = 'total' THEN TRUE ELSE FALSE END AS is_total_residence,
        CASE WHEN wealth_quintile = 'total' THEN TRUE ELSE FALSE END AS is_total_wealth,
        
        CASE 
            WHEN sex = 'both' AND residence = 'total' AND wealth_quintile = 'total' 
            THEN TRUE 
            ELSE FALSE 
        END AS is_total_population,
        
        CASE 
            WHEN wealth_quintile IN ('q1', 'q2') THEN TRUE
            ELSE FALSE 
        END AS is_low_wealth,
        
        CASE 
            WHEN residence = 'rural' THEN TRUE
            ELSE FALSE
        END AS is_rural,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM dimension_combinations
)

SELECT * FROM enriched_dimensions
ORDER BY demographic_key
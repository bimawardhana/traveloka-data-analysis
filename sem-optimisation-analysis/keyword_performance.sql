WITH

-- 1) PREPARING CURRENCY CONVERSION DATA

-- a) unnest conversion data
unnest_conversion_data AS (
SELECT 
    approved_timestamp, 
    conversion_data.source_currency,
    conversion_data.target
FROM 
    `tvlk-data-mkt-prod.datamart.conversion_table`, 
    UNNEST(conversion_data) AS conversion_data),

-- b) final conversion data (second unnest)
conversion_data AS (
SELECT
    source_currency,
    target.currency AS target_currency,
    target.exchange_rates AS exchange_rate,
    ROW_NUMBER() OVER (PARTITION BY source_currency, target.currency ORDER BY source_currency DESC) AS rank
FROM
    unnest_conversion_data, UNNEST(target) AS target),

-- 2) TOTAL FARE PER KEYWORDS
total_fare_per_keywords AS (
SELECT
    DATE(CAST(issued_time_wib AS TIMESTAMP)) AS issued_date_wib,
    CriteriaId AS keywords_id,
    CriteriaParameters AS keywords,
    SUM(total_fare * conversion_data.exchange_rate) AS total_fare
FROM 
    `tvlk-data-mkt-prod.s3_denorm.hotel_edw_hotel_issued_mkt_attr_utc7` AS denorm
    --`tvlk-data-mkt-prod.s3_denorm.flight_issued_mkt_attr_utc7` AS denorm 
    LEFT JOIN `tvlk-data-mkt-prod.google_adwords_traveloka.p_ClickStats_5442152622` AS click
    ON LOWER(REGEXP_EXTRACT(denorm.last_paid_url, '&gclid=(.*)')) = LOWER(click.GclId)
    LEFT JOIN conversion_data
    ON conversion_data.source_currency = denorm.currency_id
       AND conversion_data.target_currency = 'USD'
       AND conversion_data.rank = 1
WHERE
    DATE(denorm._PARTITIONTIME) BETWEEN '2019-07-01' AND '2019-09-30'
    AND DATE(click._PARTITIONTIME) BETWEEN '2019-01-01' AND '2019-09-30'
    AND LOWER(click.AccountDescriptiveName) = 'sem hotel indonesia - id'           
    -- AND LOWER(click.AccountDescriptiveName) = 'sem flight indonesia - id'
    AND LOWER(denorm.country_id) = 'id'
GROUP BY
    1,
    2,
    3),

-- 3) COST PERFORMANCE PER KEYWORDS

dim AS (
SELECT 
    DISTINCT 
    CriterionId, 
    Criteria
FROM 
    `tvlk-data-mkt-prod.google_adwords_traveloka.p_Criteria_5442152622` 
WHERE 
    DATE(_PARTITIONTIME) BETWEEN "2019-07-01" AND "2019-09-30"),
    
cost_per_keywords AS (
SELECT
    stats.Date as date,
    stats.CriterionId keywords_id,
    dim.Criteria AS keywords,
    SUM((stats.cost/1000000) * conversion_data.exchange_rate) AS total_cost
FROM
    `tvlk-data-mkt-prod.google_adwords_traveloka.p_CriteriaStats_5442152622` AS stats
    LEFT JOIN dim
    ON stats.CriterionId = dim.CriterionId
    LEFT JOIN conversion_data
    ON conversion_data.source_currency = 'THB'
       AND conversion_data.target_currency = 'USD'
       AND conversion_data.rank = 1
WHERE
    DATE(stats._PARTITIONTIME) BETWEEN "2019-07-01" AND "2019-09-30" 
    -- AND stats.ExternalCustomerId = 6204864059
    AND stats.ExternalCustomerId = 7670502067
GROUP BY
    1,
    2,
    3)

SELECT
    DATE_TRUNC(COALESCE(cost_per_keywords.date, total_fare_per_keywords.issued_date_wib), MONTH) AS month,
    COALESCE(cost_per_keywords.keywords_id, total_fare_per_keywords.keywords_id) AS keywords_id,
    COALESCE(cost_per_keywords.keywords, total_fare_per_keywords.keywords) AS keywords,
    SUM(COALESCE(cost_per_keywords.total_cost, 0)) AS total_cost,
    SUM(COALESCE(total_fare_per_keywords.total_fare, 0)) AS total_fare
FROM
    cost_per_keywords FULL OUTER JOIN total_fare_per_keywords 
    ON cost_per_keywords.date = total_fare_per_keywords.issued_date_wib 
       AND cost_per_keywords.keywords_id = total_fare_per_keywords.keywords_id 
       AND cost_per_keywords.keywords = total_fare_per_keywords.keywords 
GROUP BY
    1,
    2,
    3
ORDER BY
    1,
    2,
    3
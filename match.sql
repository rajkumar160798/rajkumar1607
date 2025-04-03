WITH MonthlySessionData AS (
    SELECT
        CAST(year_month AS STRING) AS visit_month,
        individual_analytics_identifier,
        ah_successful_login,
        web_mobile,
        first_web_visit_date_time,
        first_mobile_visit_date_time,
        PARSE_DATE('%Y-%m-%d %H:%M:%S', visit_start_date_time) AS visit_start_date,
        PARSE_DATE('%Y-%m-%d %H:%M:%S', first_web_visit_date_time) AS first_web_visit_date,
        PARSE_DATE('%Y-%m-%d %H:%M:%S', first_mobile_visit_date_time) AS first_mobile_visit_date
    FROM
        your_bigquery_table -- Replace with your actual table name
),
MonthlyAggregations AS (
    SELECT
        visit_month,
        COUNT(DISTINCT individual_analytics_identifier) AS distinct_users,
        SUM(ah_successful_login) AS total_successful_logins,
        COUNTIF(web_mobile = 'web') AS web_sessions,
        COUNTIF(web_mobile = 'mobile') AS mobile_sessions,
        COUNT(session_id) AS total_sessions, -- Assuming session_id is unique per session
        COUNT(DISTINCT CASE
            WHEN FORMAT_DATE('%Y%m', first_web_visit_date) = visit_month THEN individual_analytics_identifier
            ELSE NULL
        END) AS new_web_users,
        COUNT(DISTINCT CASE
            WHEN FORMAT_DATE('%Y%m', first_mobile_visit_date) = visit_month THEN individual_analytics_identifier
            ELSE NULL
        END) AS new_mobile_users
    FROM
        MonthlySessionData
    GROUP BY
        visit_month
),
WaterfallData AS (
    SELECT
        visit_month,
        distinct_users,
        LAG(distinct_users, 1, 0) OVER (ORDER BY visit_month) AS previous_month_users
    FROM
        MonthlyAggregations
)
SELECT
    ma.visit_month,
    wd.previous_month_users AS waterfall_start_value,
    (ma.distinct_users - wd.previous_month_users) AS waterfall_change,
    ma.distinct_users AS waterfall_end_value,
    ma.total_successful_logins,
    ma.web_sessions,
    ma.mobile_sessions,
    ma.total_sessions,
    ma.new_web_users,
    ma.new_mobile_users
FROM
    MonthlyAggregations ma
JOIN
    WaterfallData wd ON ma.visit_month = wd.visit_month
ORDER BY
    ma.visit_month;
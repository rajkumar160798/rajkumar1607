WITH MonthlyData AS (
    -- Select the month and count distinct users for that month
    SELECT
        CAST(year_month AS STRING) AS visit_month,
        COUNT(DISTINCT individual_analytics_identifier) AS distinct_users
    FROM
        your_bigquery_table -- Replace with your actual table name
    GROUP BY
        visit_month
),
LaggedData AS (
    -- Calculate the previous month's distinct user count
    SELECT
        visit_month,
        distinct_users,
        LAG(distinct_users, 1, 0) OVER (ORDER BY visit_month) AS previous_month_users
    FROM
        MonthlyData
)
-- Final SELECT to calculate the change and format for a waterfall
SELECT
    visit_month,
    previous_month_users AS start_value,
    (distinct_users - previous_month_users) AS change,
    distinct_users AS end_value
FROM
    LaggedData
ORDER BY
    visit_month;
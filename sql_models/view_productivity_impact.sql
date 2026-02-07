/*
   Project: Giga Texas AI/Robotics Workforce Scaling
   File: view_productivity_impact.sql
   Description: Calculates 'Time-to-Productivity' by correlating hire dates with production output.
   Business Logic:
   - Aggregates hiring velocity by month and department.
   - Calculates daily 'Units Per Head' (UPH) efficiency metric.
   - Flags days with "Low Efficiency" (below 90% of average) to identify ramp-up bottlenecks.
*/

CREATE OR REPLACE VIEW `driiiportfolio.analytics_staging.v_workforce_productivity_impact` AS

WITH daily_productivity AS (
    SELECT
        date,
        units_produced,
        actual_staffing,
        -- Calculate efficiency: Units produced per employee
        SAFE_DIVIDE(units_produced, actual_staffing) AS units_per_head
    FROM `driiiportfolio.analytics_staging.manufacturing_production_raw`
),

hiring_velocity AS (
    SELECT
        department,
        DATE_TRUNC(hire_date, MONTH) AS hire_month,
        COUNT(employee_id) AS new_hires,
        AVG(performance_rating) AS avg_performance
    FROM `driiiportfolio.analytics_staging.hros_employee_raw`
    GROUP BY 1, 2
)

SELECT
    p.date,
    h.department,
    h.new_hires,
    p.units_per_head,
    -- Dynamic efficiency flagging based on rolling average
    CASE
        WHEN p.units_per_head < (SELECT AVG(units_per_head) FROM daily_productivity) * 0.9 THEN 'Low Efficiency'
        ELSE 'Standard'
    END AS productivity_status
FROM daily_productivity p
-- Join production data with hiring data on the corresponding month
LEFT JOIN hiring_velocity h 
    ON DATE_TRUNC(p.date, MONTH) = h.hire_month
WHERE h.department = 'Robotics Manufacturing'; -- Focus on the Optimus line

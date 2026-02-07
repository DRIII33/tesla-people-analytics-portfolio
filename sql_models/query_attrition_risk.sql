/*
   Project: Giga Texas AI/Robotics Workforce Scaling
   File: query_attrition_risk.sql
   Description: Identifies high-value technical staff at risk of attrition.
   Logic:
   - Filters for top performers (Rating >= 4).
   - Filters for 'High' flight risk status.
   - Targets critical departments: AI Operations & Robotics Manufacturing.
   - Orders by commute distance to highlight those most affected by RTO policies.
*/

SELECT
    employee_id,
    department,
    performance_rating,
    attrition_risk,
    commute_distance
FROM `driiiportfolio.analytics_staging.hros_employee_raw`
WHERE 
    performance_rating >= 4
    AND attrition_risk = 'High'
    AND department IN ('AI Operations', 'Robotics Manufacturing')
ORDER BY commute_distance DESC;

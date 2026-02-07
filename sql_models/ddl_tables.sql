/* Project: Giga Texas AI/Robotics Workforce Scaling
   File: ddl_tables.sql
   Description: Schema definitions for raw data ingestion.
   Author: Daniel Rodriguez III
   Target System: Google BigQuery
*/

-- 1. ATS (Applicant Tracking System) Raw Table
-- Captures the recruiting funnel velocity for AI/Robotics roles.
CREATE OR REPLACE TABLE `driiiportfolio.analytics_staging.ats_recruiting_raw` (
    candidate_id STRING OPTIONS(description="Unique identifier for the applicant"),
    job_req_id STRING OPTIONS(description="Requisition ID linking to specific technical roles"),
    application_date DATE OPTIONS(description="Date of initial application"),
    current_stage STRING OPTIONS(description="Current status in the hiring funnel"),
    technical_score FLOAT64 OPTIONS(description="0-100 score from technical assessment"),
    source STRING OPTIONS(description="Recruiting channel (e.g., LinkedIn, Referral)")
);

-- 2. HROS (HR Operating System) Raw Table
-- Stores employee lifecycle data and attrition risk factors.
CREATE OR REPLACE TABLE `driiiportfolio.analytics_staging.hros_employee_raw` (
    employee_id STRING OPTIONS(description="Unique employee ID"),
    department STRING OPTIONS(description="Functional unit (e.g., AI Operations, Robotics Mfg)"),
    hire_date DATE OPTIONS(description="Start date for tenure calculation"),
    performance_rating INT64 OPTIONS(description="1-5 Performance Score"),
    attrition_risk STRING OPTIONS(description="Predictive risk label: Low/Medium/High"),
    commute_distance FLOAT64 OPTIONS(description="Distance in miles from Giga Texas")
);

-- 3. Manufacturing Production Raw Table
-- Operational output data for the Optimus V4 line.
CREATE OR REPLACE TABLE `driiiportfolio.analytics_staging.manufacturing_production_raw` (
    date DATE OPTIONS(description="Production date"),
    station_id STRING OPTIONS(description="Specific assembly station identifier"),
    actual_staffing INT64 OPTIONS(description="Headcount present on shift"),
    units_produced INT64 OPTIONS(description="Total approved units completed"),
    defect_rate FLOAT64 OPTIONS(description="Percentage of units failing QA")
);

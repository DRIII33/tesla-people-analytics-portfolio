#INSTALL PACKAGES
!pip install pandas
!pip install numpy
!pip install datetime

# ==========================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ==========================================
# CONFIGURATION & SEEDING
# ==========================================
# Setting a seed ensures reproducibility for stakeholders/reviewers
np.random.seed(42)
random.seed(42)

# Parameters defined in project documentation [cite: 30, 31]
N_CANDIDATES = 1500
N_EMPLOYEES = 500
TARGET_DAILY_PRODUCTION = 27000  # Optimus units [cite: 69]

print("Initializing Giga Texas Data Generation Pipeline...")

# ==========================================
# 1. ATS DATASET (Recruiting Pipeline)
# ==========================================
# Generates applicant data with intentional "dirty" quality issues [cite: 33]

def generate_ats_data():
    base_date = datetime(2025, 1, 1)
    
    # Generate base dates
    date_list = [base_date + timedelta(days=np.random.randint(0, 400)) for _ in range(N_CANDIDATES)]
    
    # SILOED SYSTEM NOISE INJECTION:
    # Simulating inconsistent date formats often found in legacy ATS exports.
    # We will convert 10% of dates to string formats (e.g., "MM/DD/YYYY" vs "YYYY-MM-DD")
    dirty_dates = []
    for d in date_list:
        if random.random() < 0.1:
            # Inject noise: formatted as string US style
            dirty_dates.append(d.strftime("%m/%d/%Y"))
        else:
            dirty_dates.append(d)

    ats_data = {
        'candidate_id': [f"CAND_{i:05d}" for i in range(1, N_CANDIDATES + 1)],
        'job_req_id': np.random.choice(['REQ_AI_001', 'REQ_BOT_002', 'REQ_MFG_003'], N_CANDIDATES),
        'application_date': dirty_dates, # Includes mixed types (datetime and str)
        'current_stage': np.random.choice(
            ['Applied', 'Screening', 'Technical Assessment', 'Onsite', 'Offer', 'Hired'], 
            N_CANDIDATES, 
            p=[0.4, 0.3, 0.15, 0.05, 0.05, 0.05] # Funnel probability [cite: 40]
        ),
        'technical_score': np.random.uniform(0, 100, N_CANDIDATES), # 0-100 assessment [cite: 42]
        'source': np.random.choice(['LinkedIn', 'Referral', 'Career Site', 'Agency'], N_CANDIDATES)
    }
    
    df = pd.DataFrame(ats_data)
    
    # DATA QUALITY ISSUE INJECTION:
    # Introduce nulls in technical_score (e.g., candidates who skipped the test) [cite: 46]
    df.loc[df.sample(frac=0.1).index, 'technical_score'] = np.nan
    
    return df

# ==========================================
# 2. HROS DATASET (Employee Lifecycle)
# ==========================================
# Reflects Austin market dynamics and production roles [cite: 48-49]

def generate_hros_data():
    hros_data = {
        'employee_id': [f"EMP_{i:05d}" for i in range(1, N_EMPLOYEES + 1)],
        'department': np.random.choice(
            ['AI Operations', 'Robotics Manufacturing', 'Supply Chain', 'FSD Engineering'], 
            N_EMPLOYEES
        ),
        # Hire dates spanning back to 2024 to allow for tenure calculation [cite: 54]
        'hire_date': [datetime(2024, 1, 1) + timedelta(days=np.random.randint(0, 700)) for _ in range(N_EMPLOYEES)],
        'performance_rating': np.random.choice(
            [1, 2, 3, 4, 5], 
            N_EMPLOYEES, 
            p=[0.05, 0.1, 0.5, 0.25, 0.1] # Bell curve distribution [cite: 58]
        ),
        'attrition_risk': np.random.choice(
            ['Low', 'Medium', 'High'], 
            N_EMPLOYEES, 
            p=[0.7, 0.2, 0.1]
        ),
        # Commute distance is a key attrition driver in Austin [cite: 61]
        'commute_distance': np.random.normal(15, 8, N_EMPLOYEES).round(1) 
    }
    
    return pd.DataFrame(hros_data)

# ==========================================
# 3. PRODUCTION DATASET (Shop Floor)
# ==========================================
# Integration point for calculating "Time-to-Productivity" [cite: 63]

def generate_production_data():
    dates = pd.date_range(start='2025-01-01', periods=365, freq='D')
    
    prod_data = {
        'date': dates,
        'station_id': 'OPTIMUS_V4_ASSY_01', # [cite: 67]
        'actual_staffing': np.random.randint(18, 25, 365),
        # Random fluctuation around the target of 27k units/day [cite: 69]
        'units_produced': np.random.normal(TARGET_DAILY_PRODUCTION, 1000, 365).astype(int),
        'defect_rate': np.random.uniform(0.01, 0.05, 365).round(4) # [cite: 71]
    }
    
    return pd.DataFrame(prod_data)

# ==========================================
# DATA CLEANING PIPELINE
# ==========================================
# Standardizing IDs, handling nulls, and fixing date formats [cite: 75]

def clean_people_data(df_ats, df_hros):
    print("Running Data Cleaning Pipeline...")
    
    # 1. Handle Missing Technical Scores
    # Strategy: Impute with median to avoid skewing the "average talent bar" [cite: 79]
    median_score = df_ats['technical_score'].median()
    df_ats['technical_score'] = df_ats['technical_score'].fillna(median_score)
    
    # 2. Fix Inconsistent Date Formats (The "Siloed Noise" Fix)
    # Uses errors='coerce' to handle the mixed string/datetime types we injected
    df_ats['application_date'] = pd.to_datetime(df_ats['application_date'], errors='coerce')
    
    # 3. Standardization
    # Ensure IDs are strings and stripped of whitespace
    df_ats['candidate_id'] = df_ats['candidate_id'].astype(str).str.strip()
    df_hros['employee_id'] = df_hros['employee_id'].astype(str).str.strip()
    
    print("Cleaning complete. Nulls handled and dates normalized.")
    return df_ats, df_hros

# ==========================================
# MAIN EXECUTION
# ==========================================

if __name__ == "__main__":
    # 1. Generate Raw Data
    df_ats_raw = generate_ats_data()
    df_hros_raw = generate_hros_data()
    df_prod_raw = generate_production_data()
    
    print(f"Generated ATS Data: {df_ats_raw.shape[0]} records")
    print(f"Generated HROS Data: {df_hros_raw.shape[0]} records")
    
    # 2. Clean Data
    df_ats_clean, df_hros_clean = clean_people_data(df_ats_raw, df_hros_raw)
    
    # 3. Export to CSV (Simulating the BigQuery Load)
    # These files will be uploaded to the repository
    df_ats_clean.to_csv('ats_data.csv', index=False)
    df_hros_clean.to_csv('hros_data.csv', index=False)
    df_prod_raw.to_csv('production_data.csv', index=False)
    
    print("Files exported successfully: ats_data.csv, hros_data.csv, production_data.csv")
    print("Ready for BigQuery ingestion.")

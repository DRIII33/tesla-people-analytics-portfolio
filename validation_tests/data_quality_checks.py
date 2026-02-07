## Aggregated Data Quality Checks Script

Below is an aggregated Python script that consolidates all the data quality validation checks performed previously. This script loads the `ats_data.csv`, `hros_data.csv`, and `production_data.csv` files, and then systematically checks for:

1.  **Null values** in all columns of each DataFrame.
2.  **Full row duplicate records** in each DataFrame.
3.  **Duplicates in key identifier columns** (`candidate_id`, `employee_id`, `date`) for their respective DataFrames.

This script can be used for documenting the data quality checks in a GitHub repository (e.g., `validation_tests/data_quality_checks.py`).

---
import pandas as pd

# Load Data for Validation
df_ats = pd.read_csv('ats_data.csv')
df_hros = pd.read_csv('hros_data.csv')
df_production = pd.read_csv('production_data.csv')

print("Loaded DataFrames:\n")
print("ATS Data (df_ats):")
print(df_ats.head())
print("\nHROS Data (df_hros):")
print(df_hros.head())
print("\nProduction Data (df_production):")
print(df_production.head())

# Data Quality Check: Null Values
print("\n========================================")
print("Data Quality Check: Null Values")
print("========================================")

print("\nChecking for Null Values in df_ats:")
print(df_ats.isnull().sum())

print("\nChecking for Null Values in df_hros:")
print(df_hros.isnull().sum())

print("\nChecking for Null Values in df_production:")
print(df_production.isnull().sum())

# Data Quality Check: Duplicate Records (Full Row)
print("\n========================================")
print("Data Quality Check: Duplicate Records (Full Row)")
print("========================================")

print("\nChecking for Duplicate Records in df_ats:")
print(df_ats.duplicated().sum())

print("\nChecking for Duplicate Records in df_hros:")
print(df_hros.duplicated().sum())

print("\nChecking for Duplicate Records in df_production:")
print(df_production.duplicated().sum())

# Data Quality Check: Duplicate Records (Key Identifier Columns)
print("\n========================================")
print("Data Quality Check: Duplicate Records (Key Identifier Columns)")
print("========================================")

print("\nChecking for Duplicate 'candidate_id' in df_ats:")
print(df_ats['candidate_id'].duplicated().sum())

print("\nChecking for Duplicate 'employee_id' in df_hros:")
print(df_hros['employee_id'].duplicated().sum())

print("\nChecking for Duplicate 'date' in df_production:")
print(df_production['date'].duplicated().sum())

print("\nData quality checks completed.")

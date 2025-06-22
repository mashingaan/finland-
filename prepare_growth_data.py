import pandas as pd
import sqlite3
import io
import json
from pathlib import Path

# Load data from CSVs
commodities_df = pd.read_csv("upload/commodities.csv")

# Read countries.csv, skipping the problematic second header row
with open("upload/countries.csv", "r") as f:
    lines = f.readlines()
# Find the second occurrence of the header and remove it and subsequent lines
    try:
        second_header_index = lines.index("id,text,reporterCodeIsoAlpha3,world_part\n", 1) # Start searching from index 1
        countries_data = lines[:second_header_index] + lines[second_header_index+1:]
    except ValueError:
        countries_data = lines # No second header found, use all lines

countries_df = pd.read_csv(io.StringIO("".join(countries_data)))

# Load data from SQLite
conn = sqlite3.connect("upload/Finland.db")
trade_db_df = pd.read_sql_query("SELECT * FROM trade", conn)
conn.close()

# Ensure 'id' column in countries_df is of integer type for merging
countries_df["id"] = countries_df["id"].astype(int)

# Merge dataframes
trade_df = trade_db_df.copy()
trade_df = trade_df.merge(commodities_df, left_on="cmdCode", right_on="id", how="left")

# Rename columns for clarity
trade_df.rename(columns={
    "text": "commodity_name", 
    "period": "year"
}, inplace=True)

# Convert relevant columns to appropriate types
trade_df["year"] = trade_df["year"].astype(int)
trade_df["primaryValue"] = trade_df["primaryValue"].astype(float)
trade_df["value_bln"] = trade_df["primaryValue"] / 1_000_000_000

# Handle NaN values in categorical columns
trade_df["commodity_name"] = trade_df["commodity_name"].fillna("Неизвестно")

# Filter flowCode and drop duplicates
trade_df = trade_df[trade_df.flowCode.isin(["X","M"])].copy()
trade_df = trade_df.drop_duplicates(["year","partnerCode","cmdCode","flowCode"]).copy()

# Calculate growth data for last 3 years
latest_year = trade_df["year"].max()
print(f"Latest year: {latest_year}")

# Create dataframes for 2023 and 2021
df_2023 = trade_df[trade_df.year == latest_year]
df_2021 = trade_df[trade_df.year == latest_year - 2]

print(f"Records in {latest_year}: {len(df_2023)}")
print(f"Records in {latest_year - 2}: {len(df_2021)}")

# Calculate export growth
exp_now = df_2023[df_2023.flowCode == "X"].groupby("commodity_name")["value_bln"].sum()
exp_old = df_2021[df_2021.flowCode == "X"].groupby("commodity_name")["value_bln"].sum()
exp_growth = (exp_now - exp_old).dropna().sort_values(ascending=False).head(5).reset_index()
exp_growth.rename(columns={"value_bln": "delta"}, inplace=True)

print("\nTop 5 Export Growth:")
print(exp_growth)

# Calculate import growth
imp_now = df_2023[df_2023.flowCode == "M"].groupby("commodity_name")["value_bln"].sum()
imp_old = df_2021[df_2021.flowCode == "M"].groupby("commodity_name")["value_bln"].sum()
imp_growth = (imp_now - imp_old).dropna().sort_values(ascending=False).head(5).reset_index()
imp_growth.rename(columns={"value_bln": "delta"}, inplace=True)

print("\nTop 5 Import Growth:")
print(imp_growth)

# Prepare growth data for dashboard
growth_data = {
    "export_growth": exp_growth.to_dict(orient="records"),
    "import_growth": imp_growth.to_dict(orient="records")
}

# Path to dashboard_data.json in the repository root
dashboard_file = Path(__file__).resolve().parent / "dashboard_data.json"

# Load existing dashboard data and add growth data
with open(dashboard_file, "r", encoding="utf-8") as f:
    dashboard_data = json.load(f)

dashboard_data.update(growth_data)

# Save updated data
with open(dashboard_file, "w", encoding="utf-8") as f:
    json.dump(dashboard_data, f, ensure_ascii=False, indent=4)

print("\nGrowth data added to dashboard_data.json")


import pandas as pd
import sqlite3
import io
import json

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
countries_db_df = pd.read_sql_query("SELECT * FROM countries", conn)
conn.close()

# Ensure 'id' column in countries_df is of integer type for merging
countries_df["id"] = countries_df["id"].astype(int)

# Merge dataframes
trade_df = trade_db_df.copy()
trade_df = trade_df.merge(commodities_df, left_on="cmdCode", right_on="id", how="left")
trade_df = trade_df.merge(countries_df, left_on="partnerCode", right_on="id", how="left")

# Rename columns for clarity
trade_df.rename(columns={
    "text_x": "commodity_name", 
    "text_y": "country_name", 
    "period": "year",
    "sector": "sector" # Ensure sector is present
}, inplace=True)

# Convert relevant columns to appropriate types
trade_df["year"] = trade_df["year"].astype(int)
trade_df["primaryValue"] = trade_df["primaryValue"].astype(float)

# --- Data for Dashboard --- 

dashboard_data = {}

# 1. Trade Dynamics
trade_dynamics = trade_df.groupby(["year", "flowCode"])["primaryValue"].sum().unstack().fillna(0)
trade_dynamics["balance"] = trade_dynamics["X"] - trade_dynamics["M"]
dashboard_data["trade_dynamics"] = trade_dynamics.reset_index().to_dict(orient="records")

# 2. Commodity Groups (TOP-10 export and import)
export_top10_commodities = trade_df[trade_df["flowCode"] == "X"].groupby("commodity_name")["primaryValue"].sum().nlargest(10)
import_top10_commodities = trade_df[trade_df["flowCode"] == "M"].groupby("commodity_name")["primaryValue"].sum().nlargest(10)
dashboard_data["top_export_commodities"] = export_top10_commodities.reset_index().to_dict(orient="records")
dashboard_data["top_import_commodities"] = import_top10_commodities.reset_index().to_dict(orient="records")

# 3. Economic Sectors
sector_trade = trade_df.groupby(["sector", "flowCode"])["primaryValue"].sum().unstack().fillna(0)
sector_trade["export_share"] = sector_trade["X"] / sector_trade["X"].sum()
sector_trade["import_share"] = sector_trade["M"] / sector_trade["M"].sum()
dashboard_data["economic_sectors"] = sector_trade.reset_index().to_dict(orient="records")

# 4. Trade Geography
regional_trade = trade_df.groupby(["world_part", "flowCode"])["primaryValue"].sum().unstack().fillna(0)
dashboard_data["trade_geography"] = regional_trade.reset_index().to_dict(orient="records")

# 5. Main Partner Countries (TOP-10 by total trade)
latest_year = trade_df["year"].max()
five_years_ago = latest_year - 4
recent_partner_trade = trade_df[trade_df["year"] >= five_years_ago]
partner_total_trade = recent_partner_trade.groupby("country_name")["primaryValue"].sum().nlargest(10)
dashboard_data["top_partner_countries"] = partner_total_trade.reset_index().to_dict(orient="records")

# Filters data
dashboard_data["years"] = sorted(trade_df["year"].unique().tolist())
dashboard_data["regions"] = sorted(trade_df["world_part"].dropna().unique().tolist()) # Handle NaN values
dashboard_data["countries"] = sorted(trade_df["country_name"].dropna().unique().tolist())
dashboard_data["commodities"] = sorted(trade_df["commodity_name"].dropna().unique().tolist())

# Save processed data to a JSON file
with open("dashboard_data.json", "w", encoding="utf-8") as f:
    json.dump(dashboard_data, f, ensure_ascii=False, indent=4)

print("Data prepared and saved to dashboard_data.json")


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

# Handle NaN values in categorical columns before getting unique values
trade_df["world_part"] = trade_df["world_part"].fillna("Неизвестно")
trade_df["country_name"] = trade_df["country_name"].fillna("Неизвестно")
trade_df["commodity_name"] = trade_df["commodity_name"].fillna("Неизвестно")
trade_df["sector"] = trade_df["sector"].fillna("Неизвестно")

# --- NEW: Filter flowCode and drop duplicates ---
trade_df = trade_df[trade_df.flowCode.isin(["X","M"])].copy()
trade_df = trade_df.drop_duplicates(["year","partnerCode","cmdCode","flowCode"]).copy()

# --- Data for Dashboard --- 

dashboard_data = {}

# 1. Trade Dynamics
trade_dynamics = trade_df.groupby(["year", "flowCode"])["primaryValue"].sum().unstack().fillna(0)
trade_dynamics["balance"] = trade_dynamics["X"] - trade_dynamics["M"]
dashboard_data["trade_dynamics"] = trade_dynamics.reset_index().to_dict(orient="records")

# 2. Commodity Groups (TOP-10 export and import)
export_top10_commodities = trade_df[trade_df["flowCode"] == "X"].groupby("commodity_name")["primaryValue"].sum().nlargest(10).reset_index()
import_top10_commodities = trade_df[trade_df["flowCode"] == "M"].groupby("commodity_name")["primaryValue"].sum().nlargest(10).reset_index()
dashboard_data["top_export_commodities"] = export_top10_commodities.to_dict(orient="records")
dashboard_data["top_import_commodities"] = import_top10_commodities.to_dict(orient="records")

# 3. Economic Sectors
sector_trade = trade_df.groupby(["sector", "flowCode"])["primaryValue"].sum().unstack().fillna(0)
sector_trade["export_share"] = sector_trade["X"] / sector_trade["X"].sum()
sector_trade["import_share"] = sector_trade["M"] / sector_trade["M"].sum()
dashboard_data["economic_sectors"] = sector_trade.reset_index().to_dict(orient="records")

# 4. Trade Geography
regional_trade = trade_df.groupby(["world_part", "flowCode"])["primaryValue"].sum().unstack().fillna(0)
regional_trade["export_share"] = regional_trade["X"] / regional_trade["X"].sum()
regional_trade["import_share"] = regional_trade["M"] / regional_trade["M"].sum()
dashboard_data["trade_geography"] = regional_trade.reset_index().to_dict(orient="records")

# 5. Main Partner Countries (TOP-10 by total trade)
latest_year = trade_df["year"].max()
five_years_ago = latest_year - 4

# Filter for the last 5 years
recent_partner_trade = trade_df[trade_df["year"] >= five_years_ago].copy()

# Group by country and flowCode, then pivot
partner_flow_sum = recent_partner_trade.groupby(["country_name", "flowCode"])["primaryValue"].sum().unstack(fill_value=0)

# Calculate balance and turnover
partner_flow_sum["balance"] = partner_flow_sum["X"] - partner_flow_sum["M"]
partner_flow_sum["turnover"] = partner_flow_sum["X"] + partner_flow_sum["M"]

# Sort by turnover and get top 10
top_10_partners = partner_flow_sum.nlargest(10, "turnover").reset_index()

# Convert to billion USD for consistency with app.py
top_10_partners["balance_bln"] = top_10_partners["balance"] / 1_000_000_000
top_10_partners["turnover_bln"] = top_10_partners["turnover"] / 1_000_000_000

dashboard_data["top_partner_countries"] = top_10_partners.to_dict(orient="records")

# 6. Trade with Russian Federation (last 5 years)
# Use partnerCode for Russia (643) and also check for 'Россия' and 'Российская Федерация'
russia_trade = trade_df[(trade_df["partnerCode"] == 643) | (trade_df["country_name"].isin(["Россия", "Российская Федерация", "Russian Federation"]))]
russia_trade_dynamics = russia_trade[russia_trade["year"] >= five_years_ago].groupby(["year", "flowCode"])["primaryValue"].sum().unstack().fillna(0)
russia_trade_dynamics["balance"] = russia_trade_dynamics["X"] - russia_trade_dynamics["M"]
dashboard_data["russia_trade_dynamics"] = russia_trade_dynamics.reset_index().to_dict(orient="records")

# 7. Changes in Trade Structure (10 years)
ten_years_ago = latest_year - 9

# Calculate average trade in first 5 years vs last 5 years of available data
min_year = trade_df["year"].min()

if latest_year - min_year >= 9: # Ensure at least 10 years of data for this analysis
    first_half_years = range(min_year, min_year + 5)
    second_half_years = range(latest_year - 4, latest_year + 1)

    trade_first_half = trade_df[trade_df["year"].isin(first_half_years)].groupby("commodity_name")["primaryValue"].sum()
    trade_second_half = trade_df[trade_df["year"].isin(second_half_years)].groupby("commodity_name")["primaryValue"].sum()

    combined_commodities = pd.DataFrame({"first_half": trade_first_half, "second_half": trade_second_half}).fillna(0)
    combined_commodities["change"] = combined_commodities["second_half"] - combined_commodities["first_half"]
    dashboard_data["declining_commodities"] = combined_commodities[combined_commodities["change"] < 0].sort_values(by="change").head(10).reset_index().to_dict(orient="records")

    trade_first_half_partners = trade_df[trade_df["year"].isin(first_half_years)].groupby("country_name")["primaryValue"].sum()
    trade_second_half_partners = trade_df[trade_df["year"].isin(second_half_years)].groupby("country_name")["primaryValue"].sum()

    combined_partners = pd.DataFrame({"first_half": trade_first_half_partners, "second_half": trade_second_half_partners}).fillna(0)
    combined_partners["change"] = combined_partners["second_half"] - combined_partners["first_half"]
    dashboard_data["declining_partners"] = combined_partners[combined_partners["change"] < 0].sort_values(by="change").head(10).reset_index().to_dict(orient="records")
else:
    dashboard_data["declining_commodities"] = []
    dashboard_data["declining_partners"] = []

# Save processed data to a JSON file
with open("dashboard_data.json", "w", encoding="utf-8") as f:
    json.dump(dashboard_data, f, ensure_ascii=False, indent=4)

print("Data prepared and saved to dashboard_data.json")



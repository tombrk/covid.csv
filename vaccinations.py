import requests
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

url = "https://raw.githubusercontent.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland/refs/heads/main/Deutschland_Landkreise_COVID-19-Impfungen.csv"

print(f"get {url}")
df = pd.read_csv(url, dtype={'LandkreisId_Impfort': str})

print("process vaccination data")
df['Impfdatum'] = pd.to_datetime(df['Impfdatum'])

# Rename columns to match desired output format
df_processed = df.rename(columns={
    'Impfdatum': 'date', 
    'LandkreisId_Impfort': 'ags',
    'Altersgruppe': 'age_group',
    'Impfschutz': 'stage',
    'Anzahl': 'new'
})

# Filter out ags with value "u" and ags 17000
df_processed = df_processed[df_processed['ags'] != 'u']
df_processed = df_processed[df_processed['ags'] != '17000']

# Sort by ags, age group, stage, and date for proper cumulative calculation
df_processed = df_processed.sort_values(['ags', 'age_group', 'stage', 'date'])

result = df_processed[['date', 'ags', 'age_group', 'stage', 'new']]

# Group by date and write separate files
grouped_data = list(result.groupby('date'))
for date_val, group_data in tqdm(grouped_data, desc="write csv"):
    date_obj = pd.to_datetime(date_val)
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    
    output_path = f"{year}/{month:02d}/{day:02d}/vaccinations.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    group_data.to_csv(output_path, index=False)

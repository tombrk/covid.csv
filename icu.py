# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests",
#     "pandas",
# ]
# ///
import requests
import pandas as pd
import os
import sys
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('date')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')
date_str = args.date
year = date_obj.year
month = date_obj.month
day = date_obj.day

output_path = f"{year}/{month:02d}/{day:02d}/icu.csv"

url = f"https://raw.githubusercontent.com/robert-koch-institut/Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland/refs/tags/{date_str}/Intensivregister_Landkreise_Kapazitaeten.csv"

print(f"get {url}")
response = requests.get(url)
df = pd.read_csv(url)

df_filtered = df[df['datum'] == date_str]

print("select columns")
daily_data = df_filtered[['datum', 'landkreis_id', 'intensivbetten_frei', 'intensivbetten_belegt']]
daily_data.rename(columns={'landkreis_id': 'ags'}, inplace=True)

print(f"write {output_path}")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
daily_data.to_csv(output_path, index=False)

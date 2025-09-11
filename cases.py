import requests
import pandas as pd
import os
import lzma
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

output_path = f"{year}/{month:02d}/{day:02d}/cases.csv"

base_url = "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/raw/refs/heads/main/Archiv/"
url = f"{base_url}{date_str}_Deutschland_SarsCov2_Infektionen.csv.xz"
filename = f"{date_str}.csv.xz"

print(f"get {url} > {filename}")
response = requests.get(url, stream=True)
with open(filename, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

print(f"unxz {filename} | pd.read_csv")
with lzma.open(filename, 'rt') as f:
    df = pd.read_csv(f, dtype={'IdLandkreis': str})

df_filtered = df[
    ((df['NeuerFall'].isin([1, -1])) | (df['NeuerTodesfall'].isin([1, -1]))) & 
    (df['Geschlecht'] != 'unbekannt')
]

print("group_by()")
daily_data = df_filtered.groupby(['IdLandkreis', 'Altersgruppe', 'Geschlecht', 'Refdatum']).agg({
    'AnzahlFall': 'sum',
    'AnzahlTodesfall': 'sum'
}).reset_index()

print(f"write {output_path}")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
daily_data.to_csv(output_path, index=False)
os.remove(filename)

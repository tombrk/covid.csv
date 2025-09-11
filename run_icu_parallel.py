from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from tqdm import tqdm
from icu import fetch_icu

def safe_fetch(date):
    try:
        fetch_icu(date)
    except Exception as e:
        print(f"Failed {date}: {e}")

dates = []
current = datetime(2022, 9, 30)
while current <= datetime(2025, 9, 10):
    dates.append(current.strftime('%Y-%m-%d'))
    current += timedelta(days=1)

with ThreadPoolExecutor(max_workers=5) as executor:
    list(tqdm(executor.map(safe_fetch, dates), total=len(dates)))

import os
import pandas as pd
import boto3
import zipfile
import time
import json
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# =============================
# CONFIG
# =============================

CHUNK_SIZE = 50
MAX_WORKERS = 10

TIMEFRAMES = ["1m","5m","15m","30m","1h","4h","1d","1w"]

START_YEAR = 2023
CURRENT_YEAR = int(time.strftime("%Y"))
CURRENT_MONTH = int(time.strftime("%m"))

BINANCE_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"

# =============================
# R2
# =============================

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
)

BUCKET = os.getenv("R2_BUCKET")

# =============================
# HELPERS
# =============================

def load_json(key, default):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except:
        return default


def save_json(key, data):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))


def get_existing(path):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=path)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except:
        return None


def upload(df, path):
    file = "/tmp/temp.parquet"
    df.to_parquet(file, index=False, compression="snappy")
    s3.upload_file(file, BUCKET, path)
    print(f"✅ {path}", flush=True)

# =============================
# CLEAN
# =============================

def clean_dataframe(df):
    df.columns = ["time","open","high","low","close","volume","_","_","_","_","_","_"]
    df = df[["time","open","high","low","close","volume"]]

    df = df[df["time"] != "open_time"]

    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df = df.dropna(subset=["time"])

    df["time"] = pd.to_datetime(df["time"], unit="ms")

    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# =============================
# SMART FETCH (INCREMENTAL)
# =============================

def fetch_binance(symbol, tf, existing):
    all_df = []

    # =============================
    # BACKFILL (FIRST TIME)
    # =============================
    if existing is None:
        print(f"Backfill {symbol} {tf}", flush=True)

        for year in range(START_YEAR, CURRENT_YEAR + 1):
            for month in range(1, 13):

                url = f"{BINANCE_BASE}/{symbol}/{tf}/{symbol}-{tf}-{year}-{str(month).zfill(2)}.zip"

                try:
                    res = requests.get(url, timeout=10)
                    if res.status_code != 200:
                        continue

                    with zipfile.ZipFile(BytesIO(res.content)) as z:
                        df = pd.read_csv(z.open(z.namelist()[0]), header=None)

                    df = clean_dataframe(df)

                    if not df.empty:
                        all_df.append(df)

                except:
                    continue

    # =============================
    # TRUE INCREMENTAL
    # =============================
    else:
        last_time = existing["time"].max()

        last_year = last_time.year
        last_month = last_time.month

        # 🔥 If already current month → skip
        if last_year == CURRENT_YEAR and last_month == CURRENT_MONTH:
            print(f"⏭️ Up-to-date {symbol} {tf}", flush=True)
            return None

        # only fetch missing months
        for year in range(last_year, CURRENT_YEAR + 1):
            start_month = last_month if year == last_year else 1
            end_month = CURRENT_MONTH if year == CURRENT_YEAR else 12

            for month in range(start_month, end_month + 1):

                url = f"{BINANCE_BASE}/{symbol}/{tf}/{symbol}-{tf}-{year}-{str(month).zfill(2)}.zip"

                try:
                    res = requests.get(url, timeout=10)
                    if res.status_code != 200:
                        continue

                    with zipfile.ZipFile(BytesIO(res.content)) as z:
                        df = pd.read_csv(z.open(z.namelist()[0]), header=None)

                    df = clean_dataframe(df)

                    df = df[df["time"] > last_time]

                    if not df.empty:
                        all_df.append(df)

                except:
                    continue

    return pd.concat(all_df) if all_df else None

# =============================
# PROCESS
# =============================

def process_symbol(symbol):
    sym = symbol["symbol"]

    for tf in TIMEFRAMES:
        path = f"binance/futures/{tf}/{sym}.parquet"
        existing = get_existing(path)

        df = fetch_binance(sym, tf, existing)

        if df is None:
            continue

        if existing is not None:
            df = pd.concat([existing, df])

        df = df.drop_duplicates().sort_values("time")
        upload(df, path)

# =============================
# MAIN
# =============================

def main():
    print("🚀 BINANCE ONLY PIPELINE", flush=True)

    symbols = load_json("state/binance_symbols.json", {"symbols": []})["symbols"]

    state = load_json("state/rotation.json", {"index": 0})

    start = state["index"]
    end = start + CHUNK_SIZE

    selected = symbols[start:end]

    state["index"] = 0 if end >= len(symbols) else end
    save_json("state/rotation.json", state)

    print(f"Processing {start} → {end}", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_symbol, selected)

    print("✅ DONE", flush=True)


if __name__ == "__main__":
    main()

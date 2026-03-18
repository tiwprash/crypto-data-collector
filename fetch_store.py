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

TIMEFRAMES_BINANCE = ["1m","5m","15m","30m","1h","4h","1d","1w"]
TIMEFRAMES_BYBIT = ["1","5","15","30","60","240","D","W"]

START_YEAR = 2023
CURRENT_YEAR = int(time.strftime("%Y"))

BINANCE_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
BYBIT_API = "https://api.bybit.com/v5/market/kline"

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
# BINANCE FETCH
# =============================

def fetch_binance(symbol, tf, existing):
    all_df = []

    if existing is None:
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

    else:
        year = time.strftime("%Y")
        month = time.strftime("%m")

        url = f"{BINANCE_BASE}/{symbol}/{tf}/{symbol}-{tf}-{year}-{month}.zip"

        try:
            res = requests.get(url, timeout=10)
            if res.status_code != 200:
                return None

            with zipfile.ZipFile(BytesIO(res.content)) as z:
                df = pd.read_csv(z.open(z.namelist()[0]), header=None)

            df = clean_dataframe(df)

            last_time = existing["time"].max()
            df = df[df["time"] > last_time]

            if not df.empty:
                all_df.append(df)

        except:
            return None

    return pd.concat(all_df) if all_df else None

# =============================
# BYBIT FETCH
# =============================

def fetch_bybit(symbol, tf, existing):
    try:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": tf,
            "limit": 200
        }

        res = requests.get(BYBIT_API, params=params, timeout=10)
        data = res.json()

        if "result" not in data:
            return None

        df = pd.DataFrame(data["result"]["list"])
        df.columns = ["time","open","high","low","close","volume","turnover"]

        df["time"] = pd.to_datetime(df["time"].astype(int), unit="ms")

        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if existing is not None:
            last_time = existing["time"].max()
            df = df[df["time"] > last_time]

        return df if not df.empty else None

    except:
        return None

# =============================
# PROCESS
# =============================

def process_symbol(symbol):
    sym = symbol["symbol"]

    # BINANCE
    for tf in TIMEFRAMES_BINANCE:
        path = f"binance/futures/{tf}/{sym}.parquet"
        existing = get_existing(path)

        df = fetch_binance(sym, tf, existing)

        if df is not None:
            if existing is not None:
                df = pd.concat([existing, df])

            df = df.drop_duplicates().sort_values("time")
            upload(df, path)

    # BYBIT
    for tf in TIMEFRAMES_BYBIT:
        path = f"bybit/futures/{tf}/{sym}.parquet"
        existing = get_existing(path)

        df = fetch_bybit(sym, tf, existing)

        if df is not None:
            if existing is not None:
                df = pd.concat([existing, df])

            df = df.drop_duplicates().sort_values("time")
            upload(df, path)

# =============================
# MAIN
# =============================

def main():
    print("🚀 FULL MULTI-TF PIPELINE", flush=True)

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

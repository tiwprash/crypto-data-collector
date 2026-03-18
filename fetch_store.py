import os
import requests
import pandas as pd
import boto3
import zipfile
import time
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# =============================
# CONFIG
# =============================

INTERVAL = "1h"
CHUNK_SIZE = 30
MAX_WORKERS = 5   # 🔥 parallel threads

BINANCE_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
BYBIT_BASE = "https://api.bybit.com/v5/market/kline"

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

def get_existing(path):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=path)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except:
        return None


def upload(df, path):
    file = "/tmp/temp.parquet"
    df.to_parquet(file, index=False)
    s3.upload_file(file, BUCKET, path)
    print(f"✅ {path}", flush=True)


# =============================
# SYMBOLS
# =============================

def get_top_bybit():
    data = requests.get("https://api.bybit.com/v5/market/tickers?category=linear").json()["result"]["list"]
    return sorted(data, key=lambda x: float(x["turnover24h"]), reverse=True)[:300]


def get_top_binance():
    data = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr").json()
    return sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)[:300]


# =============================
# BINANCE INCREMENTAL (DUMP)
# =============================

def process_binance(symbol):
    sym = symbol["symbol"]
    path = f"binance/futures/{INTERVAL}/{sym}.parquet"

    print(f"Binance {sym}", flush=True)

    existing = get_existing(path)
    last_time = existing["time"].max() if existing is not None else None

    # 🔥 only latest month
    month = time.strftime("%m")
    year = time.strftime("%Y")

    url = f"{BINANCE_BASE}/{sym}/{INTERVAL}/{sym}-{INTERVAL}-{year}-{month}.zip"

    try:
        res = requests.get(url)
        if res.status_code != 200:
            return

        with zipfile.ZipFile(BytesIO(res.content)) as z:
            df = pd.read_csv(z.open(z.namelist()[0]), header=None)

        df.columns = [
            "time","open","high","low","close","volume",
            "_","_","_","_","_","_"
        ]

        df = df[["time","open","high","low","close","volume"]]
        df["time"] = pd.to_datetime(df["time"], unit="ms")

        if last_time is not None:
            df = df[df["time"] > last_time]

        if df.empty:
            return

        if existing is not None:
            df = pd.concat([existing, df])

        df = df.drop_duplicates().sort_values("time")

        upload(df, path)

    except Exception as e:
        print(f"Binance error {sym}: {e}", flush=True)


# =============================
# BYBIT INCREMENTAL
# =============================

def process_bybit(symbol):
    sym = symbol["symbol"]
    path = f"bybit/futures/{INTERVAL}/{sym}.parquet"

    print(f"Bybit {sym}", flush=True)

    existing = get_existing(path)
    last_time = existing["time"].max() if existing is not None else None

    try:
        params = {
            "category": "linear",
            "symbol": sym,
            "interval": INTERVAL,
            "limit": 200
        }

        res = requests.get(BYBIT_BASE, params=params).json()

        if "result" not in res:
            return

        data = res["result"]["list"]
        if not data:
            return

        df = pd.DataFrame(data)
        df.columns = ["time","open","high","low","close","volume","turnover"]

        df["time"] = pd.to_datetime(df["time"].astype(int), unit="ms")

        if last_time is not None:
            df = df[df["time"] > last_time]

        if df.empty:
            return

        if existing is not None:
            df = pd.concat([existing, df])

        df = df.drop_duplicates().sort_values("time")

        upload(df, path)

    except Exception as e:
        print(f"Bybit error {sym}: {e}", flush=True)


# =============================
# MAIN (PARALLEL)
# =============================

def main():
    print("⚡ PARALLEL PIPELINE STARTED", flush=True)

    binance = get_top_binance()[:CHUNK_SIZE]
    bybit = get_top_bybit()[:CHUNK_SIZE]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        # Binance parallel
        executor.map(process_binance, binance)

        # Bybit parallel
        executor.map(process_bybit, bybit)

    print("✅ DONE", flush=True)


if __name__ == "__main__":
    main()

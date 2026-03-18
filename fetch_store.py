import os
import requests
import pandas as pd
import boto3
import zipfile
import time
import json
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# =============================
# CONFIG
# =============================

TIMEFRAMES = ["1h"]
CHUNK_SIZE = 20
MAX_WORKERS = 5
RETRIES = 3

BINANCE_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
BYBIT_BASE = "https://api.bybit.com/v5/market/kline"

START_YEAR = 2023
CURRENT_YEAR = int(time.strftime("%Y"))

# =============================
# R2 CLIENT
# =============================

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
)

BUCKET = os.getenv("R2_BUCKET")

# =============================
# STATE
# =============================

def load_state():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="state/rotation.json")
        return json.loads(obj["Body"].read())
    except:
        return {"index": 0}


def save_state(state):
    s3.put_object(Bucket=BUCKET, Key="state/rotation.json", Body=json.dumps(state))

# =============================
# HELPERS
# =============================

def retry_request(url, params=None):
    for _ in range(RETRIES):
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 200:
                return res
        except:
            time.sleep(1)
    return None


def get_existing(path):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=path)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except:
        return None


def upload(df, path):
    file = "/tmp/temp.parquet"

    # 🔥 compression
    df.to_parquet(file, index=False, compression="snappy")

    s3.upload_file(file, BUCKET, path)
    print(f"✅ {path}", flush=True)

# =============================
# VALIDATION
# =============================

def validate_data(df, interval):
    try:
        freq_map = {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "1h": "1H",
            "4h": "4H",
            "1d": "1D"
        }

        expected = pd.date_range(
            start=df["time"].min(),
            end=df["time"].max(),
            freq=freq_map[interval]
        )

        missing = len(expected) - len(df)

        score = max(0, 100 - (missing / len(expected) * 100))

        return round(score, 2)

    except:
        return 0

# =============================
# SYMBOLS
# =============================

def get_top_bybit():
    res = retry_request("https://api.bybit.com/v5/market/tickers?category=linear")
    if not res:
        return []

    data = res.json()
    if "result" not in data:
        return []

    return sorted(data["result"]["list"], key=lambda x: float(x["turnover24h"]), reverse=True)[:300]

# =============================
# BINANCE FETCH
# =============================

def fetch_binance(symbol, interval, existing):
    all_df = []

    # BACKFILL
    if existing is None:
        print(f"Backfilling {symbol}", flush=True)

        for year in range(START_YEAR, CURRENT_YEAR + 1):
            for month in range(1, 13):
                url = f"{BINANCE_BASE}/{symbol}/{interval}/{symbol}-{interval}-{year}-{str(month).zfill(2)}.zip"

                res = retry_request(url)
                if not res:
                    continue

                try:
                    with zipfile.ZipFile(BytesIO(res.content)) as z:
                        df = pd.read_csv(z.open(z.namelist()[0]), header=None)

                    df.columns = ["time","open","high","low","close","volume","_","_","_","_","_","_"]
                    df = df[["time","open","high","low","close","volume"]]
                    df["time"] = pd.to_datetime(df["time"], unit="ms")

                    all_df.append(df)

                except:
                    continue

    # INCREMENTAL
    else:
        last_time = existing["time"].max()

        year = time.strftime("%Y")
        month = time.strftime("%m")

        url = f"{BINANCE_BASE}/{symbol}/{interval}/{symbol}-{interval}-{year}-{month}.zip"

        res = retry_request(url)
        if not res:
            return None

        try:
            with zipfile.ZipFile(BytesIO(res.content)) as z:
                df = pd.read_csv(z.open(z.namelist()[0]), header=None)

            df.columns = ["time","open","high","low","close","volume","_","_","_","_","_","_"]
            df = df[["time","open","high","low","close","volume"]]
            df["time"] = pd.to_datetime(df["time"], unit="ms")

            df = df[df["time"] > last_time]

            all_df.append(df)

        except:
            return None

    if not all_df:
        return None

    return pd.concat(all_df)

# =============================
# BYBIT FETCH
# =============================

def fetch_bybit(symbol, interval, existing):
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": 200
    }

    res = retry_request(BYBIT_BASE, params)
    if not res:
        return None

    data = res.json()
    if "result" not in data:
        return None

    candles = data["result"]["list"]
    if not candles:
        return None

    df = pd.DataFrame(candles)
    df.columns = ["time","open","high","low","close","volume","turnover"]
    df["time"] = pd.to_datetime(df["time"].astype(int), unit="ms")

    if existing is not None:
        last_time = existing["time"].max()
        df = df[df["time"] > last_time]

    return df if not df.empty else None

# =============================
# PROCESS
# =============================

def process_symbol(symbol_obj, interval):
    sym = symbol_obj["symbol"]

    try:
        # -------- BINANCE --------
        path_b = f"binance/futures/{interval}/{sym}.parquet"
        existing_b = get_existing(path_b)

        df_b = fetch_binance(sym, interval, existing_b)

        if df_b is not None:
            if existing_b is not None:
                df_b = pd.concat([existing_b, df_b])

            df_b = df_b.drop_duplicates().sort_values("time")

            score = validate_data(df_b, interval)
            print(f"{sym} Binance Quality: {score}%", flush=True)

            upload(df_b, path_b)

        # -------- BYBIT --------
        path_y = f"bybit/futures/{interval}/{sym}.parquet"
        existing_y = get_existing(path_y)

        df_y = fetch_bybit(sym, interval, existing_y)

        if df_y is not None:
            if existing_y is not None:
                df_y = pd.concat([existing_y, df_y])

            df_y = df_y.drop_duplicates().sort_values("time")

            score = validate_data(df_y, interval)
            print(f"{sym} Bybit Quality: {score}%", flush=True)

            upload(df_y, path_y)

    except Exception as e:
        print(f"Error {sym}: {e}", flush=True)

# =============================
# MAIN
# =============================

def main():
    print("🚀 ULTIMATE PIPELINE STARTED", flush=True)

    symbols = get_top_bybit()
    if not symbols:
        print("No symbols")
        return

    state = load_state()
    start = state["index"]
    end = start + CHUNK_SIZE

    selected = symbols[start:end]

    state["index"] = 0 if end >= len(symbols) else end
    save_state(state)

    print(f"Processing {start} → {end}", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for tf in TIMEFRAMES:
            executor.map(lambda s: process_symbol(s, tf), selected)

    print("✅ DONE", flush=True)


if __name__ == "__main__":
    main()

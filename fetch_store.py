import os
import requests
import pandas as pd
import boto3
import zipfile
import time
import json
from io import BytesIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# =============================
# CONFIG
# =============================

CHUNK_SIZE = 20
MAX_WORKERS = 5
RETRIES = 3

START_YEAR = 2023
CURRENT_YEAR = int(time.strftime("%Y"))

BINANCE_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"

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
# JSON HELPERS
# =============================

def load_json(key, default):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except:
        return default


def save_json(key, data):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))

# =============================
# SYMBOL SYSTEM
# =============================

def get_binance_symbols():
    print("Fetching Binance symbols...", flush=True)

    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        res = requests.get(url, timeout=10)
        data = res.json()

        usdt_pairs = [x for x in data if x["symbol"].endswith("USDT")]

        sorted_data = sorted(
            usdt_pairs,
            key=lambda x: float(x["quoteVolume"]),
            reverse=True
        )

        symbols = [{"symbol": x["symbol"]} for x in sorted_data[:300]]

        print(f"✅ Binance symbols: {len(symbols)}", flush=True)

        return symbols

    except Exception as e:
        print("❌ Binance API failed:", e)
        return []


def get_bybit_symbols(binance_symbols):
    cache = load_json("state/bybit_symbols.json", {"symbols": []})

    try:
        print("Fetching Bybit symbols...", flush=True)

        url = "https://api.bybit.com/v5/market/tickers?category=linear"
        res = requests.get(url, timeout=10)

        data = res.json()
        symbols = data["result"]["list"]

        sorted_data = sorted(
            symbols,
            key=lambda x: float(x.get("turnover24h", 0)),
            reverse=True
        )

        top = [{"symbol": x["symbol"]} for x in sorted_data[:300]]

        save_json("state/bybit_symbols.json", {"symbols": top})

        print(f"✅ Bybit symbols: {len(top)}", flush=True)

        return top

    except Exception as e:
        print("⚠️ Bybit API failed:", e, flush=True)

    fallback = cache["symbols"]

    if fallback:
        print(f"Using cached Bybit symbols: {len(fallback)}", flush=True)
    else:
        print("No cached symbols, using base fallback", flush=True)
        fallback = [
            {"symbol": "BTCUSDT"},
            {"symbol": "ETHUSDT"},
            {"symbol": "SOLUSDT"},
            {"symbol": "XRPUSDT"},
        ]

    # 🔥 Fill from Binance
    if len(fallback) < 300 and binance_symbols:
        print("Filling missing symbols from Binance...", flush=True)

        existing = set(x["symbol"] for x in fallback)

        for coin in binance_symbols:
            if coin["symbol"] not in existing:
                fallback.append(coin)
                existing.add(coin["symbol"])

            if len(fallback) >= 300:
                break

    print(f"Final Bybit symbol count: {len(fallback)}", flush=True)

    return fallback

# =============================
# RETRY
# =============================

def retry_request(url):
    for _ in range(RETRIES):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                return res
        except:
            time.sleep(1)
    return None

# =============================
# DATA HELPERS
# =============================

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
    print(f"✅ Uploaded: {path}", flush=True)

# =============================
# CLEAN DATA (FIXED)
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
# FETCH BINANCE
# =============================

def fetch_binance(symbol, existing):
    all_df = []

    if existing is None:
        print(f"Backfilling {symbol}", flush=True)

        for year in range(START_YEAR, CURRENT_YEAR + 1):
            for month in range(1, 13):

                url = f"{BINANCE_BASE}/{symbol}/1h/{symbol}-1h-{year}-{str(month).zfill(2)}.zip"

                res = retry_request(url)

                if not res or res.status_code != 200:
                    continue

                try:
                    with zipfile.ZipFile(BytesIO(res.content)) as z:
                        df = pd.read_csv(z.open(z.namelist()[0]), header=None)

                    df = clean_dataframe(df)

                    if not df.empty:
                        all_df.append(df)

                except Exception as e:
                    print(f"Zip error {symbol}: {e}", flush=True)

    else:
        last_time = existing["time"].max()

        year = time.strftime("%Y")
        month = time.strftime("%m")

        url = f"{BINANCE_BASE}/{symbol}/1h/{symbol}-1h-{year}-{month}.zip"

        res = retry_request(url)

        if not res or res.status_code != 200:
            return None

        try:
            with zipfile.ZipFile(BytesIO(res.content)) as z:
                df = pd.read_csv(z.open(z.namelist()[0]), header=None)

            df = clean_dataframe(df)
            df = df[df["time"] > last_time]

            if not df.empty:
                all_df.append(df)

        except Exception as e:
            print(f"Incremental error {symbol}: {e}", flush=True)

    if not all_df:
        print(f"⚠️ No data found for {symbol}", flush=True)
        return None

    final_df = pd.concat(all_df)
    print(f"✅ {symbol} fetched {len(final_df)} rows", flush=True)

    return final_df

# =============================
# PROCESS
# =============================

def process_symbol(symbol_obj):
    sym = symbol_obj["symbol"]

    try:
        path = f"binance/futures/1h/{sym}.parquet"
        existing = get_existing(path)

        df = fetch_binance(sym, existing)

        if df is not None and not df.empty:
            if existing is not None:
                df = pd.concat([existing, df])

            df = df.drop_duplicates().sort_values("time")

            upload(df, path)

    except Exception as e:
        print(f"Error {sym}: {e}", flush=True)

# =============================
# MAIN
# =============================

def main():
    print("🚀 FINAL PIPELINE STARTED", flush=True)

    binance_symbols = get_binance_symbols()
    bybit_symbols = get_bybit_symbols(binance_symbols)

    # 🔥 Using Binance for processing (stable)
    symbols = binance_symbols if binance_symbols else bybit_symbols

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

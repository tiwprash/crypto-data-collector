import os
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

CHUNK_SIZE = 20
MAX_WORKERS = 5

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
# SYMBOL SOURCE (R2 ONLY)
# =============================

def get_symbols():
    data = load_json("state/binance_symbols.json", {"symbols": []})

    if not data["symbols"]:
        print("⚠️ No symbols found in R2", flush=True)
        return []

    print(f"✅ Loaded {len(data['symbols'])} symbols from R2", flush=True)

    return data["symbols"]

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
# CLEAN DATA (HEADER FIX)
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
# FETCH BINANCE DATA
# =============================

def fetch_binance(symbol, existing):
    all_df = []

    if existing is None:
        print(f"Backfilling {symbol}", flush=True)

        for year in range(START_YEAR, CURRENT_YEAR + 1):
            for month in range(1, 13):

                url = f"{BINANCE_BASE}/{symbol}/1h/{symbol}-1h-{year}-{str(month).zfill(2)}.zip"

                try:
                    import requests
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
        last_time = existing["time"].max()

        year = time.strftime("%Y")
        month = time.strftime("%m")

        url = f"{BINANCE_BASE}/{symbol}/1h/{symbol}-1h-{year}-{month}.zip"

        try:
            import requests
            res = requests.get(url, timeout=10)

            if res.status_code != 200:
                return None

            with zipfile.ZipFile(BytesIO(res.content)) as z:
                df = pd.read_csv(z.open(z.namelist()[0]), header=None)

            df = clean_dataframe(df)
            df = df[df["time"] > last_time]

            if not df.empty:
                all_df.append(df)

        except:
            return None

    if not all_df:
        return None

    final_df = pd.concat(all_df)
    print(f"✅ {symbol} fetched {len(final_df)} rows", flush=True)

    return final_df

# =============================
# PROCESS SYMBOL
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
    print("🚀 GITHUB PIPELINE STARTED", flush=True)

    symbols = get_symbols()

    if not symbols:
        print("No symbols found — exiting")
        return

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

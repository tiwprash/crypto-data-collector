import os
import pandas as pd
import boto3
import zipfile
import time
import json
import requests
import gzip
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# =============================
# CONFIG
# =============================

CHUNK_SIZE = 50
MAX_WORKERS = 5   # 🔥 safer

TIMEFRAMES = ["1m","5m","15m","30m","1h","4h","1d","1w"]

CURRENT_YEAR = int(time.strftime("%Y"))
CURRENT_MONTH = int(time.strftime("%m"))

START_YEAR = CURRENT_YEAR - 1  # last 2 yrs

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
        decompressed = gzip.decompress(obj["Body"].read())
        return pd.DataFrame(json.loads(decompressed))
    except:
        return None


def upload(df, path):
    print(f"⬆️ Uploading {path} | rows={len(df)}", flush=True)

    json_data = df.to_dict(orient="records")
    compressed = gzip.compress(json.dumps(json_data).encode("utf-8"))

    s3.put_object(
        Bucket=BUCKET,
        Key=path,
        Body=compressed,
        ContentType="application/json",
        ContentEncoding="gzip"
    )

    print(f"✅ Uploaded {path}", flush=True)

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
# FETCH (FIXED)
# =============================

def fetch_binance(symbol, tf):
    all_df = []
    success_count = 0

    print(f"📥 Fetching {symbol} {tf}", flush=True)

    for year in range(START_YEAR, CURRENT_YEAR + 1):
        for month in range(1, 13):

            if year == CURRENT_YEAR and month > CURRENT_MONTH:
                continue

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
                    success_count += 1

            except Exception as e:
                continue

    if not all_df:
        print(f"❌ No data at all for {symbol} {tf}", flush=True)
        return None

    print(f"✅ {symbol} {tf} months fetched: {success_count}", flush=True)

    return pd.concat(all_df)

# =============================
# PROCESS
# =============================

def process_symbol(symbol):
    sym = symbol["symbol"]

    for tf in TIMEFRAMES:
        path = f"binance/futures/{tf}/{sym}.json.gz"

        existing = get_existing(path)

        df = fetch_binance(sym, tf)

        if df is None or df.empty:
            continue

        # merge existing
        if existing is not None and not existing.empty:
            existing["time"] = pd.to_datetime(existing["time"])
            df = pd.concat([existing, df])

        # keep only last 2 yrs
        cutoff = pd.Timestamp.now() - pd.DateOffset(years=2)
        df = df[df["time"] >= cutoff]

        df = df.drop_duplicates().sort_values("time")

        upload(df, path)

# =============================
# MAIN
# =============================

def main():
    print("🚀 BINANCE PIPELINE (FIXED)", flush=True)

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

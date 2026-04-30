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
from botocore.config import Config

# =============================
# CONFIG
# =============================
CHUNK_SIZE = 400 
MAX_WORKERS = 15 
UPLOAD_CHUNK_ROWS = 100000

TIMEFRAMES = ["1m","5m","15m","30m","1h","4h","1d","1w"]

CURRENT_YEAR = int(time.strftime("%Y"))
CURRENT_MONTH = int(time.strftime("%m"))
START_YEAR = CURRENT_YEAR - 1

BINANCE_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"

# =============================
# R2 CONFIG
# =============================
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
BUCKET = os.getenv("R2_BUCKET")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(
        retries={"max_attempts": 3, "mode": "standard"},
        connect_timeout=10,
        read_timeout=60,
    )
)

print("🔍 DEBUG ENV", flush=True)
print("BUCKET:", BUCKET, flush=True)
print("ENDPOINT:", R2_ENDPOINT, flush=True)

# =============================
# PHASE 1: SCOUT FUNCTIONS
# =============================
def get_binance():
    print("Fetching top Binance symbols...", flush=True)
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        data = requests.get(url, timeout=10).json()
        
        if isinstance(data, dict):
            print(f"⚠️ Binance API Error: {data.get('msg', 'Unknown Error')}", flush=True)
            return None
            
        usdt = [x for x in data if x["symbol"].endswith("USDT")]
        sorted_data = sorted(usdt, key=lambda x: float(x["quoteVolume"]), reverse=True)
        return [{"symbol": x["symbol"]} for x in sorted_data[:400]]
    except Exception as e:
        print(f"❌ Failed to fetch Binance symbols: {e}", flush=True)
        return None

def get_bybit():
    print("Fetching top Bybit symbols...", flush=True)
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    try:
        data = requests.get(url, timeout=10).json()
        
        if "result" not in data or "list" not in data["result"]:
            print(f"⚠️ Bybit API Error: {data.get('retMsg', 'Unknown Error')}", flush=True)
            return None
            
        symbols = data["result"]["list"]
        sorted_data = sorted(symbols, key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
        return [{"symbol": x["symbol"]} for x in sorted_data[:400]]
    except Exception as e:
        print(f"❌ Failed to fetch Bybit symbols: {e}", flush=True)
        return None

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
    print(f"✅ Saved state: {key}", flush=True)

def get_existing(path):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=path)
        decompressed = gzip.decompress(obj["Body"].read())
        return pd.DataFrame(json.loads(decompressed))
    except:
        return None

# =============================
# FIXED CHUNKED UPLOAD
# =============================
def upload(df, base_path):
    total_rows = len(df)
    chunks = (total_rows // UPLOAD_CHUNK_ROWS) + 1

    print(f"⬆️ Uploading {base_path} in {chunks} chunks", flush=True)

    for i in range(chunks):
        chunk_df = df.iloc[i*UPLOAD_CHUNK_ROWS:(i+1)*UPLOAD_CHUNK_ROWS]

        if chunk_df.empty:
            continue

        chunk_df = chunk_df.copy()
        chunk_df["time"] = chunk_df["time"].astype("int64") // 10**6

        path = base_path.replace(".json.gz", f"_part{i}.json.gz")

        try:
            json_data = chunk_df.to_dict(orient="records")
            compressed = gzip.compress(json.dumps(json_data).encode("utf-8"))

            response = s3.put_object(
                Bucket=BUCKET,
                Key=path,
                Body=compressed,
                ContentType="application/json",
                ContentEncoding="gzip"
            )
            print(f"📡 {path} → {response['ResponseMetadata']['HTTPStatusCode']}", flush=True)

        except Exception as e:
            print(f"❌ FAILED {path}", flush=True)
            print(e, flush=True)

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
# FETCH: ARCHIVE (Bulk history)
# =============================
def fetch_binance_archive(symbol, tf):
    all_df = []
    print(f"📥 Fetching Archive {symbol} {tf}", flush=True)

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
            except:
                continue

    if not all_df:
        return None

    return pd.concat(all_df)

# =============================
# FETCH: LIVE (Fills the gap backward to avoid API bugs)
# =============================
def fetch_binance_live(symbol, tf, start_time_ms=None):
    print(f"⚡ Fetching LIVE gap for {symbol} {tf}", flush=True)
    
    all_rows = []
    limit = 1500
    
    # Target time: Where we want to stop fetching backward
    if start_time_ms is None:
        target_oldest_time = int((pd.Timestamp.now() - pd.DateOffset(years=2)).timestamp() * 1000)
    else:
        target_oldest_time = start_time_ms
        
    end_time = int(pd.Timestamp.now().timestamp() * 1000)

    # Safety check: If our target is in the future, do nothing
    if target_oldest_time >= end_time:
        return None

    while True:
        # NOTICE: We use endTime here to walk backward!
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={tf}&endTime={end_time}&limit={limit}"
        
        try:
            res = requests.get(url, timeout=10)
            data = res.json()
            
            # Print explicit API errors instead of silently failing
            if isinstance(data, dict): 
                print(f"⚠️ Live API Error {symbol} {tf}: {data.get('msg', data)}", flush=True)
                break

            # If no data returned, we've walked back as far as Binance allows
            if not data:
                break

            for row in data:
                # Only keep the candles that are newer than our target time
                if row[0] >= target_oldest_time:
                    all_rows.append({
                        "time": row[0],
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5])
                    })

            oldest_in_batch = data[0][0]

            # Break if we've successfully walked back past our target time
            if oldest_in_batch <= target_oldest_time:
                break

            # Break if Binance returned less than the limit (we hit the coin's launch date)
            if len(data) < limit:
                break

            # Step backward: set the next end_time to the candle just before the oldest one we just got
            end_time = oldest_in_batch - 1
            time.sleep(0.1) # Rate limit protection

        except Exception as e:
            print(f"❌ Live fetch error {symbol} {tf}: {e}", flush=True)
            break

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    
    # Sort chronologically to fix the backward fetching order
    df = df.sort_values("time").drop_duplicates(subset=["time"])
    return df

# =============================
# PROCESS
# =============================
def process_symbol(symbol):
    sym = symbol["symbol"]

    for tf in TIMEFRAMES:
        base_path = f"binance/futures/{tf}/{sym}.json.gz"
        existing = get_existing(base_path)

        df_list = []
        last_timestamp_ms = None

        if existing is not None and not existing.empty:
            existing["time"] = pd.to_datetime(existing["time"], unit="ms")
            df_list.append(existing)
            last_timestamp_ms = int(existing["time"].max().timestamp() * 1000) + 1
            
        else:
            archive_df = fetch_binance_archive(sym, tf)
            if archive_df is not None and not archive_df.empty:
                df_list.append(archive_df)
                last_timestamp_ms = int(archive_df["time"].max().timestamp() * 1000) + 1

        live_df = fetch_binance_live(sym, tf, start_time_ms=last_timestamp_ms)
        if live_df is not None and not live_df.empty:
            df_list.append(live_df)

        if not df_list:
            print(f"❌ No data found anywhere for {sym} {tf}", flush=True)
            continue

        df = pd.concat(df_list, ignore_index=True)

        cutoff = pd.Timestamp.now() - pd.DateOffset(years=2)
        df = df[df["time"] >= cutoff]

        df = df.drop_duplicates(subset=["time"]).sort_values("time")

        upload(df, base_path)

# =============================
# MAIN
# =============================
def main():
    print("🚀 UNIFIED PIPELINE STARTING", flush=True)

    binance_symbols = get_binance()
    bybit_symbols = get_bybit()

    if not binance_symbols:
        print("⚠️ Falling back to existing Binance symbols from R2...", flush=True)
        binance_symbols = load_json("state/binance_symbols.json", {"symbols": []})["symbols"]
    else:
        save_json("state/binance_symbols.json", {"symbols": binance_symbols})

    if not bybit_symbols:
        print("⚠️ Falling back to existing Bybit symbols from R2...", flush=True)
        bybit_symbols = load_json("state/bybit_symbols.json", {"symbols": []})["symbols"]
    else:
        save_json("state/bybit_symbols.json", {"symbols": bybit_symbols})

    if not binance_symbols:
        print("❌ Critical: No Binance symbols available. Exiting.", flush=True)
        return

    state = load_json("state/rotation.json", {"index": 0})
    start = state["index"]
    end = start + CHUNK_SIZE
    
    selected = binance_symbols[start:end]

    state["index"] = 0 if end >= len(binance_symbols) else end
    save_json("state/rotation.json", state)

    print(f"Processing {start} → {end} ({len(selected)} symbols)", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_symbol, selected)

    print("✅ PIPELINE COMPLETE", flush=True)

if __name__ == "__main__":
    main()

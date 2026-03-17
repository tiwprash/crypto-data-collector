import os
import requests
import pandas as pd
import boto3
import time
from datetime import datetime

# =============================
# CONFIG
# =============================

# ✅ Binance Futures API
BINANCE_FUTURES_BASE = "https://fapi.binance.com/fapi/v1/klines"

INTERVAL = "1h"
LIMIT = 1000

# =============================
# R2 CONFIG
# =============================

R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
BUCKET_NAME = os.getenv("R2_BUCKET")

# =============================
# R2 CLIENT
# =============================

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

# =============================
# FETCH BINANCE FUTURES DATA
# =============================

def fetch_binance_futures(symbol):
    all_data = []
    end_time = int(datetime.now().timestamp() * 1000)

    for i in range(10):  # test mode
        try:
            params = {
                "symbol": symbol,
                "interval": INTERVAL,
                "limit": LIMIT,
                "endTime": end_time,
            }

            response = requests.get(BINANCE_FUTURES_BASE, params=params)

            if response.status_code != 200:
                print(f"HTTP Error {response.status_code}", flush=True)
                break

            res = response.json()

            if isinstance(res, dict):
                print(f"API Error: {res}", flush=True)
                break

            if not isinstance(res, list) or len(res) == 0:
                print("No more data", flush=True)
                break

            df = pd.DataFrame(res)
            all_data.append(df)

            end_time = res[0][0]

            print(f"{symbol} futures batch {i+1} fetched", flush=True)

            time.sleep(0.2)

        except Exception as e:
            print(f"Error fetching {symbol}: {e}", flush=True)
            break

    if not all_data:
        return None

    df = pd.concat(all_data)

    df.columns = [
        "time","open","high","low","close","volume",
        "_","_","_","_","_","_"
    ]

    df = df[["time","open","high","low","close","volume"]]

    df["time"] = pd.to_datetime(df["time"], unit="ms")

    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates().sort_values("time")

    return df


# =============================
# UPLOAD TO R2
# =============================

def upload_to_r2(df, filename):
    try:
        file_path = f"/tmp/{filename}.parquet"

        df.to_parquet(file_path, index=False)

        print(f"Uploading {filename} to R2...", flush=True)

        s3.upload_file(file_path, BUCKET_NAME, f"futures/{filename}.parquet")

        print(f"✅ Uploaded: futures/{filename}.parquet", flush=True)

    except Exception as e:
        print(f"❌ Upload error: {e}", flush=True)


# =============================
# MAIN
# =============================

def main():
    print("🚀 FUTURES DATA PIPELINE STARTED", flush=True)

    symbols = ["BTCUSDT", "ETHUSDT"]

    for symbol in symbols:
        try:
            print(f"\nFetching FUTURES {symbol}...", flush=True)

            df = fetch_binance_futures(symbol)

            if df is not None and not df.empty:
                filename = f"binance_futures_{symbol}"
                upload_to_r2(df, filename)
            else:
                print(f"No valid data for {symbol}", flush=True)

        except Exception as e:
            print(f"Error processing {symbol}: {e}", flush=True)

    print("✅ FUTURES PIPELINE FINISHED", flush=True)


if __name__ == "__main__":
    main()

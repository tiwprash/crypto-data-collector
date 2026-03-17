import os
import requests
import pandas as pd
import boto3
import time
from datetime import datetime

# =============================
# CONFIG
# =============================

BINANCE_BASE = "https://api.binance.com/api/v3/klines"
INTERVAL = "1h"
LIMIT = 1000

# =============================
# R2 CONFIG (FROM ENV)
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
# FETCH BINANCE DATA (SAFE)
# =============================

def fetch_binance(symbol):
    all_data = []
    end_time = int(datetime.now().timestamp() * 1000)

    for i in range(30):  # ~30k candles max
        try:
            params = {
                "symbol": symbol,
                "interval": INTERVAL,
                "limit": LIMIT,
                "endTime": end_time,
            }

            response = requests.get(BINANCE_BASE, params=params)

            if response.status_code != 200:
                print(f"HTTP Error {response.status_code}")
                break

            res = response.json()

            # ✅ HANDLE BINANCE ERRORS
            if isinstance(res, dict):
                print(f"Binance API Error: {res}")
                break

            if not isinstance(res, list) or len(res) == 0:
                print("No more data")
                break

            df = pd.DataFrame(res)

            all_data.append(df)

            # move backward in time
            end_time = res[0][0]

            print(f"{symbol} batch {i+1} fetched")

            time.sleep(0.3)  # avoid rate limits

        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
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

    # convert to numeric
    for col in ["open", "high", "low", "close", "volume"]:
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

        s3.upload_file(file_path, BUCKET_NAME, f"data/{filename}.parquet")

        print(f"Uploaded to R2: {filename}")

    except Exception as e:
        print(f"Upload error: {e}")


# =============================
# MAIN
# =============================

def main():
    symbols = ["BTCUSDT", "ETHUSDT"]  # keep small for testing

    for symbol in symbols:
        try:
            print(f"\nFetching {symbol}...")

            df = fetch_binance(symbol)

            if df is not None and not df.empty:
                upload_to_r2(df, f"binance_{symbol}")
            else:
                print(f"No valid data for {symbol}")

        except Exception as e:
            print(f"Error processing {symbol}: {e}")


if __name__ == "__main__":
    main()

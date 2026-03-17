import os
import requests
import pandas as pd
import boto3
from datetime import datetime, timedelta

# =============================
# CONFIG
# =============================

BINANCE_BASE = "https://api.binance.com/api/v3/klines"
BYBIT_BASE = "https://api.bybit.com/v5/market/kline"

INTERVAL = "1h"
LIMIT = 1000  # max per request

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
# FETCH BINANCE DATA
# =============================

def fetch_binance(symbol):
    data = []
    end_time = int(datetime.now().timestamp() * 1000)

    for _ in range(30):  # loop for history
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "limit": LIMIT,
            "endTime": end_time,
        }

        res = requests.get(BINANCE_BASE, params=params).json()

        if not res:
            break

        df = pd.DataFrame(res)
        data.append(df)

        end_time = res[0][0]  # move backward

    if not data:
        return None

    df = pd.concat(data)

    df.columns = [
        "time","open","high","low","close","volume",
        "_","_","_","_","_","_"
    ]

    df = df[["time","open","high","low","close","volume"]]
    df["time"] = pd.to_datetime(df["time"], unit="ms")

    return df


# =============================
# UPLOAD TO R2
# =============================

def upload_to_r2(df, filename):
    file_path = f"/tmp/{filename}.parquet"
    df.to_parquet(file_path, index=False)

    s3.upload_file(file_path, BUCKET_NAME, f"data/{filename}.parquet")


# =============================
# MAIN
# =============================

def main():
    symbols = ["BTCUSDT", "ETHUSDT"]  # replace with top 300 later

    for symbol in symbols:
        print(f"Fetching {symbol}...")
        df = fetch_binance(symbol)

        if df is not None:
            upload_to_r2(df, f"binance_{symbol}")
            print(f"Uploaded {symbol}")


if __name__ == "__main__":
    main()
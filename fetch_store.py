# =============================
# PHASE 1: SCOUT FUNCTIONS
# =============================
def get_binance():
    print("Fetching top Binance symbols...", flush=True)
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        data = requests.get(url, timeout=10).json()
        
        # FIX: Check if Binance returned an error dictionary instead of a list
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
        
        # Bybit nests its data, check if it was successful
        if "result" not in data or "list" not in data["result"]:
            print(f"⚠️ Bybit API Error: {data.get('retMsg', 'Unknown Error')}", flush=True)
            return None
            
        symbols = data["result"]["list"]
        sorted_data = sorted(symbols, key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
        return [{"symbol": x["symbol"]} for x in sorted_data[:400]]
    except Exception as e:
        print(f"❌ Failed to fetch Bybit symbols: {e}", flush=True)
        return None

# ... [KEEP ALL YOUR EXISTING HELPERS AND FETCH FUNCTIONS HERE] ...

# =============================
# MAIN
# =============================
def main():
    print("🚀 UNIFIED PIPELINE STARTING", flush=True)

    # 1. Scrape fresh top symbols from exchanges
    binance_symbols = get_binance()
    bybit_symbols = get_bybit()

    # 2. FALLBACK LOGIC: If live fetch fails, load the last known list from R2
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

    # If both the API and R2 are empty, we have to stop
    if not binance_symbols:
        print("❌ Critical: No Binance symbols available. Exiting.", flush=True)
        return

    # 3. Load rotation state
    state = load_json("state/rotation.json", {"index": 0})
    start = state["index"]
    end = start + CHUNK_SIZE
    
    selected = binance_symbols[start:end]

    # Reset rotation if we hit the end
    state["index"] = 0 if end >= len(binance_symbols) else end
    save_json("state/rotation.json", state)

    print(f"Processing {start} → {end} ({len(selected)} symbols)", flush=True)

    # 4. Execute the hybrid fetching pipeline on all selected symbols
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_symbol, selected)

    print("✅ PIPELINE COMPLETE", flush=True)

if __name__ == "__main__":
    main()

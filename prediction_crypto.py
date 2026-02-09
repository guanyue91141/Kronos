# -*- coding: utf-8 -*-
"""
prediction_crypto.py

Description:
    Predicts future market data for Crypto assets (e.g., BTC/USDT) using Kronos model.
    Fetches data from Binance public API (no API key required).

Usage:
    python prediction_crypto.py --symbol BTCUSDT --interval 1d

Arguments:
    --symbol     Trading pair symbol (e.g. BTCUSDT, ETHUSDT)
    --interval   K-line interval (e.g. 1d, 4h, 1h, 15m)
    --proxy      Optional HTTP proxy (e.g. http://127.0.0.1:7890)

Output:
    - Saves prediction results to ./outputs/pred_crypto_<symbol>_<interval>.csv
    - Saves prediction plot to ./outputs/pred_crypto_<symbol>_<interval>.png
"""

import os
import argparse
import time
import json
import urllib.request
import urllib.error
import pandas as pd
import matplotlib.pyplot as plt
import sys
from datetime import datetime, timedelta

# Add parent directory to path to import model
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

try:
    from model import Kronos, KronosTokenizer, KronosPredictor
except ImportError:
    # Fallback if run from a different directory structure
    sys.path.append("../")
    from model import Kronos, KronosTokenizer, KronosPredictor


SAVE_DIR = "./outputs"
os.makedirs(SAVE_DIR, exist_ok=True)

# Settings
TOKENIZER_PRETRAINED = "NeoQuasar/Kronos-Tokenizer-base"
MODEL_PRETRAINED = "NeoQuasar/Kronos-base"
DEVICE = "cpu"  # Change to "cuda:0" if GPU is available
MAX_CONTEXT = 512
LOOKBACK = 400
PRED_LEN = 120
T = 1.0
TOP_P = 0.9
SAMPLE_COUNT = 1


def fetch_binance_klines(symbol="BTCUSDT", interval="1d", limit=1000, proxy=None):
    """
    Fetches K-line data from Binance public API.
    """
    base_url = "https://api.binance.com/api/v3/klines"
    url = f"{base_url}?symbol={symbol.upper()}&interval={interval}&limit={limit}"
    
    print(f"📥 Fetching {symbol} ({interval}) data from Binance...")
    
    proxies = {}
    if proxy:
        proxies = {'http': proxy, 'https': proxy}
        print(f"   Using proxy: {proxy}")
        proxy_handler = urllib.request.ProxyHandler(proxies)
        opener = urllib.request.build_opener(proxy_handler)
        urllib.request.install_opener(opener)

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
        # Binance API returns:
        # [
        #   [
        #     1499040000000,      // Open time  (Index 0)
        #     "0.01634790",       // Open       (Index 1)
        #     "0.80000000",       // High       (Index 2)
        #     "0.01575800",       // Low        (Index 3)
        #     "0.01577100",       // Close      (Index 4)
        #     "148976.11427815",  // Volume     (Index 5)
        #     1499644799999,      // Close time (Index 6)
        #     "2434.19055334",    // Quote Asset Volume (Amount) (Index 7)
        #     ...
        #   ]
        # ]
        
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume", 
            "close_time", "amount", "num_trades", "taker_buy_vol", "taker_buy_amount", "ignore"
        ])
        
        # Convert timestamp
        df["date"] = pd.to_datetime(df["open_time"], unit='ms')
        
        # Select and type cast required columns
        numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Final cleanup match Kronos format
        df = df[["date", "open", "high", "low", "close", "volume", "amount"]]
        
        print(f"✅ Data loaded: {len(df)} rows, range: {df['date'].min()} ~ {df['date'].max()}")
        return df

    except Exception as e:
        print(f"❌ Failed to fetch data: {e}")
        print("   Tips: Check your network connection. If you are in a restricted region, use --proxy.")
        # Return empty DataFrame or raise
        return pd.DataFrame()


def prepare_inputs(df, pred_len):
    # Ensure we have enough data
    if len(df) < LOOKBACK:
        raise ValueError(f"Not enough data. Need at least {LOOKBACK} rows, got {len(df)}")
        
    x_df = df.iloc[-LOOKBACK:][["open", "high", "low", "close", "volume", "amount"]]
    x_timestamp = df.iloc[-LOOKBACK:]["date"]
    
    # Generate future timestamps
    last_date = df["date"].iloc[-1]
    
    # Infer frequency from data
    if len(df) > 1:
        diff = df["date"].iloc[-1] - df["date"].iloc[-2]
    else:
        diff = pd.Timedelta(days=1)
        
    y_timestamp = [last_date + (i + 1) * diff for i in range(pred_len)]
    y_timestamp = pd.Series(y_timestamp)
    
    return x_df, pd.Series(x_timestamp), y_timestamp


def plot_result(df_hist, df_pred, symbol, interval, save_path):
    plt.figure(figsize=(12, 6))
    
    # Plot historical (last part)
    # Only show last 200 points to keep chart readable
    display_lookback = 200
    hist_subset = df_hist.iloc[-display_lookback:] if len(df_hist) > display_lookback else df_hist
    
    plt.plot(hist_subset["date"], hist_subset["close"], label="Historical", color="blue")
    plt.plot(df_pred["date"], df_pred["close"], label="Predicted", color="red", linestyle="--")
    
    plt.title(f"Kronos Prediction for {symbol} ({interval})")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    print(f"📊 Chart saved: {save_path}")


def main():
    parser = argparse.ArgumentParser(description="Kronos Crypto Prediction")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair, e.g. BTCUSDT")
    parser.add_argument("--interval", type=str, default="1d", help="K-line interval, e.g. 1d, 4h, 15m")
    parser.add_argument("--proxy", type=str, default=None, help="HTTP Proxy, e.g. http://127.0.0.1:7890")
    args = parser.parse_args()

    # 1. Fetch Data
    df = fetch_binance_klines(args.symbol, args.interval, limit=LOOKBACK + 10, proxy=args.proxy)
    
    if df.empty:
        sys.exit(1)

    # 2. Init Model
    print(f"🚀 Loading Kronos model ({MODEL_PRETRAINED})...")
    try:
        tokenizer = KronosTokenizer.from_pretrained(TOKENIZER_PRETRAINED)
        model = Kronos.from_pretrained(MODEL_PRETRAINED)
        predictor = KronosPredictor(model, tokenizer, device=DEVICE, max_context=MAX_CONTEXT)
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        sys.exit(1)

    # 3. Prepare Inputs
    x_df, x_timestamp, y_timestamp = prepare_inputs(df, PRED_LEN)

    # 4. Predict
    print("🔮 Generating predictions...")
    pred_df = predictor.predict(
        df=x_df,
        x_timestamp=x_timestamp,
        y_timestamp=y_timestamp,
        pred_len=PRED_LEN,
        T=T,
        top_p=TOP_P,
        sample_count=SAMPLE_COUNT
    )
    
    # 5. Save & Plot
    pred_df["date"] = y_timestamp.values
    
    # Merge for consistent saving (Optional, similar to original script)
    # Just saving prediction for clarity
    file_symbol = args.symbol.replace("/", "")
    csv_path = os.path.join(SAVE_DIR, f"pred_crypto_{file_symbol}_{args.interval}.csv")
    chart_path = os.path.join(SAVE_DIR, f"pred_crypto_{file_symbol}_{args.interval}.png")
    
    pred_df.to_csv(csv_path, index=False)
    print(f"✅ Prediction saved: {csv_path}")
    
    plot_result(df, pred_df, args.symbol, args.interval, chart_path)


if __name__ == "__main__":
    main()

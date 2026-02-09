import os
import sys
import pandas as pd
from datetime import datetime

# Add project root directory to path to import prediction_crypto
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

try:
    from prediction_crypto import fetch_binance_klines
except ImportError:
    # Attempt absolute import if relative fails
    try:
        sys.path.append("h:/Kronos")
        from prediction_crypto import fetch_binance_klines
    except ImportError:
        print("Warning: Could not import prediction_crypto.fetch_binance_klines")

def main(inst_id, bar):
    """
    Fetch crypto data using prediction_crypto.py logic (Binance API).
    
    Args:
        inst_id (str): Trading pair symbol (e.g., 'BTCUSDT')
        bar (str): Timeframe interval (e.g., '1h', '1d', '5m')
        
    Returns:
        dict: Result with status and file path
    """
    try:
        # Normalize inputs
        # Remove hyphens/underscores for Binance symbol format (e.g., BTC-USDT -> BTCUSDT)
        symbol = inst_id.replace('-', '').replace('_', '').upper()
        
        # Map timeframe to Binance format if needed
        # Kronos webui might pass '1H', '1D', '1W', '1M' -> Convert to lowercase for Binance '1h', '1d', etc.
        interval = bar.lower()
        
        # Call fetch function from prediction_crypto
        # Fetch generous amount of data to ensure lookback is covered
        df = fetch_binance_klines(symbol=symbol, interval=interval, limit=1000)
        
        if df.empty:
            return {"success": False, "error": f"Failed to fetch data for {symbol} {interval}"}
            
        # Ensure data directory exists
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # Save to CSV
        safe_inst = symbol
        output_filename = f"{safe_inst}_{interval}.csv"
        output_path = os.path.join(data_dir, output_filename)
        
        # Rename date column to timestamps to match Kronos expectation
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'timestamps'})
            # Adjust timezone from UTC to UTC+8
            df['timestamps'] = pd.to_datetime(df['timestamps']) + pd.Timedelta(hours=8)
            
        # Ensure columns are in correct order
        cols = ["timestamps", "open", "high", "low", "close", "volume", "amount"]
        # Filter for existing columns only, filling missing with 0 if criticals exist
        available_cols = [c for c in cols if c in df.columns]
        df_save = df[available_cols].copy()
        
        # Fill missing volume/amount if they don't exist (though fetch_binance_klines handles this)
        if 'volume' not in df_save.columns: df_save['volume'] = 0
        if 'amount' not in df_save.columns: df_save['amount'] = 0
            
        # Sort by timestamp ascending
        df_save = df_save.sort_values('timestamps')
        
        df_save.to_csv(output_path, index=False)
        
        return {
            "success": True, 
            "saved_csv": output_path, 
            "rows": len(df_save),
            "message": f"Successfully fetched {len(df_save)} rows for {symbol}"
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

# Keep update function for compatibility if needed, but redirect to main
def update(default_inst_id, default_bar):
    return main(default_inst_id, default_bar)

if __name__ == "__main__":
    # Test
    print(main("BTCUSDT", "1d"))

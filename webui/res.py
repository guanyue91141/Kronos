import okx.MarketData as MarketData
import time
import csv
import random
from datetime import datetime
import os

REQUEST_INTERVAL_SEC = 0


def main(inst_id, bar, limit="100", pages=20, after=None):
    try:
        flag = "0"
        marketDataAPI = MarketData.MarketAPI(flag=flag)

        all_rows = []
        seen_ts = set()

        if after is None:
            after = str(int(time.time() * 1000))
        
        resp = marketDataAPI.get_mark_price_candlesticks(
            instId=inst_id,
            after=after,
            bar=bar,
            limit=limit
        )
        page_rows = resp.get("data", []) if isinstance(resp, dict) else []
        for r in page_rows:
            ts = r[0]
            if ts not in seen_ts:
                seen_ts.add(ts)
                all_rows.append(r)

        for _ in range(pages - 1):
            if not all_rows:
                break
            min_ts = min(int(r[0]) for r in all_rows)
            time.sleep(REQUEST_INTERVAL_SEC)
            resp = marketDataAPI.get_mark_price_candlesticks(
                instId=inst_id,
                after=str(min_ts),
                bar=bar,
                limit=limit
            )
            page_rows = resp.get("data", []) if isinstance(resp, dict) else []
            if not page_rows:
                break
            for r in page_rows:
                ts = r[0]
                if ts not in seen_ts:
                    seen_ts.add(ts)
                    all_rows.append(r)

        safe_inst = inst_id.replace('-', '_')
        output_path = f"data/{safe_inst}_{bar}_mark.csv"
        
        if os.path.exists(output_path):
            with open(output_path, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                existing_rows = []
                for row in reader:
                    if row:  
                        existing_rows.append(row)
            
            for row in existing_rows:
                ts_str = row[0]
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                ts_ms = int(dt.timestamp() * 1000)
                if str(ts_ms) not in seen_ts:
                    o, h, l, c = row[1], row[2], row[3], row[4]
                    existing_data = [str(ts_ms), o, h, l, c]
                    all_rows.append(existing_data)
                    seen_ts.add(str(ts_ms))
        
        all_rows_sorted = sorted(all_rows, key=lambda r: int(r[0]))
        
        unique_rows = []
        seen_unique_ts = set()
        for row in all_rows_sorted:
            ts = row[0]
            if ts not in seen_unique_ts:
                seen_unique_ts.add(ts)
                unique_rows.append(row)
        
        all_rows_sorted = unique_rows

        with open(output_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamps", "open", "high", "low", "close", "volume", "amount"])
            for r in all_rows_sorted:
                ts_ms = int(r[0])
                o, h, l, c = r[1], r[2], r[3], r[4]
                volume = random.randint(1000, 5000)
                amount = volume*random.randint(1100,1500)
                ts_str = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([ts_str, o, h, l, c, volume, amount])

        return {"success": True, "saved_csv": output_path, "rows": len(all_rows_sorted)}
    except Exception as e:
        return {"success": False, "error": str(e)}


def update(default_inst_id, default_bar):
    try:
        safe_inst = default_inst_id.replace('-', '_')
        output_path = f"data/{safe_inst}_{default_bar}_mark.csv"
        
        if not os.path.exists(output_path):
            print(f"File {output_path} does not exist, will fetch data from current time")
            return main(default_inst_id, default_bar)
        
        with open(output_path, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            rows = list(reader)
        
        if not rows:
            print(f"File {output_path} is empty, will fetch data from current time")
            return main(default_inst_id, default_bar)

        oldest_row = rows[0]
        oldest_timestamp_str = oldest_row[0]
        
        dt = datetime.strptime(oldest_timestamp_str, "%Y-%m-%d %H:%M:%S")
        print(dt)
        oldest_timestamp_ms = int(dt.timestamp() * 1000)
        
        return main(default_inst_id, default_bar, after=str(oldest_timestamp_ms))
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    default_inst_id = "BTC-USD-SWAP"
    default_bar = "5m"
    update(default_inst_id, default_bar)

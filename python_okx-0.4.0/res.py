import okx.MarketData as MarketData
import time
import csv
import random
from datetime import datetime

REQUEST_INTERVAL_SEC = 0


def main(inst_id, bar, limit="100", pages=20):
    flag = "0"  # 实盘:0 , 模拟盘：1
    marketDataAPI = MarketData.MarketAPI(flag=flag)

    all_rows = []
    seen_ts = set()

    # 第1页：使用 before=当前毫秒，拿到最新100条
    now_ms = str(int(time.time() * 1000))
    resp = marketDataAPI.get_mark_price_candlesticks(
        instId=inst_id,
        after=now_ms,
        bar=bar,
        limit=limit
    )
    page_rows = resp.get("data", []) if isinstance(resp, dict) else []
    for r in page_rows:
        ts = r[0]
        if ts not in seen_ts:
            seen_ts.add(ts)
            all_rows.append(r)

    # 后续翻页：每次取当前已收集数据最小 ts，作为 after，获取更旧的数据
    for _ in range(pages - 1):
        if not all_rows:
            break
        min_ts = min(int(r[0]) for r in all_rows)
        time.sleep(REQUEST_INTERVAL_SEC)
        resp = marketDataAPI.get_mark_price_candlesticks(
            instId=inst_id,
            after=str(min_ts),  # 更旧的数据
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

    # 排序与落盘（timestamps,open,high,low,close,volume,amount）
    all_rows_sorted = sorted(all_rows, key=lambda r: int(r[0]))

    safe_inst = inst_id.replace('-', '_')
    output_path = f"data/{safe_inst}_{bar}_mark.csv"
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

    print({"saved_csv": output_path, "rows": len(all_rows_sorted)})


if __name__ == "__main__":
    # 默认参数，可按需修改或扩展为命令行参数
    default_inst_id = "BTC-USD-SWAP"
    default_bar = "5m"
    main(default_inst_id, default_bar)

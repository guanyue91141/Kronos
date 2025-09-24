import okx.MarketData as MarketData
import time
import csv
import random
from datetime import datetime
import os

REQUEST_INTERVAL_SEC = 0


def main(inst_id, bar, limit="100", pages=20, after=None):
    try:
        flag = "0"  # 实盘:0 , 模拟盘：1
        marketDataAPI = MarketData.MarketAPI(flag=flag)

        all_rows = []
        seen_ts = set()

        # 使用传入的时间戳，如果未提供则使用当前时间
        if after is None:
            after = str(int(time.time() * 1000))
        
        # 第1页：使用 after=当前毫秒或传入时间戳，拿到最新100条
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

        # 检查输出文件是否存在，并与现有数据合并
        safe_inst = inst_id.replace('-', '_')
        output_path = f"data/{safe_inst}_{bar}_mark.csv"
        
        if os.path.exists(output_path):
            with open(output_path, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)  # 跳过标题行（如果存在）
                existing_rows = []
                for row in reader:
                    if row:  # 跳过空行
                        existing_rows.append(row)
            
            # 将现有时间戳转换回毫秒以便比较
            for row in existing_rows:
                # 解析时间戳字符串回毫秒
                ts_str = row[0]
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                ts_ms = int(dt.timestamp() * 1000)
                # 如果尚未存在，则将原始数据值（开盘价、最高价、最低价、收盘价）添加回all_rows
                if str(ts_ms) not in seen_ts:
                    # 从现有行中提取值（开盘价、最高价、最低价、收盘价）
                    o, h, l, c = row[1], row[2], row[3], row[4]
                    # 以API响应的相同格式创建数据条目 [timestamp_ms, 开盘价, 最高价, 最低价, 收盘价]
                    existing_data = [str(ts_ms), o, h, l, c]
                    all_rows.append(existing_data)
                    seen_ts.add(str(ts_ms))
        
        # 按时间戳对合并的数据进行排序，为去重做准备
        all_rows_sorted = sorted(all_rows, key=lambda r: int(r[0]))
        
        # 删除重复时间戳（以防有任何遗漏）- 保留第一次出现的
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
    """
    更新函数：读取数据文件，获取最旧的日期转为时间戳，传入main函数当做after的值并调用
    """
    try:
        safe_inst = default_inst_id.replace('-', '_')
        output_path = f"data/{safe_inst}_{default_bar}_mark.csv"
        
        if not os.path.exists(output_path):
            print(f"文件 {output_path} 不存在，将从当前时间开始获取数据")
            return main(default_inst_id, default_bar)
        
        # 读取现有数据文件
        with open(output_path, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)  # 跳过标题行
            rows = list(reader)
        
        if not rows:
            print(f"文件 {output_path} 为空，将从当前时间开始获取数据")
            return main(default_inst_id, default_bar)

        # 获取最旧的日期（文件中的最后一行，因为数据是按时间排序的）
        oldest_row = rows[0]
        oldest_timestamp_str = oldest_row[0]  # 时间戳字符串在第一列
        
        # 将日期字符串转换为时间戳
        dt = datetime.strptime(oldest_timestamp_str, "%Y-%m-%d %H:%M:%S")
        print(dt)
        oldest_timestamp_ms = int(dt.timestamp() * 1000)
        
        # 调用main函数，传入最旧时间戳作为after参数
        return main(default_inst_id, default_bar, after=str(oldest_timestamp_ms))
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    # 默认参数，可按需修改或扩展为命令行参数
    default_inst_id = "BTC-USD-SWAP"
    default_bar = "5m"
    #main(default_inst_id, default_bar)
    update(default_inst_id, default_bar)

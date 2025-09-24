import okx.MarketData as MarketData
import time

flag = "0"  # 实盘:0 , 模拟盘：1

marketDataAPI =  MarketData.MarketAPI(flag=flag)

# 获取当前时间之前的BTC历史标记价格K线（history mark price candles）
now_ms = str(int(time.time() * 1000))
result = marketDataAPI.get_mark_price_candlesticks(
    instId="BTC-USD-SWAP",  # 可按需调整合约ID
    after=now_ms,
    bar="5m",
    limit="50"
)
print(result)

import asyncio
from okx.websocket.WsPublicAsync import WsPublicAsync

def callbackFunc(message):
    print(message)

async def main():
    ws = WsPublicAsync(url="wss://wspap.okx.com:8443/ws/v5/public")
    await ws.start()
    args = [{
        "channel": "mark-price",
        "instId": "BTC-USDT"
    }]

    await ws.subscribe(args, callback=callbackFunc)
    await asyncio.sleep(10)

    #await ws.unsubscribe(args, callback=callbackFunc)
    await asyncio.sleep(10)

while True:

    asyncio.run(main())


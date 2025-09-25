import asyncio
import json
from logger_config import setup_logger
from exchange_ws import get_binance_ticker_ws, get_binance_candle_ws
from nats.aio.client import Client as NATS
from NATS_setup import ensure_streams_from_yaml
import os
from alert_manager import send_alert

async def main():
    
    logger = setup_logger()
    logger.info(
                json.dumps({
                        "EventCode": 0,
                        "Message": f"Starting QuantFlow_DataCollector..."
                    })
            )
    
 
    symbols = ["BTC/USDT", "ETH/BTC"]
    timeframes= ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"]

    nc = NATS()
    await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
    await ensure_streams_from_yaml(nc, "streams.yaml")

    candle_tasks = [
        get_binance_candle_ws((base + quote).lower(), base, quote, timeframe, nc)
        for symbol in symbols
        for base, quote in [symbol.split("/")]
        for timeframe in timeframes
    ]
    
    ticker_tasks = [
        get_binance_ticker_ws((base + quote).lower(), base, quote, nc)
        for symbol in symbols
        for base, quote in [symbol.split("/")]
    ]
    
    await asyncio.gather(*candle_tasks, *ticker_tasks)
      
   
if __name__ == "__main__":
    asyncio.run(main())

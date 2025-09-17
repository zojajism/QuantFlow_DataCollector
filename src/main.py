import asyncio
from logger_config import setup_logger
from exchange_ws import get_binance_ticker_ws, get_binance_candle_ws

async def main():
    
    logger = setup_logger()
    logger.info("Starting QuantFlow_DataCollector system...")
    symbols = ["BTC/USDT", "ETH/BTC"]
    timeframes= ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"]

    
    candle_tasks = [
        get_binance_candle_ws((base + quote).lower(), base, quote, timeframe)
        for symbol in symbols
        for base, quote in [symbol.split("/")]
        for timeframe in timeframes
    ]
    
    ticker_tasks = [
        get_binance_ticker_ws((base + quote).lower(), base, quote)
        for symbol in symbols
        for base, quote in [symbol.split("/")]
    ]
    
    await asyncio.gather(*candle_tasks, *ticker_tasks)
   
   
if __name__ == "__main__":
    asyncio.run(main())

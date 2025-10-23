import asyncio
import json

import yaml
from logger_config import setup_logger
from exchange_ws import get_binance_ticker_ws, get_binance_candle_ws
from nats.aio.client import Client as NATS
from NATS_setup import ensure_streams_from_yaml
from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType
import os
from pathlib import Path
from dotenv import load_dotenv

CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"

async def main():
    
        
    try:

        # Try the Docker volume location first
        env_path = Path("/data/.env")
        # Fallback for local dev
        if not env_path.exists():
            env_path = Path(__file__).resolve().parent / "data" / ".env"
        load_dotenv(dotenv_path=env_path)

        logger = setup_logger()
        logger.info(
                    json.dumps({
                            "EventCode": 0,
                            "Message": f"Starting QuantFlow_DataCollector..."
                        })
               )
        
        await start_telegram_notifier()   
        notify_telegram(f"❇️ Data Collector App started....", ChatType.ALERT)
        
        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f) or {}

        symbols = [str(s) for s in config_data.get("symbols", [])]
        timeframes = [str(t) for t in config_data.get("timeframes", [])]
        
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
        
    finally:
        notify_telegram(f"⛔️ Data Collector App stopped.", ChatType.ALERT)
        await close_telegram_notifier()

if __name__ == "__main__":
    asyncio.run(main())

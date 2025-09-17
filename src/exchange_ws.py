import json
import websockets
from datetime import datetime, timezone
import logging


logger = logging.getLogger(__name__)
    
async def get_binance_ticker_ws(symbol: str, base_currency: str, quote_currency: str):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@ticker"
    async with websockets.connect(url) as ws:
       
        c_symbol = base_currency.upper() + "/" + quote_currency.upper()
        printSymbol = "'" + c_symbol + "'" + '        '
        printSymbol = printSymbol[:11]

        logger.info(f"Connected to Binance ticker stream for {c_symbol}")

        async for message in ws:
            msg = json.loads(message)
            
            ticker_data = {
                "exchange": "Binance",
                "tick_time": datetime.fromtimestamp(int(msg['E']) / 1000, tz=timezone.utc),
                "symbol": c_symbol,
                "base_currency": base_currency.upper(),
                "quote_currency": quote_currency.upper(),
                "bid": float(msg['b']),
                "ask": float(msg['a']),
                "last_price": float(msg['c']),
                "high": float(msg['h']),
                "low": float(msg['l']),
                "volume": float(msg['v'])
            }
            #tick_buffer.append(c_symbol, ticker_data)

            logger.info(f"Got tick: Ex: Binance, Symbol: {printSymbol}, Tick Time: {ticker_data['tick_time']}, Price: {ticker_data['last_price']}")

            
async def get_binance_candle_ws(symbol: str, base_currency: str, quote_currency: str, timeframe: str):
   
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{timeframe}"
    c_symbol = base_currency.upper() + "/" + quote_currency.upper()
    printSymbol = "'" + c_symbol + "'" + '        '
    printSymbol = printSymbol[:11]
        
    async with websockets.connect(url) as ws:
        
        logger.info(f"Connected to Binance candle stream for Symbol:'{c_symbol}', Timeframe: '{timeframe}'")
        print(f"Connected to Binance candle stream for Symbol:'{c_symbol}', Timeframe: '{timeframe}'")    
        async for message in ws:
            msg = json.loads(message)
            k = msg['k']

            # Only closed candles
            if k['x']:
                candle_data = {
                    "exchange": "Binance",
                    "symbol": c_symbol,
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "timeframe": k['i'],
                    "open_time": datetime.fromtimestamp(int(k['t']) / 1000, tz=timezone.utc),#.strftime('%Y-%m-%d %H:%M:%S'),
                    "open": float(k['o']),
                    "high": float(k['h']),
                    "low": float(k['l']),
                    "close": float(k['c']),
                    "volume": float(k['v']),
                    "close_time": datetime.fromtimestamp(int(k['T']) / 1000, tz=timezone.utc)#.strftime('%Y-%m-%d %H:%M:%S')
                }
                
                logger.info(f"Candle closed for Ex: Binance, Symbol: {printSymbol} , Timeframe: '{timeframe}', Close_Time: {candle_data['close_time']}")

                
               
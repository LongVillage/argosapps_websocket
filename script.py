import asyncio
import logging
from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES
from cryptofeed.exchanges import Binance

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def candle_callback(candle, receipt_timestamp):
    print(f"Candle received for {candle.symbol} at {candle.timestamp}:")
    print(f"Open={candle.open}, High={candle.high}, Low={candle.low}, Close={candle.close}, Volume={candle.volume}")

def main():
    f = FeedHandler()
    symbols = ['BTC-USDT-1m']
    f.add_feed(Binance(
        symbols=symbols,
        channels=[CANDLES],
        callbacks={CANDLES: candle_callback}
    ))
    f.run()

if __name__ == '__main__':
    main()

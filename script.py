import asyncio
import threading
import time
from multiprocessing import Process
import pymysql
import requests
import logging
from threading import Lock
from datetime import datetime
from pymysql.err import Error
from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES, OPEN_INTEREST
from cryptofeed.symbols import Symbol
from cryptofeed.exchanges import Binance, BinanceFutures, BinanceDelivery
from cryptofeed.callback import CandleCallback, OpenInterestCallback
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
latest_open_interest = {}
open_interest_lock = Lock()
f = None
feed_process = None
def convert_to_sql_datetime(timestamp_ms):
    newTimestramp = datetime.utcfromtimestamp(timestamp_ms).strftime('%Y-%m-%d %H:%M:%S')
    return newTimestramp
# Fonction pour insérer les données de bougie dans la base de données
def insert_candle_into_db(candle_data, pair_id):
    host = "db-argos-sql.cfc44smyuydl.eu-north-1.rds.amazonaws.com"
    port = 3306
    user = "admin"
    password = "!2#4%6AoEuId"
    db = "argos_candle"
    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db)
        cursor = conn.cursor()
        query = "INSERT INTO Candlestick (PairID, DateTime, Open, High, Low, Close, Volume, OpenInterest) VALUES (%s, %s, %s, %s, %s, %s,%s,%s)"
        cursor.execute(query, (pair_id,) + candle_data)
        conn.commit()
        logging.info('Candle data inserted into DB')
    except Error as e:
        logging.error(f"Error while inserting into MySQL: {e}")
    finally:
        conn.close()
        print('Candle data inserted into DB')
# Define a callback for candle
async def candle_callback(candle, receipt_timestamp):
    pair_id = pair_id_mapping.get(candle.symbol)
    if pair_id:
        # Retrieve the latest open interest data
        with open_interest_lock:
            open_interest = latest_open_interest.get(f"{candle.symbol}-PERP", 0)
            print (f"{candle.symbol}-PERP: {open_interest}")
        candle_data = (
            convert_to_sql_datetime(candle.timestamp),
            float(candle.open),
            float(candle.high),
            float(candle.low),
            float(candle.close),
            float(candle.volume),
            float(open_interest)
        )
        insert_candle_into_db(candle_data, pair_id)
# Define a callback for open interest
async def open_interest_handler(data, receipt_timestamp):
    with open_interest_lock:
        latest_open_interest[data.symbol] = data.open_interest
def get_argos_pairs():
    try:
        logging.info("1234")
        url = "https://bff.argos-apps.com/api/currencyPairs"
        response = requests.get(url)
        logging.info(response)
        logging.info("test")
        data = response.json()
        logging.info('test2')
        return data['currencyPairs']
    except requests.RequestException as e:
        logging.error(f"Error fetching Argos pairs: {e}")
        return []
def parse_currency_pairs(json_data):
    pairs = []
    for pair in json_data:
        symbol = pair['baseCurrency']['symbol'].upper() + '-' + pair['quoteCurrency']['symbol'].upper()
        pairs.append(symbol)
    return pairs
 
# Main function
def run_feed_handler():
    global f
    
    try:
        logging.info("Feed handler starting.")
        argos_pairs = get_argos_pairs()
        
        if not argos_pairs:
            logging.error("Failed to fetch Argos pairs, stopping feed handler.")
            return
        currency_pairs = parse_currency_pairs(argos_pairs)
        
        
        #currency_pairs = ["BTC-USDT", "ETH-USDT", "BNB-USDT", "SOL-USDT", "XRP-USDT"]
        
        currency_perp_pairs = [pair+"-PERP" for pair in currency_pairs]
        
        binancePairs = Binance.symbols()
        logging.info(f"binancePairs: {binancePairs}")
        binanceFuturesPairs = BinanceFutures.symbols()
        logging.info(f"binanceFuturesPairs {binanceFuturesPairs}")
        common_pairs = [pair for pair in currency_pairs if pair in binancePairs]
        common_perp_pairs= [pair for pair in currency_perp_pairs if pair in binanceFuturesPairs]
        logging.info(common_pairs)
        logging.info(common_perp_pairs)
        global pair_id_mapping
        pair_id_mapping = {pair_symbol: argos_pair['id'] for pair_symbol in common_pairs for argos_pair in argos_pairs if argos_pair['baseCurrency']['symbol'].upper() + '-' + argos_pair['quoteCurrency']['symbol'].upper() == pair_symbol}
        if f is not None:
            f.stop()
        f = FeedHandler()

        logging.info(pair_id_mapping)
        #BINANCE
        f.add_feed(BinanceFutures(symbols=common_perp_pairs, channels=[OPEN_INTEREST], callbacks={OPEN_INTEREST: OpenInterestCallback(open_interest_handler)}))
        f.add_feed(Binance(symbols=common_pairs, channels=[CANDLES], candle_interval="15m", callbacks={CANDLES: CandleCallback(candle_callback)}))
        #f.add_feed(CoinBase(etc))
        
        f.run()
    
    except Exception as e:
        logging.error(f"An error occurred in the feed handler: {e}")
def restart_feed_process():
    """
    Safely terminates any existing feed handler process and starts a new one.
    """
    global feed_process
    if feed_process and feed_process.is_alive():
        logging.info("Terminating the existing feed process.")
        
        feed_process.terminate()
        feed_process.join()
    
    logging.info("Starting a new feed process.")
    feed_process = Process(target=run_feed_handler)
    
    feed_process.start()
    
def scheduled_run(interval=43200):
    """
    Schedules the feed handler to restart at the specified interval (default is 12 hours).
    """
    while True:
        restart_feed_process()
        logging.info(f"Feed process restarted. Waiting {interval} seconds before next restart.")
        time.sleep(interval)
def start_scheduled_run():
    """
    Starts the scheduled restart logic in a separate thread to avoid blocking the main application.
    """
    from threading import Thread
    
    logging.info("Starting scheduled feed handler management in a separate thread.")
    thread = Thread(target=scheduled_run, daemon=True)
    thread.start()
    
    return thread
def stop_run():
    global feed_process
    if feed_process is not None and feed_process.is_alive():
        feed_process.terminate()
        feed_process.join()
        feed_process = None
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    #scheduler_thread = start_scheduled_run()
    run_feed_handler()
if __name__ == '__main__':
    main()
#HNT-USDT: NOT IN
#BSV-USDT: NOT IN
#KAS-USDT: NOT IN
#DOGE-USDC: NOT IN
#1000RATS-USDT: NOT IN
#SRM-USDT: NOT IN
#BTS-USDT: NOT IN
#ORBS-USDT: NOT IN
#1000BONK-USDT: NOT IN
#PYTH-USDT: NOT IN
#1000SHIB-USDT: NOT IN
#TOKEN-USDT: NOT IN
#BTCST-USDT: NOT IN
#WIF-USDT: NOT IN
#ONDO-USDT: NOT IN
#BIGTIME-USDT: NOT IN
#DEFI-USDT: NOT IN
#ETHW-USDT: NOT IN

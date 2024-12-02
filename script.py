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
from cryptofeed.exchanges import Binance, BinanceFutures

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

latest_open_interest = {}
open_interest_lock = Lock()

f = None
feed_process = None

def convert_to_sql_datetime(timestamp_ms):
    newTimestamp = datetime.utcfromtimestamp(timestamp_ms).strftime('%Y-%m-%d %H:%M:%S')
    return newTimestamp

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
        query = "INSERT INTO Candlestick (PairID, DateTime, Open, High, Low, Close, Volume, OpenInterest) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (pair_id,) + candle_data)
        conn.commit()
        logging.info('Candle data inserted into DB')

    except Error as e:
        logging.error(f"Error while inserting into MySQL: {e}")
    finally:
        conn.close()
        print('Candle data inserted into DB')

# Définition du callback pour les bougies
async def candle_callback(feed, symbol, timestamp, receipt_timestamp, open_price, close_price, high_price, low_price, volume, closed, interval):
    pair_id = pair_id_mapping.get(symbol)
    if pair_id:
        # Récupérer les dernières données d'Open Interest
        with open_interest_lock:
            open_interest = latest_open_interest.get(f"{symbol}-PERP", 0)
            print(f"{symbol}-PERP: {open_interest}")

        candle_data = (
            convert_to_sql_datetime(timestamp),
            float(open_price),
            float(high_price),
            float(low_price),
            float(close_price),
            float(volume),
            float(open_interest)
        )
        insert_candle_into_db(candle_data, pair_id)

# Définition du callback pour l'Open Interest
async def open_interest_handler(feed, symbol, open_interest, receipt_timestamp):
    with open_interest_lock:
        latest_open_interest[symbol] = open_interest

def get_argos_pairs():
    try:
        print("1234")
        url = "https://bff.argos-apps.com/api/currencyPairs"
        response = requests.get(url)
        print(response)
        print("test")
        data = response.json()
        print('test2')
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

# Fonction principale
def run_feed_handler():
    global f

    try:
        logging.info("Feed handler starting.")

        argos_pairs = get_argos_pairs()

        if not argos_pairs:
            logging.error("Failed to fetch Argos pairs, stopping feed handler.")
            return

        currency_pairs = ["BTC-USDT", "ETH-USDT", "BNB-USDT", "SOL-USDT", "XRP-USDT"]
        currency_perp_pairs = [pair + '-PERP' for pair in currency_pairs]

        binancePairs = Binance.symbols()
        binanceFuturesPairs = BinanceFutures.symbols()

        common_pairs = [pair for pair in currency_pairs if pair in binancePairs]
        common_perp_pairs = [pair for pair in currency_perp_pairs if pair in binanceFuturesPairs]

        global pair_id_mapping
        pair_id_mapping = {
            pair_symbol: argos_pair['id']
            for pair_symbol in common_pairs
            for argos_pair in argos_pairs
            if argos_pair['baseCurrency']['symbol'].upper() + '-' + argos_pair['quoteCurrency']['symbol'].upper() == pair_symbol
        }

        if f is not None:
            f.stop()

        f = FeedHandler()
        # BINANCE FUTURES
        f.add_feed(BinanceFutures(
            symbols=common_perp_pairs,
            channels=[OPEN_INTEREST],
            callbacks={OPEN_INTEREST: open_interest_handler}
        ))
        # BINANCE SPOT
        f.add_feed(Binance(
            symbols=common_pairs,
            channels=[CANDLES],
            candle_interval='15m',  # Utilisation de l'intervalle en chaîne de caractères
            callbacks={CANDLES: candle_callback}
        ))

        f.run()

    except Exception as e:
        logging.error(f"An error occurred in the feed handler: {e}")

def restart_feed_process():
    """
    Termine en toute sécurité tout processus de gestionnaire de flux existant et en démarre un nouveau.
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
    Programme le gestionnaire de flux pour redémarrer à l'intervalle spécifié (par défaut toutes les 12 heures).
    """
    while True:
        restart_feed_process()
        logging.info(f"Feed process restarted. Waiting {interval} seconds before next restart.")
        time.sleep(interval)

def start_scheduled_run():
    """
    Démarre la logique de redémarrage programmé dans un thread séparé pour éviter de bloquer l'application principale.
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

    # scheduler_thread = start_scheduled_run()
    run_feed_handler()

if __name__ == '__main__':
    main()

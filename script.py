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

# Désactiver uvloop pour éviter l'erreur avec read_limit
asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

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
def insert_candle_into_db(symbol, interval, candle_data_values, pair_id):
    host = "db-argos-sql.cfc44smyuydl.eu-north-1.rds.amazonaws.com"
    port = 3306
    user = "admin"
    password = "!2#4%6AoEuId"
    db = "argos_candle"

    try:
        conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db)
        cursor = conn.cursor()
        query = """
        INSERT INTO Candlestick (PairID, DateTime, Open, High, Low, Close, Volume, OpenInterest)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (pair_id,) + candle_data_values)
        conn.commit()
        logging.info(f'Candle data for {symbol} ({interval}) inserted into DB at {candle_data_values[0]}')
    except Error as e:
        logging.error(f"Error while inserting into MySQL: {e}")
    finally:
        conn.close()

# Définition du callback pour les bougies
async def candle_callback(candle_data, receipt_timestamp):
    symbol = candle_data.symbol
    pair_id = pair_id_mapping.get(symbol)
    if pair_id:
        # Récupérer les dernières données d'Open Interest
        with open_interest_lock:
            open_interest = latest_open_interest.get(f"{symbol}-PERP", 0)
            print(f"{symbol}-PERP: {open_interest}")

        candle_data_values = (
            convert_to_sql_datetime(candle_data.timestamp),
            float(candle_data.open),
            float(candle_data.high),
            float(candle_data.low),
            float(candle_data.close),
            float(candle_data.volume),
            float(open_interest)
        )

        insert_candle_into_db(symbol, candle_data.interval, candle_data_values, pair_id)

# Définition du callback pour l'Open Interest
async def open_interest_handler(open_interest_data, receipt_timestamp):
    with open_interest_lock:
        latest_open_interest[open_interest_data.symbol] = open_interest_data.open_interest

def get_argos_pairs():
    try:
        print("Fetching Argos pairs...")
        url = "https://bff.argos-apps.com/api/currencyPairs"
        response = requests.get(url)
        data = response.json()
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

        # Récupérer les paires de devises à partir de argos_pairs
        currency_pairs = parse_currency_pairs(argos_pairs)
        currency_perp_pairs = [pair + '-PERP' for pair in currency_pairs]

        # Obtenir les symboles supportés par Binance et Binance Futures
        binancePairs = Binance.symbols()
        binanceFuturesPairs = BinanceFutures.symbols()

        # Filtrer les paires pour ne garder que celles supportées par les exchanges
        common_pairs = [pair for pair in currency_pairs if pair in binancePairs]
        common_perp_pairs = [pair for pair in currency_perp_pairs if pair in binanceFuturesPairs]

        # Afficher les informations pour le débogage
        print(f"Total pairs from Argos: {len(currency_pairs)}")
        print(f"Common pairs with Binance Spot: {len(common_pairs)}")
        print(f"Common pairs with Binance Futures: {len(common_perp_pairs)}")

        print("List of common pairs with Binance Spot:")
        print(common_pairs)

        print("List of common pairs with Binance Futures:")
        print(common_perp_pairs)

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
            candle_interval='15m',  # Utilisation de l'intervalle de 15 minutes
            callbacks={CANDLES: candle_callback}
        ))

        f.run()

    except Exception as e:
        logging.error(f"An error occurred in the feed handler: {e}")

def restart_feed_process():
    global feed_process
    if feed_process and feed_process.is_alive():
        logging.info("Terminating the existing feed process.")

        feed_process.terminate()
        feed_process.join()

    logging.info("Starting a new feed process.")
    feed_process = Process(target=run_feed_handler)

    feed_process.start()

def scheduled_run(interval=43200):
    while True:
        restart_feed_process()
        logging.info(f"Feed process restarted. Waiting {interval} seconds before next restart.")
        time.sleep(interval)

def start_scheduled_run():
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

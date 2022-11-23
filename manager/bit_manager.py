import logging
import signal
import string
import threading
from datetime import datetime

import schedule as schedule

import pyupbit as pu
import time
import pymysql as sql
import configparser as conf
import os


class BitManager:
    def __init__(self, log_file=None):
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.logger = logging.getLogger("BitManager")
        self.log_file = log_file

        if log_file:
            self.log_handler = logging.FileHandler(self.log_file)
            self.logger.addHandler(self.log_handler)

        self.__stop = False

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def __connect_db(self):
        # DB 커넥션 정보는 로컬의 파일에서 가져와야한다. (보안상)
        path = os.getcwd()
        config = conf.ConfigParser()
        config.read(path + "/config.ini")
        key = 'mariaDB'
        host = config[key]['host']
        user=config[key]['user']
        password=config[key]['password']
        db=config[key]['db']
        port=int(config[key]['port'])
        self.connection = sql.connect(
            host=config['mariaDB']['host'],
            user=config['mariaDB']['user'],
            password=config['mariaDB']['password'],
            db=config['mariaDB']['db'],
            port=int(config['mariaDB']['port'])
        )

    def __close_db(self):
        if self.connection is not None:
            self.connection.close()

    def __getMarkets(self, fiat="", is_detail=False):
        return pu.get_markets(fiat, is_details=is_detail)

    def __getTickers(self, fiat="", limit_info=False):
        retVal = pu.get_tickers(fiat, limit_info)
        return retVal

    def __sliceTickers(self, tickers):
        sliced_tickers = []
        if(len(tickers) > 100):
            for i in range(0, len(tickers), 100):
                if len(tickers) < (i + 100):
                    sliced_tickers.append(tickers[i:(i + (len(tickers) - i))])
                else:
                    sliced_tickers.append(tickers[i:(i + 100)])
            return sliced_tickers
        else:
            sliced_tickers.append(tickers)
            return sliced_tickers

    def __getCurrentPrices(self, ticker='KRW-BTC'):
        total_current_prices = {}
        if isinstance(ticker, list):
            tickers = ticker
            sliced_tickers = []

            if len(tickers) > 100:
                for i in range(0, len(tickers), 100):
                    if len(tickers) < (i + 100):
                        sliced_tickers.append(tickers[i:(i + (len(tickers) - i))])
                    else:
                        sliced_tickers.append(tickers[i:(i + 100)])

                for item in sliced_tickers:
                    total_current_prices.update(pu.get_current_price(item))
                    time.sleep(0.1)
            else:
                total_current_prices = pu.get_current_price(tickers)
        else:
            total_current_prices = pu.get_current_price(ticker)

        return total_current_prices

    def __getCurrentTickerInfo(self, ticker='KRW-BTC'):
        infos = []
        if isinstance(ticker, list):
            # ticker가 리스트이면
            tickers = ticker
            sliced_tickers = self.__sliceTickers(tickers)
            for tickers in sliced_tickers:
                infos += pu.get_current_price(tickers, True, False)
                time.sleep(0.1)
            return infos
        else:
            # ticker가 리스트가 아니면
            return pu.get_current_price(ticker, True, False)

    def __update_market(self):
        sql = "SELECT * FROM market"

        cur = self.connection.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()

        if len(results) == 0:
            cur = self.connection.cursor()
            markets = self.__getMarkets("KRW", True)
            values = []
            sql = "INSERT INTO market (market, korean_name, english_name, market_warning) VALUES(%s, %s, %s, %s)"

            for market in markets:
                values.append(
                    (market['market'], market['korean_name'], market['english_name'], market['market_warning']))

            cur.executemany(sql, values)
            self.connection.commit()
            cur.close()
        else:
            cur = self.connection.cursor()
            markets = self.__getMarkets("KRW", True)
            values = []
            sql = "INSERT INTO market (market, korean_name, english_name, market_warning) VALUES(%s, %s, %s, %s) ON DUPLICATE KEY UPDATE market_warning = VALUES(market_warning)"
            for market in markets:
                values.append((market['market'], market['korean_name'], market['english_name'], market['market_warning']))
            cur.executemany(sql, values)
            self.connection.commit()
            cur.close()

    def __get_tables(self):
        cur = self.connection.cursor()
        sql = "show tables"
        cur.execute(sql)
        results = cur.fetchall()
        tables = []
        for table in results:
            for name in table:
                tables.append(name)
        cur.close()
        return tables

    def __generate_ticker_table_name(self, market):
        temp = market.lower().split("-")
        table_name = "ticker_{0}_{1}".format(temp[0], temp[1])
        return table_name
    def __create_ticker_table(self):
        markets = self.__getMarkets("KRW")
        tables = self.__get_tables()
        tables_dictionary = dict(zip(tables, tables))
        cur = self.connection.cursor()
        for market in markets:
            table_name = self.__generate_ticker_table_name(market['market'])
            if not table_name in tables_dictionary:
                # 테이블이 존재하지 않으면 생성한다.
                sql = "CREATE TABLE {0} ( market varchar(100) not null, trade_date varchar(100) not null, trade_time varchar(100) not null, trade_date_kst varchar(100) not null, trade_time_kst varchar(100) not null, trade_timestamp bigint not null, opening_price double not null, high_price double not null, low_price double not null, trade_price double not null, prev_closing_price double not null, `change` varchar(10) not null, change_price double not null, change_rate double not null, signed_change_price double not null, signed_change_rate double not null, trade_volume double not null, acc_trade_price double not null, acc_trade_price_24h double not null, acc_trade_volume double not null, acc_trade_volume_24h  double not null, highest_52_week_price double not null, highest_52_week_date  varchar(100) not null, lowest_52_week_price double not null, lowest_52_week_date varchar(100) not null, timestamp bigint not null)".format(table_name)
                cur.execute(sql)
        self.connection.commit()
        cur.close()

    def __start_update_tickers(self):
        if not self.__stop:
            threading.Timer(1, self.__start_update_tickers).start()
            start = time.time()
            cur = self.connection.cursor()
            tickers = self.__getTickers(fiat="KRW")
            infos = self.__getCurrentTickerInfo(tickers)
            for info in infos:
                sql = "insert into {0} (market, trade_date, trade_time, trade_date_kst, trade_time_kst, trade_timestamp, opening_price, high_price, low_price, trade_price, prev_closing_price, `change`, change_price, change_rate, signed_change_price, signed_change_rate, trade_volume, acc_trade_price, acc_trade_price_24h, acc_trade_volume, acc_trade_volume_24h, highest_52_week_price, highest_52_week_date, lowest_52_week_price, lowest_52_week_date, `timestamp`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                    self.__generate_ticker_table_name(info['market']))
                value = (
                info['market'], info['trade_date'], info['trade_time'], info['trade_date_kst'], info['trade_time_kst'],
                info['trade_timestamp'], info['opening_price'], info['high_price'], info['low_price'], info['trade_price'],
                info['prev_closing_price'], info['change'], info['change_price'], info['change_rate'],
                info['signed_change_price'], info['signed_change_rate'], info['trade_volume'], info['acc_trade_price'],
                info['acc_trade_price_24h'], info['acc_trade_volume'], info['acc_trade_volume_24h'],
                info['highest_52_week_price'], info['highest_52_week_date'], info['lowest_52_week_price'],
                info['lowest_52_week_date'], info['timestamp'])
                cur.execute(sql, value)
            self.connection.commit()
            cur.close()
            end = time.time()
            self.logger.info("[{0}] Updated tickers count : {1} -> {2}s".format(datetime.now(), len(infos), (end-start)))

    def run(self):
        # DB 연결
        self.__connect_db()

        # Market 업데이트
        # self.__update_market()

        self.__create_ticker_table()

        # Current Ticker 수집
        self.__start_update_tickers()
        while not self.__stop:
            self.logger.info("Running Main Loop")
            time.sleep(1)

        self.__close_db()

    def stop(self, signum, frame):
        self.__stop = True
        self.logger.info("Receive Signal {0}".format(signum))
        self.logger.info("Stop BitManager")



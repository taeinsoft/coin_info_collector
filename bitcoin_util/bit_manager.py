import pyupbit as pu
import time
import pymysql as sql
import configparser as conf


class BitManager:
    def __init__(self):
        pass

    def connect_db(self):
        # DB 커넥션 정보는 로컬의 파일에서 가져와야한다. (보안상)
        config = conf.ConfigParser()
        config.read('config.ini')
        self.connection = sql.connect(
            host=config['mariaDB']['host'],
            user=config['mariaDB']['user'],
            passwd=config['mariaDB']['passwd'],
            db=config['mariaDB']['db'],
            port=int(config['mariaDB']['port'])
        )
        self.cursor = self.connection.cursor()

    def close_db(self):
        if self.connection is not None:
            self.connection.close()

    def getMarket(self, is_detail=False):
        return pu.get_market(is_details=is_detail)

    def getTickers(self, fiat="", limit_info=False):
        retVal = pu.get_tickers(fiat, limit_info)
        return retVal

    def getCurrentPrices(self, ticker='KRW-BTC'):
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


def getMarket(param):
    return None
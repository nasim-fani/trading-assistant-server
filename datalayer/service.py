import csv
import os

import pandas as pd
import redis
import ta
from django.conf import settings


class RedisClient:
    __redis_client = None

    def __init__(self):
        self.__redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT,
                                                db=settings.REDIS_DB)

    def get_all_companies(self) -> list:
        return self.__redis_client.keys()

    def get_symbol(self, company_code: str):
        return self.__redis_client.get(company_code)


class Indicator:
    def calculate_indicator(self, indicator, stock, day):
        stock_df = pd.DataFrame.from_dict(stock, orient='index')
        window = int(indicator[3:])
        try:
            if 'Close' in stock_df.columns:
                close = stock_df['Close']
                if "RSI" in indicator:
                    res = ta.momentum.RSIIndicator(close=close, window=window).rsi().dropna()
                elif "BollingerBands" in indicator:
                    res = ta.volatility.BollingerBands(close=close,
                                                       window=window).bollinger_hband().dropna()  # todo check function
                elif "Stochastic" in indicator:
                    res = ta.momentum.stochrsi(close=close, window_slow=window).macd().dropna()  # todo window
                elif "MACD" in indicator:
                    res = ta.trend.MACD(close=close, window_slow=window).macd().dropna()  # todo window
                elif "MFI" in indicator:
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.volume.MFIIndicator(high=high, low=low, close=close,
                                                     window_slow=window).macd().dropna()
            return res[-day:]
        except Exception as ex:
            print(str(ex))
            print(stock)

    def group_map(self, id):
        csv_file = csv.reader(open('C:/Users/nasim/Desktop/project/EX_api/Groups.csv', "r", encoding="utf8"),
                              delimiter=",")
        for row in csv_file:
            if id == row[1]:
                return row[0]
        return "Not Found"

    def name_map(self, id, stock):
        csv_file = csv.reader(open('C:/Users/nasim/Desktop/project/EX_api/Companies.csv', "r", encoding="utf8"),
                              delimiter=",")
        stock_df = pd.DataFrame.from_dict(stock, orient='index')
        last = stock_df.tail(1)
        close = "unspecified"
        trend = -1
        if 'Close' in last.columns:
            close = last.iloc[-1]["Close"]
            if 'Open' in last.columns:
                trend = last.iloc[-1]["Open"] > close

        name = id.split(":")[2].split("-")[0]
        for row in csv_file:
            if name == row[4]:
                return {'id': id, 'name': row[2], 'group': self.group_map(row[9]), 'close': close, 'trend': int(trend)}
        return "Not Found"

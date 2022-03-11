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
    def calculate_indicator(self, filtering, stock, day):
        stock_df = pd.DataFrame.from_dict(stock, orient='index')
        indicator = filtering["name"]
        try:
            if 'Close' in stock_df.columns:
                close = stock_df['Close']
                if indicator == "RSI":
                    res = ta.momentum.RSIIndicator(close=close, window=filtering["window"]).rsi().dropna()
                elif indicator == "ROC":
                    res = ta.momentum.ROCIndicator(close=close, window=filtering["window"]).roc().dropna()
                elif indicator == "BollingerBands":
                    res = ta.volatility.BollingerBands(close=close, window=filtering["window"]).bollinger_hband().dropna()
                elif indicator == "StochRSI":
                    res = ta.momentum.stochrsi(close=close, window=filtering["window"], smooth1=filtering["smooth1"],
                                               smooth2=filtering["smooth2"]).stochrsi().dropna()
                elif indicator == "MACD":
                    res = ta.trend.MACD(close=close, window_slow=filtering["window_slow"]).macd().dropna()
                elif indicator == "MFI":
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.volume.MFIIndicator(high=high, low=low, close=close,
                                                     window_slow=filtering["window_slow"]).macd().dropna()  # todo functionesh ghalate

                elif indicator == "KAMA":
                    res = ta.momentum.KAMAIndicator(close=close, window=filtering["window"], pow1=filtering["pow1"],
                                                    pow2=filtering["pow2"]).dropna()
                elif indicator == "PPO":
                    res = ta.momentum.PercentagePriceOscillator(close=close, window_slow=filtering["window_slow"],
                                                                window_fast=filtering["window_fast"],
                                                                window_sign=filtering["window_sign"]).ppo().dropna()
                elif indicator == "StochasticOscillator":
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.momentum.StochasticOscillator(high=high, low=low, close=close, window=filtering["window"],
                                                               smooth_window=filtering[
                                                                   "smooth_window"]).stoch().dropna()
                elif indicator == "TSI":
                    res = ta.momentum.TSIIndicator(close=close, window_slow=filtering["window_slow"],
                                                   window_fast=filtering["window_fast"]).tsi().dropna()

                elif indicator == "UltimateOscillator":
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.momentum.UltimateOscillator(high=high, low=low, close=close, window1=filtering["window1"],
                                                             window2=filtering["window2"], window3=filtering["window3"],
                                                             weight1=filtering["weight1"], weight2=filtering["weight2"],
                                                             weight3=filtering[
                                                                 "weight3"]).ultimate_oscillator().dropna()
                elif indicator == "WilliamsR":
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.momentum.WilliamsRIndicator(high=high, low=low, close=close, lbp=filtering["lbp"]) \
                            .williams_r().dropna()
                elif indicator == "stoch":
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.momentum.awesome_oscillator(high=high, low=low, close=close, window=filtering["window"],
                                                             smooth_window=filtering["smooth_window"]).dropna()
            else:
                if indicator == "AwesomeOscillatorIndicator":
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.momentum.AwesomeOscillatorIndicator(high=high, low=low, window1=filtering["window1"],
                                                                     window2=filtering[
                                                                         "window2"]).awesome_oscillator().dropna()
                elif indicator == "awesome_oscillator":
                    if 'High' and 'Low' in stock_df.columns:
                        high = stock_df['High']
                        low = stock_df['Low']
                        res = ta.momentum.awesome_oscillator(high=high, low=low, window1=filtering["window1"],
                                                             window2=filtering["window2"]).dropna()
                elif indicator == "PVO":
                    if 'Volume' in stock_df.columns:
                        res = ta.momentum.PercentageVolumeOscillator(volume=stock_df['Volume'], window_slow=filtering["window_slow"],
                                                                     window_fast=filtering["window_fast"],
                                                                     window_sign=filtering[
                                                                         "window_sign"]).pvo().dropna()

            return res[-day:]
        except Exception as ex:
            print(str(ex))

    def group_map(self, stock_id):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, '../Groups.csv')
        csv_file = csv.reader(open(filename, "r", encoding="utf8"),
                              delimiter=",")
        for row in csv_file:
            if stock_id == row[1]:
                return row[0]
        return "Not Found"

    def name_map(self, stock_id, stock):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, '../Companies.csv')
        csv_file = csv.reader(open(filename, "r", encoding="utf8"),
                              delimiter=",")
        stock_df = pd.DataFrame.from_dict(stock, orient='index')
        last = stock_df.tail(1)
        close = -1
        trend = -1
        if 'Close' in last.columns:
            close = last.iloc[-1]["Close"]
            if 'Open' in last.columns:
                trend = last.iloc[-1]["Open"] > close
        name = stock_id.split(":")[2].split("-")[0]
        for row in csv_file:
            if name == row[4]:
                return {'id': name, 'name': row[2], 'group': self.group_map(row[9]), 'close': close,
                        'trend': int(trend), 'group_id': int(row[9])}
        return "Not Found"

    def graph(self, stock_id, stock):
        data = []
        stock_df = pd.DataFrame.from_dict(stock, orient='index')
        stock_df = stock_df.reset_index()  # make sure indexes pair with number of rows
        for index, row in stock_df.iterrows():
            if (
                    row['Close'] and row['Open'] and row['High'] and row['Low']):
                data.append({"High": row['High'], "Low": row['Low'], "Close": row['Close'], "Open": row['Open'],
                             "index": row[0]})
        return data

import redis
from django.conf import settings
import pandas as pd
import ta


class RedisClient:
    __redis_client = None

    def __init__(self):
        self.__redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT,
                                                db=settings.REDIS_DB)

    def get_all_companies(self) -> list:
        return self.__redis_client.keys()

    def get_stocks(self, company_code: int):
        return self.__redis_client.get(company_code)


class Indicator:
    def calculate_indicator(self, indicator, stock, day):
        stock_df = pd.DataFrame.from_dict(stock, orient='index')
        # print(stock_df)
        if "RSI" in indicator:
            try:
                window = int(indicator[3:])
                if 'Close' in stock_df.columns:
                    close = stock_df['Close']
                    res = ta.momentum.RSIIndicator(close=close[-day:], window=window).rsi().dropna()
                    # print(res)
                    return res
            except Exception as ex:
                print(str(ex))
                print(stock)
            # return stock

import json
from rest_framework.decorators import api_view
from rest_framework.response import Response
from datalayer.service import Indicator, RedisClient
from mainapp.models import Response as response
from django.http import JsonResponse, HttpResponse
import numpy as np
from rest_framework import status
import pickle
import pandas as pd

redis_client = RedisClient()
indicator_service = Indicator()


def get_company_codes():
    try:
        data = redis_client.get_all_companies()
        company_list = [item.decode('utf-8') for item in data]
        company_list_price = list(filter(lambda x: 'Price' in x, company_list))
        serialize = json.dumps([i for i in company_list_price])
        return serialize
    except Exception as ex:
        print(str(ex))


@api_view(['GET'])
def get_symbols(request):
    try:
        result = []
        codes = json.loads(get_company_codes())
        for code in codes:
            stock = pickle.loads(redis_client.get_symbol(code))
            result.append(indicator_service.name_map(code, stock))
        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'OK!'
        res.result = json.loads(json.dumps(result))
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = None
        return Response(data=json.loads(json.dumps(res.__dict__)))


@api_view(['GET'])
def get_symbol(request, stock_id):
    try:
        full_id = ":1:" + stock_id + "-Price"
        stock = pickle.loads(redis_client.get_symbol(full_id))
        result = indicator_service.graph(stock_id, stock)
        # result.stock_id = stock_id
        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'OK!'
        res.result = json.loads(json.dumps(result))
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = None
        return Response(data=json.loads(json.dumps(res.__dict__)))


@api_view(['GET'])
def get_indicators(request):
    try:
        indicators = [{"name": "RSI", "params": ["window"]},
                      {"name": "ROC", "params": ["window"]},
                      {"name": "BollingerBands", "params": ["window"]},
                      {"name": "StochRSI", "params": ["window", "smooth1", "smooth2"]},
                      {"name": "MACD", "params": ["window_slow"]},
                      {"name": "MFI", "params": ["window_slow"]},
                      {"name": "KAMA", "params": ["window", "pow1", "pow2"]},
                      {"name": "PPO", "params": ["window_slow", "window_fast", "window_sign"]},
                      {"name": "StochasticOscillator", "params": ["window", "smooth_window"]},
                      {"name": "TSI", "params": ["window_slow", "window_fast"]},
                      {"name": "UltimateOscillator",
                       "params": ["window1", "window2", "window3", "weight1", "weight2", "weight3"]},
                      {"name": "WilliamsR", "params": ["lbp"]},
                      {"name": "stoch", "params": ["window", "smooth_window"]},
                      {"name": "AwesomeOscillatorIndicator", "params": ["window1", "window2"]},
                      {"name": "PVO", "params": ["window_slow", "window_fast", "window_sign"]}
                      ]
        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'OK!'
        res.indicators = json.loads(json.dumps(indicators))
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = None
        return Response(data=json.loads(json.dumps(res.__dict__)))


def filter_stock(filtering, period):
    try:
        comparator = filtering["comparator"]
        operand = filtering["operand"]
        result = []
        data = redis_client.get_all_companies()
        company_list = [item.decode('utf-8') for item in data]
        company_list_price = list(filter(lambda x: 'Price' in x, company_list))
        for company_price in company_list_price:
            stock = pickle.loads(redis_client.get_symbol(company_price))
            calculated_indicator = indicator_service.calculate_indicator(filtering, stock, period)
            if calculated_indicator is None:
                continue
            if comparator == 'eq':
                calculated_indicator = calculated_indicator[calculated_indicator == operand]
            elif comparator == 'gr':
                calculated_indicator = calculated_indicator[calculated_indicator > operand]
            elif comparator == 'ls':
                calculated_indicator = calculated_indicator[calculated_indicator < operand]
            if len(calculated_indicator) > 0:
                result.append(indicator_service.name_map(company_price, stock))
        return result
    except Exception as ex:
        print(str(ex))


@api_view(['GET'])
def filter_list(request):
    try:
        body_unicode = json.loads(request.body.decode('utf-8'))
        final = []
        filters = body_unicode["filters"]
        for filtering in filters:
            filtered = filter_stock(filtering, body_unicode["period"])
            if len(final) == 0:
                final = filtered
            final = [value for value in final if value in filtered]
            # print(final)
        print(len(final))
        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'Ok!'
        res.stocks = json.loads(json.dumps(final))
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        # print(str(ex))
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = str(ex)
        return Response(data=json.loads(json.dumps(res.__dict__)))

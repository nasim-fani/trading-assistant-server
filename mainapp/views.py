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
    # try:
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


# except Exception as ex:
#     res = response()
#     res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
#     res.status_text = str(ex)
#     res.result = None
#     return Response(data=json.loads(json.dumps(res.__dict__)))


@api_view(['GET'])
def get_indicators(request):
    try:
        indicators = ["RSI5", "RSI14", "MACD26", "BollingerBands20", "Stochastic14", "MFI14"]  # todo update list
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


def get_stocks(operator: str, number: int, indicator: str, days: int):
    try:
        result = []
        data = redis_client.get_all_companies()
        company_list = [item.decode('utf-8') for item in data]
        company_list_price = list(filter(lambda x: 'Price' in x, company_list))
        for company_price in company_list_price:
            stock = pickle.loads(redis_client.get_symbol(company_price))
            calculated_indicator = indicator_service.calculate_indicator(indicator, stock, days)
            if calculated_indicator is None:
                continue
            if operator == 'eq':
                calculated_indicator = calculated_indicator[calculated_indicator == number]
            elif operator == 'gr':
                calculated_indicator = calculated_indicator[calculated_indicator > number]
            elif operator == 'ls':
                calculated_indicator = calculated_indicator[calculated_indicator < number]
            if len(calculated_indicator) > 0:
                result.append(indicator_service.name_map(company_price))
        return result
    except Exception as ex:
        print(str(ex))


@api_view(['GET'])
def filter_list(request):
    try:
        body_unicode = json.loads(request.body.decode('utf-8'))
        final = []
        # body = body_unicode.json()
        # content = body['content']
        # print(len(body_unicode))
        for i in range(0, len(body_unicode)):
            filtered = get_stocks(body_unicode[i]['operator'], body_unicode[i]['number'], body_unicode[i]['indicator'],
                                  body_unicode[i]['days'])
            if len(final) == 0:
                final = filtered
            final = [value for value in final if value in filtered]
            # print(final)
        print(len(final))
        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'Ok!'
        res.result = json.loads(json.dumps(final))
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        print(str(ex))
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = str(ex)
        return Response(data=json.loads(json.dumps(res.__dict__)))

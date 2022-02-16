import json
from rest_framework.decorators import api_view
from rest_framework.response import Response
from datalayer.service import Indicator, RedisClient
from mainapp.models import Response as response
from django.http import JsonResponse

from rest_framework import status
import pickle
import pandas as pd

redis_client = RedisClient()
indicator_service = Indicator()


@api_view(['GET'])
def get_company_codes(request):
    try:
        data = redis_client.get_all_companies()
        company_list = [item.decode('utf-8') for item in data]
        serialize = json.dumps([i for i in company_list])
        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'OK!'
        res.result = json.loads(serialize)
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = None
        return Response(data=json.loads(json.dumps(res.__dict__)))


@api_view(['GET'])
def get_andicators(request):
    try:
        andicators = ["RSI", "MACD"]  # todo update list
        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'OK!'
        res.result = json.loads(json.dumps(andicators))
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = None
        return Response(data=json.loads(json.dumps(res.__dict__)))


@api_view(['GET'])
def get_stocks(request, operator: str, number: int, indicator: str):
    try:
        result = []
        data = redis_client.get_all_companies()
        company_list = [item.decode('utf-8') for item in data]
        company_list_price = list(filter(lambda x: 'Price' in x, company_list))
        for company_price in company_list_price:
            stock = pickle.loads(redis_client.get_stocks(company_price))
            calculated_indicator = indicator_service.calculate_indicator(indicator, stock)
            if calculated_indicator is None:
                continue
            if operator == 'maceq':
                calculated_indicator = calculated_indicator[calculated_indicator == number]
            elif operator == 'macg':
                calculated_indicator = calculated_indicator[calculated_indicator > number]
            elif operator == 'macl':
                calculated_indicator = calculated_indicator[calculated_indicator < number]
            if len(calculated_indicator) > 0:
                result.append(company_price)

        res = response()
        res.status_code = status.HTTP_200_OK
        res.status_text = 'Ok!'
        res.result = json.loads(json.dumps(result))
        return Response(data=json.loads(json.dumps(res.__dict__)))
    except Exception as ex:
        res = response()
        res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        res.status_text = str(ex)
        res.result = None
        return Response(data=json.loads(json.dumps(res.__dict__)))

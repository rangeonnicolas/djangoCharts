# -*- coding: utf-8 -*-

from django.shortcuts import render
from django.http import  Http404
import sys
import datetime as dt
import pandas as pd
from django.http import HttpResponse
import dateutil.parser

temp_path = "macharts/getdata" # sans slash a la fin

sys.path.insert(0, temp_path+'/classes/')
sys.path = list(set(sys.path))
import BucketStats as ma
import BucketMongoDBConnector as dbc
import RecoMongoDBConnector as rsc
import RecoStats as rs


from django.contrib.auth.decorators import login_required


def getdata(request,filename,marketname,start = None, end = None):

    if request.GET.get('start'):
        start = dateutil.parser.parse(request.GET.get('start'))
    if request.GET.get('end'):
        end   = dateutil.parser.parse(request.GET.get('end'))

    # Dates de debut et fin des donnees
    if end is None:                                                                                                     # TODO: verifier les dates
        end = dt.datetime.now()
    if start is None:
        start = end - dt.timedelta(30,0,0)

    # Connection a la base
    dbConnector = dbc.BucketMongoDBConnector(,,)
    db =  ma.BucketStats(dbConnector)

    # Recuperation de l'id de la market
    if marketname == "all" :                                                                                             # TODO: code a deplacer
        id_market = None
    else :
        id_market = db.getMarketIdFromName(marketname)
        if id_market is None :
            raise BaseException("The name of market was not found")


    # Chragement de la donnee
    ret = None
    if filename == "orders_creation.csv":
        ret = getdata_orders_creation(request,id_market,db,start,end)
    elif filename == "orders_sizes.csv":
        ret = getdata_orders_sizes(request,id_market,db,start,end)
    elif filename == "active_users.csv":
        ret = getdata_active_users(request,id_market,db,start,end)
    elif filename == "events.json":
        ret = getdata_all_events(request,id_market,db,start,end)
    elif filename == "reco_rules_frequencies.csv":
        ruleset_id = None
        if request.GET.get('ruleset_id'):
            ruleset_id = request.GET.get('ruleset_id')
        recoDbConnector = rsc.RecoMongoDBConnector(,,)
        dbReco = rs.RecoStats(recoDbConnector)
        ret = getdata_rules_frequencies(request,id_market,dbReco,dbConnector,ruleset_id)


    if ret is None:
        raise Http404
    else:
        return ret


@login_required
def getdata_orders_creation(request,id_market,db,start,end):

    # Recuperation de la donnee
    data = db.getOrdersEvolution(id_market, start,end,granularity="day",
                                    cumul = True,granularityOfFirstDateOfPeriod = "day",cumulIncludeBounds=True)

    return _create_csv_from_dataframe(data)

@login_required
def getdata_orders_sizes(request,id_market,db,start,end):

    data = db.getOrderSizes(id_market, start,end,granularity = "day",cumul = True,sort = False,
                              granularityOfFirstDateOfPeriod = "day", cumulIncludeBounds = True)

    return _create_csv_from_dataframe(data)

@login_required
def getdata_active_users(request,id_market,db,start,end):

    data = db.getActiveUsers(start, end, granularity = "day", granularityOfFirstDateOfPeriod= "day", id_market= id_market)

    return _create_csv_from_dataframe(data)

@login_required
def getdata_all_events(request,id_market,db,start,end):

    data = db.getEvents(start, user_id= None, dateMax= end, id_market=id_market, eventType= "all")

    return _create_json_from_dataframe(data)


def getdata_rules_frequencies(request,id_market,dbReco, bucketDbConnector,ruleset_id = None):


    print(ruleset_id)
    data = dbReco.getStatsByItem(bucketDbConnector,market_id=str(id_market),ruleset_id=ruleset_id) #id_market est de type bson.ObjectId. On le convertit en str

    return _create_csv_from_dataframe(data)

def _create_csv_from_dataframe(data):

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="data.csv"'
    response.write(data.to_csv(index=False))

    return response

def _create_json_from_dataframe(data):

    import json
    response = HttpResponse(content_type='text/javascript')
    response.write(data.to_json(orient='split'))

    return response


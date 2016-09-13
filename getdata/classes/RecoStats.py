# -*- coding: utf-8 -*-

import pandas as pd
from pandas import DataFrame
import numpy as np
import datetime as dt
import RecoMongoDBConnector as dbc
import re


class RecoStats:

    def __init__(self,dbConnector):
        self.dbConnector = dbConnector

    # todo: faire une anotation ici
    def getStatsByItem(self, bucketDbConnector, ruleset_id= None, market_id= None):

        columnsOfTheReturnedDataFrame = ['item','premises','conclusions','views']
        result = self._getStatsByItem(bucketDbConnector, ruleset_id, market_id)
        if result is None:
            return pd.DataFrame(None,columns=columnsOfTheReturnedDataFrame)
        return pd.DataFrame(result, columns= columnsOfTheReturnedDataFrame)

    def _getStatsByItem(self, bucketDbConnector, ruleset_id= None, market_id= None):

        if ruleset_id is None:
            ruleset = self.dbConnector.getLastRuleSet(market_id)
        else:
            ruleset = self.dbConnector.getRuleSetById(ruleset_id)

        if not ruleset:
            return None

        premises = ruleset.getAllPremises()

        conclusions = ruleset.getAllConclusions()
        # merging the 2 lists and removing duplicates
        all_ids = list(set(premises+conclusions))

        premisesCount = DataFrame(pd.value_counts(premises), columns=['premises'])
        conclusionsCount = DataFrame(pd.value_counts(conclusions), columns=['conclusions'])
        premisesCount['conclusions'] = 0
        conclusionsCount['premises'] = 0

        result = premisesCount.append(conclusionsCount)
        result = result.groupby(level=0).sum()

        item_views = bucketDbConnector.getNumberOfViewsByItem(
            dateMin= ruleset.startDate,
            dateMax= ruleset.endDate,
            id_market= market_id)

        result['views'] = 0

        for event in item_views:
            event['count'] = event['count']
            if event['_id'] in result.index:
                result.ix[event['_id'],'views'] = event['count']
            else:                                               # todo: gros effort d'optimisation à fournir ici
                result = result.append(
                    pd.DataFrame([[0,0,event['count']]],
                                 columns=result.columns,
                                 index=[event['_id']]))

        result['item'] = result.index

        return result.sort('views')

# -*- coding: utf-8 -*-

import datetime as dt
from MongoDBConnector import MongoDBConnector
import bson

class Rule:
    PREMISE_FIELD = 'premise'
    CONCLUSION_FIELD = 'conclusion'

    def __init__(self, ruleDict):
        self.premises = ruleDict[self.PREMISE_FIELD]
        self.conclusions = ruleDict[self.CONCLUSION_FIELD]

    def getPremisesIds(self):
        result=[]
        for premise in self.premises:
            result += [premise['_id']]
        return result
    def getConclusionsIds(self):
        result=[]
        for conclusion in self.conclusions:
            result += [conclusion['_id']]
        return result

class RuleSet:
    RULE_FIELD = 'rules'
    PERIOD_FIELD = 'period'
    DATE_FIELD = 'date'
    MARKET_ID_FIELD = 'market'

    def __init__(self,ruleSetFromDb):

        self.rules = []
        for i,ruleDict in enumerate(ruleSetFromDb):
            self.rules += [Rule(ruleDict)]

        self.period = dt.timedelta(float(ruleDict[self.PERIOD_FIELD])/86400000)
        self.endDate = ruleDict[self.DATE_FIELD]
        self.market_id = ruleDict[self.MARKET_ID_FIELD]

    def getAllPremises(self, id_only=True):
        result = []
        if id_only:
            for rule in self.rules:
                result += rule.getPremisesIds()
        else:
            for rule in self.rules:
                result += rule.premises
        return result

    def getAllConclusions(self, id_only=True):
        result = []
        if id_only:
            for rule in self.rules:
                result += rule.getConclusionsIds()
        else:
            for rule in self.rules:
                result += rule.conclusions
        return result

    def _getStartDate(self):
        return self.endDate - self.period

    startDate = property(_getStartDate)

class RecoMongoDBConnector(MongoDBConnector):
    """
    Connects and executes queries on mongo DataBase where are stored all the information used by the recommendation
    system
    """

    RULE_COLLECTION_NAME = 'RecommendationRule'
    MARKET_ID_FIELD = 'market'
    RULE_SET_ID_FIELD    = 'rulesetId'

    def getLastRuleSet(self, market_id= None):

        ruleSetId = self._getLatestRuleSetId(market_id)

        return self.getRuleSetById(ruleSetId)

    def getRuleSetById(self, ruleSetId):

        request = {self.RULE_SET_ID_FIELD : ruleSetId}
        rulesSetFromDB = self.sendFindQuery(self.RULE_COLLECTION_NAME, request)

        import sys
        # print("size: "+ str(sys.getsizeof(rulesSetFromDB)))

        if rulesSetFromDB:
            return self._createRulesetFromDict(rulesSetFromDB)
        else:
            return None

    def _createRulesetFromDict(self, ruleSetFromDb):
        return RuleSet(ruleSetFromDb)

    def _getLatestRuleSetId(self, market_id= None):
        request = {}
        if market_id is not None:
            request[self.MARKET_ID_FIELD] = market_id

        oneRule = self.sendFindQuery(self.RULE_COLLECTION_NAME, request, sort=[('date', -1)], limit=1)[0]

        return oneRule[self.RULE_SET_ID_FIELD]



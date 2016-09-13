# -*- coding: utf-8 -*-

from datetime import datetime
from MongoDBConnector import MongoDBConnector


class BucketMongoDBConnector(MongoDBConnector):
    """
    Connects and executes queries on the Bucket mongo DataBase
    """

    def getEvents(self, eventType, dateMin, dateMax, sort=False, user_id= None, id_market= None, fields=None):
        """
        Returns records from table Events
        :param dateMin: must be of type datetime.datetime, filter by date
        :param dateMax: must be of type datetime.datetime, filter by date
        :param sort: default : False, sort by date (ascending)
        :return: returns a pymongo.cursor.Cursor , cursor on the query result. use method .next() to access events
        """

        id_market = str(id_market) # Dans la base, id_market est une str. On convertit donc id_market du type ObjectId au type str

        collectionName = "Event"

        # on vérifie la validité des dates
        self._controlDates(dateMin,dateMax)

        find_query = dict()

        if eventType != "all":
            find_query["bo"] = eventType

        find_query = self._addStartAndEndDatesToQuery(find_query, dateMin, dateMax)
        find_query = self._addUserIdToQuery(find_query, user_id)

        if not id_market is None:
            find_query["market_id"] = id_market

        conn= self.connection[collectionName]

        return self._getFindQueryResult(conn, find_query, fields) #todo: non teste
        #return self._getFindQueryResult(conn, find_query, fields, sort=[("inserted_at", -1)]) #todo: non teste



    def getOrderCreationDates(self, dateMin, dateMax, sort=False, id_market=None):

        return self._getObjectCreationDates("Order", dateMin, dateMax, sort=sort,
                                           id_market=id_market, dateFieldName= "created")

    def getUserCreationDates(self, dateMin, dateMax, sort=False, id_market=None):

        result, offset = self._getObjectCreationDates("User", dateMin, dateMax, sort=sort,
                                                      dateFieldName= "created", return_id= True)

        if offset != 0:
            raise BaseException("Erreur inconnue. L'offset est normalement de 0")

        assocUserMarket = self._getAssocUserMarket()

        if id_market != None:
            try:
                valid_users = list(assocUserMarket[id_market])
            except:
                return []
            valid_result = []
            for user in result:
                if user["_id"] in valid_users:
                    valid_result = valid_result + [user]
            result = valid_result

        return result

    def getOrderContentByUser(self, dateMax, id_market=None):

        # on vérifie la validité des dates
        #self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection voulue
        conn = self._getConnection("Order")

        # Composant de la future requete aggregate
        match = {"created": {"$lt": dateMax}}
        # Si une market spécifique est demandée, ou si l'on doit prendre toutes les markets possibles
        match = self._addMarketIdToQuery(match, id_market)

        # creation de la requete
        agg_query = [{"$match": match}]

        #if id_market != None:
        #    # on complete la requete déjà commencée
        #    agg_query = agg_query + [{"$match": {"market_id": id_market.__str__(),
        #                                         "created": {"$lt": dateMax}}}]
        #else:
        #    agg_query = [{"$match": {"created": {"$lt": dateMax}}}]

        agg_query = agg_query + [
            {"$project":
                {"nbaf":
                    {"$size": {"$ifNull": ["$acf",[]]}
                    },
                "nbf":
                    {"$size": {"$ifNull": ["$hof",[]]}
                    },
                "user_id":1
                }
            },
            {"$group":{
                "_id": "$user_id",
                "hof": {"$sum": "$nbfh"},
                "acf": {"$sum": "$nbfa"}
                }
            }
        ]

        return self._getAggQueryResult(conn,agg_query)

    def getItemViewsForAUser(self,dateMin, user_id, dateMax, id_market):

        # on vérifie la validité des dates
        self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection voulue
        conn = self._getConnection("Event")

        # creation de la requete
        find_query = {
            "object_type" : "item"
        }
        find_query = self._addStartAndEndDatesToQuery(find_query, dateMin, dateMax)
        find_query = self._addMarketIdToQuery(find_query, id_market)
        find_query = self._addUserIdToQuery(find_query, user_id)

        projection = {"object_id" : 1}

        return self._getFindQueryResult(conn, find_query, projection)

    def getLocationEvent(self,dateMin, dateMax, id_market= None):

        # on vérifie la validité des dates
        self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection voulue
        conn = self._getConnection("Event")

        # creation de la requete
        find_query = {
            "bo" : "location"
        }
        find_query = self._addStartAndEndDatesToQuery(find_query, dateMin, dateMax)
        find_query = self._addMarketIdToQuery(find_query, id_market)

        return self._getFindQueryResult(conn, find_query)

    def getOrderItemsByUser(self, dateMax, id_market=None):

        # on vérifie la validité des dates
        #self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection voulue
        conn = self._getConnection("Order")

        # Composant de la future requete aggregate
        match = {"created": {"$lt": dateMax}}
        # Si une market spécifique est demandée, ou si l'on doit prendre toutes les markets possibles
        match = self._addMarketIdToQuery(match, id_market)

        # creation de la requete
        agg_query = [{"$match": match}]

        agg_query = agg_query + [
            {"$project": {
                "user_id": "$user_id",
                "dd": {"$setUnion": ["$dfg","$dfh"]}}},
            {"$unwind": "$gg"},
            {"$project":{
                "user_id": "$user_id",
                "rtf_id": "$fgh.prod_id"
            }}
        ]

        return self._getAggQueryResult(conn,agg_query)

    def getNbEventsBySeconds(self, dateMin, dateMax, id_market= None, user_id= None):

        # on vérifie la validité des dates
        self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection voulue
        conn = self._getConnection("Event")

        match = {}
        match = self._addStartAndEndDatesToQuery(match, dateMin, dateMax, "inserted_at")

        group = {}
        group["_id"] = {
            'year':   {'$year'  : "$be_at"},
            'month':  {'$month' : "$be_at"},
            'day':    {'$dayOfMonth'    : "$be_at"},
            'hour':   {"$hour"  : "$be_at"},
            'minute': {"$minute": "$be_at"},
            'second': {"$second": "$be_at"}}
        group["cnt"] = {"$sum": 1}

        agg_query = [{"$match": match}, {"$group": group}]

        return self._getAggQueryResult(conn, agg_query)

    def getNumberOfViewsByItem(self, dateMin, dateMax, id_market= None, items= 'All'):
        # on vérifie la validité des dates
        self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection voulue
        conn = self._getConnection("Event")

        match = \
            {'$match':
                {'dd_at':
                   {'$gt': dateMin,
                    '$lt': dateMax
                    },
                't' :
                    'website',
                'ip':{'$not':{'$in':[]}},
                'type':
                    'view',
                'o_type':
                    'item'
                }
            }

        if id_market:
            match['$match']['dfg_id'] = id_market
        if not items=='All':
            match['$match']['dfg_id'] = {'$in': items}

        group = \
            {'$group':
                 {'_id': '$dfg_id',
                  'count':
                      {'$sum': 1}
                  }
            }

        agg_query = [match, group]

        return self._getAggQueryResult(conn, agg_query)

    def getOrderSizes(self, dateMin, dateMax, id_market= None):

        # on vérifie la validité des dates
        self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection des orders
        conn = self._getConnection("Order")

        # Composant de la future requete aggregate
        match = {"created": {"$lt": dateMax}}
        # Si une market spécifique est demandée, ou si l'on doit prendre toutes les markets possibles
        match = self._addMarketIdToQuery(match, id_market)

        # creation de la requete
        agg_query = [{"$match": match}]

        # creation de la requete
        agg_query = agg_query + [
            {"$project":
                {"dfg":
                    {"$size": {"$ifNull": ["$dfg",[]]}
                    },
                "dfh":
                    {"$size": {"$ifNull": ["$dfh",[]]}
                    },
                "created":1
                }
            }
        ]

        return self._getAggQueryResult(conn,agg_query)


    def getDistinctEventTypes(self):
        """
        :return: returns all distinct event types
        """

        collectionName = "errt"
        fieldName = "errt_type"

        return self.connection[collectionName].distinct(fieldName)

    def getMarketIdFromName(self, name):

        collectionName = "Market"

        try:
            id = self.connection[collectionName].find({"name": name})[0]['_id']
        except KeyError:
            id = None
        except IndexError:
            id = None

        return id

    def _addMarketIdToQuery(self, query, id_market, fieldName= "market_id"):

        if id_market:
            query[fieldName] = id_market.__str__()

        return query

    def _addUserIdToQuery(self, query, user_id, fieldName= "user_id"):

        if user_id:
            query[fieldName] = user_id

        return query


    def _getObjectCreationDates(self, collectionName, dateMin, dateMax, sort=False, id_market=None,
                                dateFieldName= "created", return_id= False):

        # on vérifie la validité des dates
        self._controlDates(dateMin,dateMax)

        # on établit une connection avec la collection voulue
        conn = self._getConnection(collectionName)

        # creation de la requete
        find_query = {}
        find_query = self._addStartAndEndDatesToQuery(find_query, dateMin, dateMax, dateFieldName)
        find_query = self._addMarketIdToQuery(find_query, id_market)

        offset_find_query = {dateFieldName: {"$lt": dateMin}}
        offset_find_query = self._addMarketIdToQuery(offset_find_query, id_market)
        offset = conn.find(offset_find_query).count()

        projection = {dateFieldName: 1}
        if not return_id:
            projection["_id"] = 0

        # Envoi de la requête
        cursor = conn.find(find_query, projection)
        if sort:
            srt = [(dateFieldName, -1)]
        else:
            srt = None


        return self._getFindQueryResult(conn, find_query, None, sort=srt), offset#todo: non teste


    def _getAssocUserMarket(self):
        logger = self._getAllDocs("Logger")
        assocUserMarket = dict()
        for el in logger:
            market = el["market"]._DBRef__id.__str__()
            user  = el["user"]._DBRef__id
            if market in assocUserMarket.keys():
                assocUserMarket[market].add(user)
            else:
                assocUserMarket[market] = set([user])
        return assocUserMarket


if __name__ == "__main__" :

    import datetime as dt
    dateMax = datetime.now()
    dateMin1 = datetime(2000,1,1)
    dateMin2 = datetime.now() - dt.timedelta(1)

    eventType = "view"
    sort = False
    return_id = False
    name = "reunion"

    self.getEvents( eventType, dateMin1, dateMax, sort, user_id, id_market)
    self.getOrderCreationDates( dateMin1, dateMax, sort, id_market)
    self.getUserCreationDates( dateMin1, dateMax, sort, id_market)
    self.getOrderContentByUser( dateMax, id_market)
    self.getItemViewsForAUser(dateMin1, user_id, dateMax, id_market)
    self.getLocationEvent(dateMin2, dateMax, id_market_GPS)
    self.getOrderItemsByUser( dateMax, id_market)
    self.getOrderSizes( dateMin1, dateMax, id_market)
    self.getDistinctEventTypes()
    self.getConnection()
    self.getMarketIdFromName( name)

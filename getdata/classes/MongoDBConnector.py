from pymongo import MongoClient as mc
from datetime import datetime

class MongoDBConnector:
    """
    Connects and executes queries on a mongo DataBase
    """

    def __init__(self, host, port, dbName):
        self.connection = mc(host, port).get_database(dbName)
        self.dbName = dbName

    def sendFindQuery(self, collectionName, query, projection= None, sort= None, limit= None):
        conn = self.connection[collectionName]
        return self._getFindQueryResult(conn,query, projection, sort, limit)

    def sendAggregationQuery(self, collectionName, query):
        conn = self.connection[collectionName]
        return self._getAggQueryResult(conn,query)

    def getConnection(self):
        """
        :return: returns a pymongo.database.Database , connection to the mongo database
        """
        return self.connection

    def _addStartAndEndDatesToQuery(self, query, start= None, end= None, fieldName= "date") :

        date_query = dict({})

        if start:
            date_query["$gt"] = start
        if end:
            date_query["$lte"] = end

        if date_query != dict({}):
            query[fieldName] = date_query

        return query

    def _getFindQueryResult(self,conn, find_query, projection= None, sort= None, limit= None):

        if projection is None:
            cursor = conn.find(find_query)
        else:
            cursor = conn.find(find_query,projection)

        if not sort is None:
            cursor = cursor.sort(sort)

        if not limit is None:
            cursor = cursor.limit(limit)
            cursor_size = min(limit,cursor.count())
        else:
            cursor_size = cursor.count()

        return [cursor.next() for i in range(cursor_size)]

    def _getAggQueryResult(self,conn,agg_query):
        # envoi de la requete
        cursor = conn.aggregate(agg_query)

        # deballage de la requete
        carryOn = True
        result = []
        while carryOn:
            try:
                result = result + [cursor.next()]
            except:
                carryOn = False

        return result

    def _controlDates(self,dateMin,dateMax):
        if type(dateMin) != datetime and not dateMin is None:
            raise BaseException("argument dateMin must be of type datetime.datetime")
        if type(dateMax) != datetime:
            raise BaseException("argument dateMax must be of type datetime.datetime")

    def _getConnection(self,collectionName):

        return self.connection[collectionName]

    def _getAllDocs(self,collectionName):
        conn = self._getConnection(collectionName)
        return self._getFindQueryResult(conn, None) # todo: non teste










# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from dateutil import rrule, parser
import datetime as dt
import re


class BucketStats :

    def __init__(self,DbConnector):
        self.DbConnector = DbConnector

    def getOrdersEvolution(self,id_market, dateMin,dateMax,granularity = "day",cumul = False,sort = False,
                              granularityOfFirstDateOfPeriod = "day", cumulIncludeBounds = False):

        result,offset = self._getOrdersEvolutionData(id_market, dateMin,dateMax,granularity = granularity,sort = sort,
                                             granularityOfFirstDateOfPeriod = granularityOfFirstDateOfPeriod )

        if cumul:
            try:
                result["cnt"] = result["cnt"].cumsum() + offset
            except KeyError:
                pass
            if cumulIncludeBounds:                                                                                      # TODO: inclure la meme option si cumul est FALSE
                first_value = offset
                try :
                    last_value  = result["cnt"].values[-1]
                except :
                    last_value  = offset
                first_date  = str(format(dateMin,"%Y-%m-%d"))                                                          # TODO: supprimer redondance
                last_date   = str(format(dateMax,"%Y-%m-%d"))
                to_append1  = pd.DataFrame([[first_value,first_date]],columns= ["cnt", "date"])
                to_append2  = pd.DataFrame([[last_value ,last_date ]],columns= ["cnt", "date"])
                if first_date not in result["date"]:
                    result      = to_append1.append( result )
                if last_date not in result["date"]:
                    result      = result.append( to_append2 )
                result.index = range(len(result.index))


        return result

    def getOrderSizes(self,id_market, dateMin,dateMax,granularity = "day",cumul = False,sort = False,
                              granularityOfFirstDateOfPeriod = "day", cumulIncludeBounds = False, bin = 4, percent= True):

        data = self.DbConnector.getOrderSizes(dateMin, dateMax, id_market= id_market)
        df = self._createDataFrame(data,["_id","created","activities","housings"])

        df["nb_dfh"] = df.activities + df.housings
        df["categ"] = [self._segment(i,bin) for i in df.nb_items]
        df["temp"] = [1]*len(df.index)
        colnames = list(df["categ"].unique())
        colnames = self._orderCategs(colnames)
        df = df.pivot_table( values = "temp",index = ["created","_id"] ,columns = "categ", aggfunc = np.sum, fill_value = 0)
        df.reset_index(level="created", inplace=True)
        df.index = range(len(df.index))

        df_before, df = self._segmentDataFrame(df,dateMin,"created")

        offset = df_before.sum(axis=0)

        if len(df):
            df = self._enrichData(df,"created")                                                                             # enrichissement de la donnee df avec les infos de date
            columnsForGroupBy = self._getAggregationColumns(granularity)

            result = df.groupby(columnsForGroupBy).sum()
            result = pd.DataFrame(result)

            result = self._getFirstDatesOfPeriods(result,dateMin,dateMax,granularity,
                                                  granularityOfFirstDateOfPeriod,dateColNameOUT="date")
            result.sort(["date"])
            result["date"] = [format(el,"%Y-%m-%d") for el in result["date"]]

            result = result[colnames + ["date"]]
            result.index = range(len(result.index))
        else:
            result = pd.DataFrame(None,columns=colnames+["date"])

        if cumul:
            try:
                for col in colnames:
                    result[col] = result[col].cumsum() + offset[col]
                    result[col] = [int(elem) for elem in result[col].values]
            except KeyError:
                pass
            if cumulIncludeBounds:                                                                                      # TODO: inclure la meme option si cumul est FALSE
                first_value = offset
                try :
                    last_value  = result.ix[len(result.index)-1,colnames]
                except :
                    last_value  = offset
                first_date  = str(format(dateMin,"%Y-%m-%d"))                                                          # TODO: supprimer redondance
                last_date   = str(format(dateMax,"%Y-%m-%d"))
                to_append1  = pd.DataFrame([list(first_value.values)+[first_date]],columns= list(first_value.index) + ["date"])
                to_append2  = pd.DataFrame([list( last_value.values)+[ last_date]],columns= list( last_value.index) + ["date"])
                if first_date not in result["date"]:
                    result      = to_append1.append( result )
                if last_date not in result["date"]:
                    result      = result.append( to_append2 )
                result.index = range(len(result.index))

        if percent:
            result.index = range(len(result.index))
            for ind in result.index:
                lineSum = result.ix[ind,colnames].sum().sum()
                if lineSum > 0:
                    result.ix[ind,colnames] = 100 * result.ix[ind,colnames] / lineSum
                else:
                    result.ix[ind,colnames] = 0

        return result[colnames+["date"]]

    def _orderCategs(self,colnames):
        df = pd.DataFrame(colnames,columns=["categ"])
        for i in df.index:
            try:
                df.ix[i,"from"] = int(re.findall("\A(\d+?)-\d+",df.ix[i,"categ"])[0])
            except IndexError:
                df.ix[i,"from"] = int(re.findall("\A(\d+?)",df.ix[i,"categ"])[0])
        df = df.sort("from")
        return list(df.categ)


    def _segment(self,i,bin):
        if i == 0:
            return "0"
        else:
            categ = int((i-1)/bin)
            mini = categ*bin +1
            maxi = (categ+1)*bin
        return str(mini) +"-"+str(maxi)


    def getMarketIdFromName(self,id):

        return self.DbConnector.getMarketIdFromName(id)

    def _getOrdersEvolutionData(self,id_market, dateMin,dateMax,granularity = "day",sort = False,
                              granularityOfFirstDateOfPeriod = "day"):

        data , offset = self.DbConnector.getOrderCreationDates(dateMin, dateMax, sort = sort, id_market = id_market)

        df = self._createDataFrame(data,["created"])

        if len(df):
            df = self._enrichData(df,"created")                                                                             # enrichissement de la donnee df avec les infos de date
            columnsForGroupBy = self._getAggregationColumns(granularity)

            result = df.groupby(columnsForGroupBy).count().ix[:,0]
            result = pd.DataFrame(result)
            result.columns = ["cnt"]

            result = self._getFirstDatesOfPeriods(result,dateMin,dateMax,granularity,
                                                          granularityOfFirstDateOfPeriod, dateColNameOUT="date")
            result.sort(["date"])
            result["date"] = [format(el,"%Y-%m-%d") for el in result["date"]]
        else:
            result = pd.DataFrame(None,columns=["cnt","date"])

        return result,offset

    def _createDataFrame(self,data,columns):
        df = pd.DataFrame(data)
        try :
            df.ix[0]
        except IndexError:
            df = pd.DataFrame(None,columns=columns)
        return df

    def _segmentDataFrame(self,df,date,colName):

        before = df[ df[colName] < date ].index
        after = set(df.index) - set(before)
        df1 = df.ix[before]
        df2 = df.ix[after]

        return df1, df2

    def getOrderItemsByUser(self, dateMax= dt.datetime.now(), id_market=None):

        data = self.DbConnector.getOrderItemsByUser(dateMax, id_market= id_market)
        df = pd.DataFrame(data, columns= ["user_id","_id","dfh_id"])
        df.columns = ["user_id","dfh_id","dfg_id"]

        return df

    def getEvents(self, dateMin, user_id= None, dateMax= dt.datetime.now(), id_market=None, eventType= "all"):

        data = self.DbConnector.getEvents(eventType, dateMin, dateMax, user_id= user_id, id_market= id_market, fields= ["be_at","bo"])
        df = pd.DataFrame(data, columns=["inserted_at","bo"] )
        print(df)
        df['sum'] = 1
        df = df.pivot_table(index= ["be_at"], columns= "bo", values="sum" ,aggfunc = np.sum, fill_value = 0)
        df.index = [int(d.timestamp()*1000) for d in df.index]

        return df

    def getLocationEvents(self, dateMin, dateMax= dt.datetime.now(), id_market=None):

        data = self.DbConnector.getLocationEvent(dateMin, dateMax, id_market= None)
        df = pd.DataFrame(data)

        return df

    def getItemViewsForAUser(self, dateMin, user_id,dateMax= dt.datetime.now(), id_market=None):

        data = self.DbConnector.getItemViewsForAUser(dateMin, user_id, dateMax, id_market)
        df = pd.DataFrame(data)
        try:
            df.columns = ["mgbd_objid","item_id"]
        except ValueError:
            return pd.DataFrame(None, columns=["mgbd_objid","item_id"])

        return df

    def getActiveUsers(self, dateMin, dateMax, granularity = "day", granularityOfFirstDateOfPeriod= "day",
                       id_market= None, cumul=True, cumulIncludeBounds=True):

        if id_market != None:
            id_market = id_market.__str__()

        userCreationDates = self.DbConnector.getUserCreationDates(dt.datetime(1,1,1),dateMax,sort=False,id_market=id_market) # dateMin doit être la plus petite date possible, car l'offset ne differencie pas les Users d'une market specifique. Ce filstrage se fait donc à la main, dans les lignes qui suivent
        userOrderContent = self.DbConnector.getOrderContentByUser(dateMax, id_market=id_market)

        # users ayant au moins un order non vide
        valid_users = []
        for user in userOrderContent:
            if user['activities'] > 0 or user['housings'] > 0:
                valid_users = valid_users + [user["_id"]]

        df = self._createDataFrame(userCreationDates,["_id","created"])

        df["_hasANonNullOrder"] = [1*(user_id.__str__() in valid_users) for user_id in df["_id"].values]

        df_before, df = self._segmentDataFrame(df,dateMin,"created")

        offset_users = len(df_before.index)
        offset_users_with_orders = np.sum(df_before["_hasANonNullOrder"])

        if len(df.index):
            df = self._enrichData(df,"created")
            columnsForGroupBy = self._getAggregationColumns(granularity)

            gb = df.groupby(columnsForGroupBy)
            result = gb.count().ix[:,0]
            result = pd.DataFrame(result)
            result.columns = ["user_cnt"]
            result["userWithNotNullOrder_cnt"] = gb["_hasANonNullOrder"].sum()

            result = self._getFirstDatesOfPeriods(result,dateMin,dateMax,granularity,
                                                        granularityOfFirstDateOfPeriod,dateColNameOUT = "date")
            result.sort(["date"])
            result["date"] = [format(el,"%Y-%m-%d") for el in result["date"]]
        else:
            result = pd.DataFrame(None,columns=["user_cnt","userWithNotNullOrder_cnt","date"])



        if cumul:                                                                                                       # todo: code vraiment factorisable
            try:
                result["userWithNotNullOrder_cnt"] = result["userWithNotNullOrder_cnt"].cumsum() + offset_users_with_orders
                result["user_cnt"]                    = result["user_cnt"                   ].cumsum() + offset_users
            except KeyError:
                pass
            if cumulIncludeBounds:                                                                                      # TODO: inclure la meme option si cumul est FALSE
                first_value_1 = offset_users
                first_value_2 = offset_users_with_orders
                try :
                    #first_value_1 = result["user_cnt"].values[0]
                    last_value_1  = result["user_cnt"].values[-1]
                    #first_value_2 = result["userWithNotNullOrder_cnt"].values[0]
                    last_value_2  = result["userWithNotNullOrder_cnt"].values[-1]
                except :
                    last_value_1  = offset_users
                    last_value_2  = offset_users_with_orders
                first_date  = str(format(dateMin,"%Y-%m-%d"))                                                          # TODO: supprimer redondance
                last_date   = str(format(dateMax,"%Y-%m-%d"))
                to_append1  = pd.DataFrame([[first_value_1,first_value_2,first_date]],columns= ["user_cnt", "userWithNotNullOrder_cnt", "date"])
                to_append2  = pd.DataFrame([[last_value_1, last_value_2 ,last_date ]],columns= ["user_cnt", "userWithNotNullOrder_cnt", "date"])
                if first_date not in result["date"]:
                    result      = to_append1.append( result )
                if last_date not in result["date"]:
                    result      = result.append( to_append2 )
                result.index = range(len(result.index))

        return result


    def _enrichData(self,df,columnName):

        df["_year"]       = None
        df["_month"]      = None
        df["_day"  ]      = None
        df["_dayOfWeek"]  = None
        df["_dayOfYear"]  = None
        df["_weekOfYear"] = None
        df["_hour"]       = None
        df["_minute"]     = None
        df["_second"]     = None
        df[["_year","_month","_day","_dayOfWeek","_dayOfYear","_weekOfYear","_hour","_minute","_second"]] = \
            [(el.year,el.month,el.day,el.dayofweek,el.dayofyear,el.weekofyear,el.hour,el.minute,el.second) \
             for el in df[columnName]]

        return df

    def _generateAllDates(self,dateMin,dateMax,granularity):
        if dateMin >= dateMax :
            raise BaseException("dateMin is greater than dateMax, or equal")

        if granularity == "year" :
            rule = rrule.YEARLY
            dateMin = dt.datetime(dateMin.year,1,1)
        elif granularity == "month":
            rule = rrule.MONTHLY
            dateMin = dt.datetime(dateMin.year,dateMin.month,1)
        elif granularity == "day"  :
            rule = rrule.DAILY
            dateMin = dt.datetime(dateMin.year,dateMin.month,dateMin.day)
        elif granularity == "hour" :
            rule = rrule.HOURLY
            dateMin = dt.datetime(dateMin.year,dateMin.month,dateMin.day,dateMin.hour)
        elif granularity == "minute"  :
            rule = rrule.MINUTELY
            dateMin = dt.datetime(dateMin.year,dateMin.month,dateMin.day,dateMin.hour,dateMin.minute)
        elif granularity == "second"  :
            rule = rrule.SECONDLY
            dateMin = dt.datetime(dateMin.year,dateMin.month,dateMin.day,dateMin.hour,dateMin.minute,dateMin.second)
        else :
            raise BaseException("granularity must be one of : year,month,day,hour,minute,second")

        dates = list(rrule.rrule(rule,
                         dtstart=dateMin,
                         until=dateMax))

        return dates

    def _getAggregationColumns(self,granularity):

        if granularity == "year":
            columnsForGroupBy = ["_year"]
        elif granularity == "month":
            columnsForGroupBy = ["_year","_month"]
        elif granularity == "day":
            columnsForGroupBy = ["_year","_month","_day"]
        elif granularity == "dayOfWeek":
            columnsForGroupBy = ["_year","_weekOfYear","_dayOfWeek"]
        elif granularity == "dayOfYear":
            columnsForGroupBy = ["_year","_dayOfYear"]
        elif granularity == "weekOfYear":
            columnsForGroupBy = ["_year","_weekOfYear"]
        elif granularity == "hour":
            columnsForGroupBy = ["_year","_month","_day","_hour"]
        elif granularity == "minute":
            columnsForGroupBy = ["_year","_month","_day","_hour","_minute"]
        elif granularity == "second":
            columnsForGroupBy = ["_year","_month","_day","_hour","_minute","_second"]
        else :
            raise BaseException("granularity must be one of : year,month,weekOfYear,day,dayOfWeek,dayOfYear,hour,minute,second")

        return columnsForGroupBy

    def _getFirstDatesOfPeriods(self,df,dateMin,dateMax,granularity, granularityOfFirstDateOfPeriod = "day",
                                dateColNameOUT= "date"):

        self._controlGranularities(granularity, granularityOfFirstDateOfPeriod)

        allDates = pd.DataFrame(
            self._generateAllDates(dateMin,dateMax,granularityOfFirstDateOfPeriod) ,
            columns = ["date"])
        allDates = self._enrichData(allDates,"date")

        columnsForGroupBy = self._getAggregationColumns(granularity)

        firstDates = allDates.groupby(columnsForGroupBy)["date"].min()

        firstDateOfPeriod = []
        validDfIndex = []
        for i in range(len(df.index)) :
            filteredFirstDates = firstDates.copy()
            try:
                for colname in columnsForGroupBy :
                    filteredFirstDates = filteredFirstDates[df.index.get_level_values(colname)[i]]
                validDfIndex = validDfIndex + [df.index[i]]
                firstDateOfPeriod = firstDateOfPeriod + [filteredFirstDates]
            except KeyError: # cette erreur intervien lorsque df a été mal préfiltré et contient des dates à l'extérieur de l'intervalle [dateMin, dateMax]
                pass

        #return firstDateOfPeriod
        df = df.ix[validDfIndex,:]
        df[dateColNameOUT] = firstDateOfPeriod
        return df

    def _controlGranularities(self,granularity, granularityOfFirstDateOfPeriod):

        granularityHierarchy = dict({
             "year"       : 1,
             "month"      : 2,
             "weekOfYear" : 3,
             "day"        : 4,
             "dayOfWeek"  : 4,
             "dayOfYear"  : 4,
             "hour"       : 5,
             "minute"     : 6,
             "second"     : 7
        })

        if granularityHierarchy[granularityOfFirstDateOfPeriod] < granularityHierarchy[granularity] :
            raise BaseException('Granularity of FirstDayOfPeriod must be lower than the data aggregation granularity')


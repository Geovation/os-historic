from db.Postgres import PgDatabaseManager, QueryResultType
from enum import Enum
from datetime import datetime

class DeltaStrategy(Enum):
    NEW = 1
    MODIFIED = 2
    DELETED =3

class SpatialDb(PgDatabaseManager):
    def __createSQLStatement(self,source_table,xmin, ymin,xmax, ymax, srid):
         envelope = "ST_MakeEnvelope({xMin},{yMin},{xMax},{yMax},{SRID})".format(xMin = xmin, yMin=ymin, xMax=xmax, yMax=ymax,SRID=srid)
         query =  "select * from {table_name} where st_contains({bbox}, {table_name}.geom)".format(table_name=source_table,bbox=envelope)
         return query

    def ClipByBBOX(self,source_table,xmin, ymin,xmax, ymax, srid):
        statement = self.__createSQLStatement(source_table,xmin, ymin,xmax, ymax, srid)
        results = self.ExecuteQuery(statement)
        return results

    def QueryAndFilterByRectangle(self,source_table,fields,attribute_query,xmin, ymin,xmax, ymax, srid):
       
        fieldsExpr = ",".join(fields)
        envelope = "ST_MakeEnvelope({xMin},{yMin},{xMax},{yMax},{SRID})".format(xMin = xmin, yMin=ymin, xMax=xmax, yMax=ymax,SRID=srid)
        query =  "select " + fieldsExpr + " from {table_name} where st_contains({bbox}, {table_name}.geom)".format(table_name=source_table,bbox=envelope)
        
        if attribute_query is not None:
            query += " and ({0})".format(attribute_query)

        results = self.ExecuteQuery(query,returnType=QueryResultType.DICTIONARY)

        return results
            

    def ClipByBBOXAsView(self,source_table,xmin, ymin,xmax, ymax, srid,viewName):
        statement = self.__createSQLStatement(source_table,xmin, ymin,xmax, ymax, srid)
        self.ExecuteQueryAsTableOrView(statement,viewName,'view')

    def ClipByBBOXAsTable(self,source_table,xmin, ymin,xmax, ymax, srid,viewName):
        statement = self.__createSQLStatement(source_table,xmin, ymin,xmax, ymax, srid)
        self.ExecuteQueryAsTableOrView(statement,viewName,'table')

    def FindDeltasFeauturesBasedOnKey(self,old_table,new_table,pkey_field,strategy):
        if strategy == DeltaStrategy.NEW:
           query =  "SELECT A.*, 'NEW'  AS 'CHANGE_TYPE' FROM {0} A LEFT JOIN {1}  B ON B.{2} = A.PK_ID WHERE B.{2} IS NULL".format(old_table,new_table,pkey_field)
        else: 
            pass

class OSMMTopoUtilites:
    def FindDataDeltas(previous_version, current_version, pkField, strategy):
        if type(previous_version) != list or type(current_version) != list:
            raise Exception("Input were not list")

        if strategy == DeltaStrategy.NEW:
            
            previous_version_set = set([p[pkField] for p in previous_version])
            current_version_set = set([c[pkField] for c in current_version])
            
            new_features = [x for x in current_version_set if x not in previous_version_set]
            
            return new_features
            





       
    
    
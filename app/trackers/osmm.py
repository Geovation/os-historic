import os
import json
from utils.utilities import Utilities
from db.Postgres import PgDatabaseManager, QueryResultType
from spatial.toolbox import SpatialDb, OSMMTopoUtilites, DeltaStrategy

class TopographyLayerTrackerDb:
    
    def __init__(self,dbConfiguration):
        self.config = dbConfiguration

        self.currentVersion = self.config["current_version_db"]
        self.historicVersions = self.config["previous_version_db"]
        self.settings = {
            "master_schema" : "osmm_topo",
            "master_table" : "topographicarea"
        }
        self.CompareField = None

    def GenerateLabelingTable(self):
        configCurrentDb = self.currentVersion
        currentVersionDB = PgDatabaseManager(configCurrentDb["host"],configCurrentDb["username"],configCurrentDb["password"],configCurrentDb["db_name"],configCurrentDb["port"])
        currentVersionDB.ExecuteQuery("CREATE TABLE IF NOT EXISTS public.{0} (toid character varying, version_first_appearance character varying)".format("version_feature_map"))
        currentVersionDB.ExecuteQuery("DELETE FROM public.{0}".format("version_feature_map"))
    
    def FilterDataByArea(self,table_name,compareKeyField,attributeFilter,extent,dbConnectionDetails):
          toolboxEntity = SpatialDb(dbConnectionDetails["host"],dbConnectionDetails["username"],dbConnectionDetails["password"],dbConnectionDetails["db_name"],dbConnectionDetails["port"])
          results= toolboxEntity.QueryAndFilterByRectangle(table_name, [compareKeyField], attributeFilter,extent[0],extent[1],extent[2],extent[3],27700)
          return results

    def SaveResultsAsViewOrTable(self,name,structure_type):
        currentVersionDB = PgDatabaseManager(self.currentVersion["host"],self.currentVersion["username"],self.currentVersion["password"],self.currentVersion["db_name"],self.currentVersion["port"])
        query = "select a.*, b.version_first_appearance from " + self.settings["master_schema"] + "." + self.settings["master_table"] + " a INNER JOIN public.version_feature_map b ON a." + self.CompareField + "= b." + self.CompareField
        currentVersionDB.ExecuteQueryAsTableOrView(query,name,structure_type)

    def StoreVersionsData(self,pkValueVersionList):
          currentVersion = self.currentVersion
          currentVersionDB = PgDatabaseManager(self.currentVersion["host"],self.currentVersion["username"],self.currentVersion["password"],self.currentVersion["db_name"],self.currentVersion["port"])
          insert_statement = "INSERT INTO version_feature_map ({1},version_first_appearance) VALUES {0}".format(",".join(pkValueVersionList),self.CompareField)
          currentVersionDB.ExecuteQuery(insert_statement)

    def SetVersionLabel(self,attributeQuery,compareKeyField,extent,savetoTable=True):
        labels = []

        if compareKeyField is None:
            raise Exception("Compare key field is not specified")

        self.CompareField = compareKeyField

        currentVersionDbConfig = self.currentVersion

        if savetoTable:
            self.GenerateLabelingTable()

        tableName = self.settings["master_schema"] + "." +  self.settings["master_table"]

        data_current = self.FilterDataByArea(tableName,compareKeyField,attributeQuery,extent,currentVersionDbConfig)
        
        version_processed = currentVersionDbConfig["version_name"]
        
        for i in range(len(self.historicVersions)):

            version = self.historicVersions[i]
            previous_version_features = self.FilterDataByArea(tableName,compareKeyField,attributeQuery,extent, version)

            new_features = OSMMTopoUtilites.FindDataDeltas(previous_version_features,data_current,compareKeyField,DeltaStrategy.NEW)
        
            bulk_insert_values = []
        
            if len(new_features) > 0: 
                for feature_compare_field_value in new_features:
                    label = {compareKeyField : feature_compare_field_value, "version_found": version_processed}
                    labels.append(label)
                    data_current.remove({compareKeyField : feature_compare_field_value})
                    if savetoTable:
                        bulk_insert_values.append("('{0}','{1}')".format(feature_compare_field_value, version_processed))

            version_processed = version["version_name"]
        
        if savetoTable:
            self.StoreVersionsData(bulk_insert_values)

        return labels
       
import os
import json
from utils.utilities import Utilities
from db.Postgres import PgDatabaseManager, QueryResultType
from spatial.toolbox import SpatialDb, OSMMTopoUtilites, DeltaStrategy
from trackers.osmm import TopographyLayerTrackerDb

def ImportDB(workspace):

        importScript = "/Users/amitrou/Documents/CodeProjects/historical_data/restore-db.sh"
        if not os.path.exists(importScript):
            raise Exception("Script doesn't exist")
            return

        settingsPath = os.path.join(workspace,"import_settings.json")
        if not os.path.exists(settingsPath):
            raise Exception("Configuration file doen't exist")
            return
            
        import_settings = Utilities.ReadJSON(settingsPath)

        if import_settings == None:
            raise Exception("Error")
      
        server = import_settings['host']
        port = import_settings['port']
        user = import_settings['user']
        dump_list = import_settings["dump_list"]

        if len(dump_list) == 0:
            raise Exception("No contents")

        for item in dump_list:
            dump_file = item["dupm_file"]
            db_name = item["db_name"]

            if (dump_file != None) and (db_name != None):
                Utilities.ExecuteShellScript(importScript,server,port,user,db_name,dump_file)

def DbConnect():
    pgManager = PgDatabaseManager("localhost","postgres","andreas","osmm_2018",7432)
    results = pgManager.ExecuteQuery("SELECT NOW()")
    print(results)

def ReadConfig(path):
    with open(path) as f:
        data = json.load(f)
    return data

def GenerateResultsView(featureName, configCurrentDb):
    currentVersionDB = PgDatabaseManager(configCurrentDb["host"],configCurrentDb["username"],configCurrentDb["password"],configCurrentDb["db_name"],configCurrentDb["port"])
    query = "select a.*, b.version_first_appearance from osmm_topo.clipped_topo_area a LEFT JOIN public.version_feature_map b ON a.toid = b.toid  where ARRAY_TO_STRING(a.descriptivegroup,',') like '%" + featureName +"%'"
    currentVersionDB.ExecuteQueryAsTableOrView(query,"V_VERSION_IDENTIFIED_" + featureName,"view")

if __name__ == "__main__":
    
    bbox = [531195.2968,170817.8032,536873.2005,180696.8954]
    config = ReadConfig(os.path.join(os.getcwd(),"app","db_compare_settings.json"))

    featureName = "Building"
    attributeQuery = "ARRAY_TO_STRING(descriptivegroup,',') like '%" + featureName + "%'"
    tracker = TopographyLayerTrackerDb(config)
    
    labels = tracker.SetVersionLabel(attributeQuery,"toid",bbox, True)
    tracker.SaveResultsAsViewOrTable("V_MyResults","view")
    #print(labels)

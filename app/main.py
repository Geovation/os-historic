import os
from utils.utilities import Utilities

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

ImportDB("/Users/amitrou/Documents/CodeProjects/historical_data/host_data")
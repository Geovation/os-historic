import subprocess
import os
import json

class Utilities:
    def ExecuteShellScript(scriptPath,*args):
        if scriptPath == None:
            raise Exception("Executable path was not specified")
        
        command = "sh {path}".format(path=scriptPath)

        if len(args) > 0:
            separator =  " "
            parameters = separator.join(args)
            command += " {params}".format(params=parameters)

        os.system(command)   
    
    def ReadJSON(file):
        if not os.path.exists(file):
            raise Exception("Could not find file {filename}".format(filename=file))

        with open(file) as json_file:
            data = json.load(json_file)

        return data


        
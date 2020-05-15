import psycopg2
import psycopg2.extras
from enum import Enum;

class QueryResultType:
    NORMAL = 0
    DICTIONARY = 1

class PgDatabaseManager:
    def __init__(self,server,user,password,database,port=5432):
        self.server = server
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.connection = None

    def __OpenConnection(self):
        self.connection = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.server, port=self.port)

    def __CloseConnection(self):
        if self.connection is not None:
            self.connection.close()

    def ExecuteQuery(self,queryText,returnType=QueryResultType.NORMAL):
        results=[]
        self.__OpenConnection()
        try:
            if returnType == QueryResultType.DICTIONARY:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            else:
                cursor = self.connection.cursor()
            
            cursor.execute(queryText)
            
            if str(queryText.upper()).startswith("CREATE".upper()):
                self.connection.commit()
            elif str(queryText.upper()).startswith("ALTER".upper()):
                self.connection.commit()
            elif str(queryText.upper()).startswith("DROP".upper()):
                self.connection.commit()                     
            elif str(queryText.upper()).startswith("DELETE".upper()):
                self.connection.commit()
            elif str(queryText.upper()).startswith("INSERT".upper()):
                self.connection.commit()
            elif str(queryText.upper()).startswith("SELECT".upper()):
                if returnType == QueryResultType.NORMAL:
                    results = cursor.fetchall()
                elif returnType == QueryResultType.DICTIONARY:
                    row = cursor.fetchone()
                    while row is not None:
                        results.append(dict(row))
                        row = cursor.fetchone()

            return results
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            self.__CloseConnection()

    def ExecuteQueryAsTableOrView(self,query,viewOrTableName,structureType):
        create_statement = ""
        if str(structureType).lower() == "view":
            create_statement = "CREATE OR REPLACE VIEW {0} AS".format(viewOrTableName)
        elif str(structureType).lower() == "table":
            create_statement = "CREATE TABLE IF NOT EXISTS {0} AS".format(viewOrTableName)
        
        sqlStatement = "{0} {1}".format(create_statement,query) 
        self.ExecuteQuery(sqlStatement)
        

    

    
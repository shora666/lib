import pyodbc
import pymssql

class DataDriver:

        def __init__(self , name):
                self.name = name

        def set_driver (self,driver):
                self.Driver=driver

        def set_server(self,server):
                self.Server = server
        
        def set_database(self, db):
                self.Database = db

        def set_uid (self, uid) :
                self.Uid = uid
    
        def set_passwd (self, passwd):
                self.Pwd = passwd

        def set_trusted_connection(self, trusted_connection):
                self.Trusted_Connection = trusted_connection
    
        def get_connection (self):
            #self.Driver = pyodbc.drivers()[2]
            #print("SELF NAME ==> {} DRIVER {}".format(self.name,self.Driver))
            if 'pymssql' not in self.name:
                print (self.Driver,self.Server,self.Database,self.Uid,self.Pwd)
                self.conn = pyodbc.connect("Driver="+self.Driver+";"
                      "Server="+self.Server+";"
                      "Database="+self.Database+";"
                      "UID="+self.Uid+";"
                      "PWD="+self.Pwd+";"
                      "Trusted_Connection="+self.Trusted_Connection+";")
            else:
                self.conn = pymssql.connect(self.Server, self.Uid, self.Pwd, self.Database,autocommit=False)
            return self.conn
    

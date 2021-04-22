from DataDriver import DataDriver
from EsmProperties import EsmProperties
import pandas as pd
import platform
import logging

class DataWriter:

        def __init__(self , name):
                self.name = name
                self.data_driver  = DataDriver(name)
                self.os = platform.system()
                if (self.os=='Windows'):
                    self.dbdriver="SQL Server"
                else:
                    self.dbdriver="/usr/lib64/libmsodbcsql-17.so"
                

        def init_connection (self,_prop):
                self.prop=EsmProperties("EsmProperties")
                if (self.os=='Windows'):
                    self.prop.set_properties_file("H:/conf/esmjsql.ini")
                else:
                    self.prop.set_properties_file("/usr/share/vidrio/conf/esmjsql.ini")
                dbserver=self.prop.get_property(_prop,"db.msserver")
                db=self.prop.get_property(_prop,"db.database")
                dbuser=self.prop.get_property(_prop,"db.user") 
                dbpasswd=self.prop.get_property(_prop,"db.passwd") 
                #dbport=self.prop.get_property(_prop,"db.msserver.port")
                self.data_driver.set_server(dbserver)
                self.data_driver.set_database(db)
                self.data_driver.set_uid(dbuser)
                self.data_driver.set_passwd(dbpasswd)
                self.data_driver.set_trusted_connection("no")
                self.data_driver.set_driver(self.dbdriver)
                self.conn=self.data_driver.get_connection()
                self.cursor = self.conn.cursor()

        def set_debug(self, debug):
            self.debug=debug

        def set_logger(self, logger):
            self.logger=logger

        def get_connection(self):
            return self.conn

        def exec_query (self,query):
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)

        def call_stored_proc (self,sp_name,sp_args):
            self.cursor.callproc(sp_name,sp_args)

        def exec_statement(self,stmnt):
            self.cursor.execute(stmnt)

        def commit(self):
            self.conn.commit()

        def rollback(self):
            self.cursor.rollback()

        #ex = sys.exc_info()
        def handle_error_message(self, ex):
            if(self.debug):
                print("Problem: " + ex[0])
                print("Reason: " + ex[1])
            raise Exception(
                "Exiting because: " +
                ex[0])


        def close(self):
            self.cursor.close()
            self.conn.close()
            

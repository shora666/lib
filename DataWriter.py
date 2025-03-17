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
                    self.prop.set_properties_file("G:\Shared drives\Common\Anatoliy\conf\esmjsql.ini")
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

        def init_connection_bypassing_prop (self,dbserver=None,db=None,dbuser=None,dbpasswd=None,dbdriver=None):
            self.dbdriver = dbdriver
            print("Data Writer ==> Driver: ",self.dbdriver)
            self.data_driver.set_server(dbserver)
            self.data_driver.set_database(db)
            self.data_driver.set_uid(dbuser)
            self.data_driver.set_passwd(dbpasswd)
            self.data_driver.set_trusted_connection("no")
            self.data_driver.set_driver(dbdriver)
            self.conn=self.data_driver.get_connection()
            self.cursor = self.conn.cursor()


        def set_debug(self, debug):
            self.debug=debug

        def set_logger(self, logger):
            self.logger=logger

        def set_connection (self,conn):
            self.conn = conn
            self.cursor = self.conn.cursor()

        def get_name(self):
            return self.name

        def get_connection(self):
            return self.conn

        def exec_query (self,query):
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)

        #pymssql
        def call_stored_proc(self,sp_name,sp_args):
            self.cursor = self.conn.cursor()
            return self.cursor.callproc(sp_name,sp_args)

        #pyodbc
        def exec_stored_proc (self,sp_name,sp_args):
            self.cursor = self.conn.cursor()
            self.cursor.execute(sp_name,sp_args)

        def exec_batch_statement(self, sp_name, sp_args):
            self.cursor.fast_executemany = True
            self.cursor.executemany(sp_name, sp_args)

        def get_result_set(self):
            # Get a cursor description which contains column names
            desc = self.cursor.description
            #print ("DataWriter: get_result_set ==> REC COUNT: ", self.cursor.rowcount)
            #Fetch all rowset from execute
            rowset=self.cursor.fetchall()
            #print ("DataWriter get_result_set ==> ROWSET: ", rowset)
            #print ("COLUMNS SIZE: ",len(desc))

            # Fetch all results from the cursor into a sequence,
            # display the values as column name=value pairs,
            # and then close the connection
            recs = []
            rcnt = 0
            for row in rowset:
                #print ("ROW: ",row)
                rec = {}
                for col in range(len(desc)):
                    rec[desc[col][0]] = row[col]
                    #print (col, "  " , desc[col][0], " ==> " ,row[col] )
                recs.append(rec)
            self.cursor.close()
            return recs


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
                "DataWriter ==> Exiting because: " +
                ex[0])

        def close_cursor(self):
            self.cursor.close()

        def close(self):
            self.conn.close()

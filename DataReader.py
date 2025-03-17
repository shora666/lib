
from DataDriver import DataDriver
from EsmProperties import EsmProperties
import pandas as pd
import platform

class DataReader:

        def __init__(self , name):
                self.name = name
                self.data_reader  = DataDriver(name)
                self.os = platform.system()
                if (self.os=='Windows'):
                    self.dbdriver="SQL Server"
                else:
                    self.dbdriver="/usr/lib64/libmsodbcsql-17.so"
                

        def init_connection (self,_prop):
                self.prop=EsmProperties("EsmProperties")
                if (self.os=='Windows'):
                    self.prop.set_properties_file("G:/Shared drives/Common/Anatoliy/conf/esmjsql.ini")
                else:
                    self.prop.set_properties_file("/usr/share/vidrio/conf/esmjsql.ini")
                #print("DataReader PROPERTIES:",self.prop)
                dbserver=self.prop.get_property(_prop,"db.msserver")
                db=self.prop.get_property(_prop,"db.database")
                dbuser=self.prop.get_property(_prop,"db.user") 
                dbpasswd=self.prop.get_property(_prop,"db.passwd") 
                print("DataReader.init_connection ==>DB SERVER: ",dbserver)
                #dbport=self.prop.get_property(_prop,"db.msserver.port")
                self.data_reader.set_server(dbserver)
                self.data_reader.set_database(db)
                self.data_reader.set_uid(dbuser)
                self.data_reader.set_passwd(dbpasswd)
                self.data_reader.set_trusted_connection("no")
                self.data_reader.set_driver(self.dbdriver)
                self.conn=self.data_reader.get_connection()

        def init_connection_bypassing_prop (self,dbserver=None,db=None,dbuser=None,dbpasswd=None,dbdriver=None):
                self.data_reader.set_server(dbserver)
                self.data_reader.set_database(db)
                self.data_reader.set_uid(dbuser)
                self.data_reader.set_passwd(dbpasswd)
                self.data_reader.set_trusted_connection("no")
                self.data_reader.set_driver(dbdriver)
                self.conn=self.data_reader.get_connection()

        def set_connection (self,conn):
            self.conn = conn

        def get_connection (self):
            return self.conn

        def exec_query (self,query):
            try:
                self.cursor = self.conn.cursor()
                self.cursor.execute(query)
            except Exception as e:
                print("ERROR executing query: ",query, " with exception: " ,str(e))

        #pyodbc
        def exec_stored_proc (self,sp_name,sp_args):
            self.cursor = self.conn.cursor()
            self.cursor.execute(sp_name,sp_args)

        def get_query_result_field (self,query,field):
            self.frame = pd.read_sql(query, self.conn)
            if self.frame.size == 0:
                return None
            else:
                return self.frame.loc[0,field]
        
        def get_query_result_frame (self,query):
            self.frame = pd.read_sql(query, self.conn)
            return self.frame

        def get_query_result_frame_chunks (self,query,chunksize):
            self.frame = None
            cnt = 1
            for chunk in  pd.read_sql(query, self.conn,chunksize=chunksize):
                print (f'Chunk # {cnt}')
                #print (chunk)
                if cnt==1:
                    self.frame = chunk
                else:
                    self.frame = self.frame.append(chunk)
                cnt = cnt + 1
            #print (self.frame)
            return self.frame

        def get_query_result_json (self,query):
            self.frame = pd.read_sql(query, self.conn)
            self.json = self.frame.to_json(orient="records")
            return self.json

        def get_result_first_row (self):
            for row in self.cursor:
                return row[0]

        def get_result_row (self):
            for row in self.cursor:
                return row


        def get_result_set(self):
            # Get a cursor description which contains column names
            desc = self.cursor.description
            #print ("COLUMNS SIZE: ",len(desc))

            # Fetch all results from the cursor into a sequence, 
            # display the values as column name=value pairs,
            # and then close the connection
            rowset = self.cursor.fetchall()
            recs = []
            rcnt = 0
            for row in rowset:
                rec = {}
                for col in range(len(desc)):
                    rec[desc[col][0]] = row[col]
                    #print (col, "  " , desc[col][0], " ==> " ,row[col] )
                recs.append(rec)
            return recs

        def close(self):
            self.conn.close()

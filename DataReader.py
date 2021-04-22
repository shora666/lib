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
                    self.prop.set_properties_file("H:/conf/esmjsql.ini")
                else:
                    self.prop.set_properties_file("/usr/share/vidrio/conf/esmjsql.ini")
                dbserver=self.prop.get_property(_prop,"db.msserver")
                db=self.prop.get_property(_prop,"db.database")
                dbuser=self.prop.get_property(_prop,"db.user") 
                dbpasswd=self.prop.get_property(_prop,"db.passwd") 
                #dbport=self.prop.get_property(_prop,"db.msserver.port")
                self.data_reader.set_server(dbserver)
                self.data_reader.set_database(db)
                self.data_reader.set_uid(dbuser)
                self.data_reader.set_passwd(dbpasswd)
                self.data_reader.set_trusted_connection("no")
                self.data_reader.set_driver(self.dbdriver)
                self.conn=self.data_reader.get_connection()

        def get_connection (self):
            return self.conn

        def exec_query (self,query):
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)

        def get_query_result_field (self,query,field):
            self.frame = pd.read_sql(query, self.conn)
            if self.frame.size == 0:
                return None
            else:
                return self.frame.loc[0,field]
        
        def get_query_result_frame (self,query):
            self.frame = pd.read_sql(query, self.conn)
            return self.frame

        def get_result_first_row (self):
            for row in self.cursor:
                return row[0]

        def get_result_set(self):
            # Get a cursor description which contains column names
            desc = self.cursor.description
            print ("COLUMNS SIZE: ",len(desc))

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
            self.cursor.close()
            self.conn.close()
            

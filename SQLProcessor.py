import os
import platform
from DataReader import DataReader
from DataWriter import DataWriter
import pandas as pd

class SQLProcessor:
    def __init__(self,driver="pymssql", db="ESMPROD",connection_string=None,uid=None, password=None):
        #Absolute file path
        self.driver=driver
        dirname = os.path.dirname
        self.connection_string = connection_string
        if uid is not None:  #not to store uid and passwd for cloud env vars -- on linux it is in ini config file
            self.uid = uid
        else:
            self.uid = 'esm_sa' 
        if password  is not None:
            self.password = password
        else:
            self.password = 'esm15vidrio'
        self.script_dir = dirname(dirname(__file__))
        self.data_reader = DataReader(driver)
        self.data_writer = DataWriter(driver)

        connection = db

        self.conn_dict = dict()
        self.process_conn_str()

        if len(self.conn_dict) == 0:
            print("CONN DB:",connection)
            self.data_reader.init_connection(connection)
            self.data_writer.init_connection(connection)
            print("READER CONNECTION: ",self.data_reader.get_connection(), " CONN DB:",connection)
        else:
            self.data_reader.init_connection_bypassing_prop(dbserver=self.conn_dict['Server'],db=self.conn_dict['Database'],dbuser=self.conn_dict['Uid'],dbpasswd=self.conn_dict['Password'],dbdriver=self.conn_dict['Driver'])
            self.data_writer.init_connection_bypassing_prop(dbserver=self.conn_dict['Server'],db=self.conn_dict['Database'],dbuser=self.conn_dict['Uid'],dbpasswd=self.conn_dict['Password'],dbdriver=self.conn_dict['Driver'])
        self.fields = {}
        self.filename = ""

    def get_processed_sql(self, fields=None, filename=None):
        if filename != None:
            self.filename=filename
        if fields != None:
            self.fields = fields
        rel_path = f'SQL/{self.filename}.sql'
        path = os.path.join(self.script_dir, rel_path).replace("\\","/")
        file = open(path)
        recording = False
        var = ""
        processed_sql = ""
        for line in file:
            #print("LINE: ",line.lstrip().rstrip())
            if not line.lstrip().rstrip().startswith('--'):
                for char in line:
                    if char == "{":
                        recording = True
                        continue
                    elif char == "}":
                        recording = False
                        processed_sql += str(self.fields[var])
                        var = ""
                        continue
                    if recording:
                        var+=char
                    else:
                        processed_sql+=char
        return processed_sql

    def execute_query(self,sql,result_count=None, write=False,out_file_name=None):
        if write:
            self.data_writer.exec_query(sql)
            self.data_writer.commit()
        else:
            self.data_reader.exec_query(sql)
            result_set = self.data_reader.get_result_set()
            if (result_count):
                if result_count == 1:
                    if len(result_set) > 0:
                        return result_set[0]
                    else:
                        return None
                result_set = result_set[0:result_count]
            if out_file_name is not None:
                df = pd.DataFrame(result_set)
                with pd.ExcelWriter(out_file_name, engine='xlsxwriter') as writer:
                    df.to_excel(writer,index=False,sheet_name="Data")
            return result_set
        return None

        
    def execute_sql(self, fields, filename, result_count=None, write=False,out_file_name=None):
        self.fields = fields
        self.filename = filename
        sql = self.get_processed_sql()
        result_set =  self.execute_query(sql=sql,result_count=result_count, write=write,out_file_name=out_file_name)
        return result_set

    # If you don't want to pass an argument, use an empty tuple
    def execute_sp(self,sp_name,sp_args,result_count=None, write=False,get_result=False,bulk=False,commit=True):
        res='OK'
        if self.driver=='pymssql':
            self.data_writer.call_stored_proc(sp_name,sp_args)
        elif self.driver=='pyodbc':
            if bulk:
                self.data_writer.exec_batch_statement(sp_name,sp_args)
            else:
                self.data_writer.exec_stored_proc(sp_name,sp_args)
        
        if get_result:
            res = self.data_writer.get_result_set()

        if commit:
            self.data_writer.commit()
        return res
        
    def get_connection(self):
        return self.data_writer.get_connection()


    def process_conn_str(self):
        if self.connection_string is not None: 
            l = self.connection_string.replace("{","").replace("}","").split(";")
            for e in l:
                #print (e.strip())
                if e is not None and len(e.strip()) > 0:
                    k= e.strip().split("=")[0]
                    v = e.strip().split("=")[1]
                    #print (v,type(v))
                    self.conn_dict[k]=v
            self.conn_dict['Uid'] = self.uid
            self.conn_dict['Password'] = self.password
        print("process_conn_str ==> CONN DICT: ",self.conn_dict)

if __name__ == "__main__":

    drvr = SQLProcessor(driver='pyodbc',db='ESMTEST') 

    #sql =   "declare @syid int \n " \
    #        "exec [dbo].[VIDRIO_BO$Insert_Global_Sy_Record] " \
    #        "@IN_SY_SYID = @syid  OUTPUT, @IN_TICKER=?, @IN_SYCODE_FINAL = ?, " \
    #        "@IN_SEC_TYPE=?, @IN_DATA_SOURCE = ?, @IN_IDENTIFIED_BY = ? \n" \
    #        " SELECT @syid"
    #params=('XBH3','XBH3 Comdty',None,'MANUAL','shora')
    #result = drvr.execute_sp(sql,params,get_result=True)
    #print (result)
    sql =   "DECLARE @o_entity_uid INT " \
            "DECLARE @o_deliverable_uid INT " \
            "DECLARE @o_proc_ident INT " \
            "DECLARE @proc_period VARCHAR(10) =  '02/28/2023' "\
            "EXEC [dbo].[ESM_AUTOMATION$Save_Vidrio_Deliverable]    @IN_DELIVERABLE_ID = NULL, " \
													   "@IN_ENTITY_NAME = ?," \
													   "@IN_DATA_TYPE = ?," \
													   "@IN_VIDRIO_DB = ?," \
													   "@IN_LEVEL = ?," \
													   "@IN_ASSIGNED_TO = 'DZ'," \
													   "@IN_DATA_ITEM_PROC_TYPE = ?," \
													   "@IN_VIDRIO_ENTITY_ID = '1111111|6304|403386'," \
													   "@OUT_DELIVERABLE_ID =@o_deliverable_uid OUTPUT," \
													   "@OUT_ENTITY_UID = @o_entity_uid OUTPUT"
    params=('RV Capital Asia Opportunity Fund','Fund','Vidrio','Fund','Securities Enrichment')
    result = drvr.execute_sp(sql,params,get_result=True)
    print (result)
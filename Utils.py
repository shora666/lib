#utilities package
import sys
import os
import pandas as pd
import datetime as dt
import zeep

sys.path.insert(1,'/usr/local/share/python/lib')
from DataDriver import DataDriver
from DataReader import DataReader
from DataWriter import DataWriter
from EsmProperties import EsmProperties
from Logger import Logger
from Mail import Mail

from datetime import datetime
import dateutil.relativedelta
from calendar import monthrange

debug=0

def ifnull(var, val):
    #if isinstance(var,float):
    #    print("Is ", var, "nan ? ",pd.isna(float(var)))

    if isinstance(var,float) and pd.isna(var):
        return val
    elif var is None:
        return val
    elif isinstance(var,str) and len(var)==0:
        return val
    else:
        return var

def isnull(var,val,val_ret):
    #if isinstance(var,float):
    #    print("Is ", var, "nan ? ",pd.isna(float(var)))

    if isinstance(var,float) and pd.isna(var):
        return val
    elif var is None:
        return val
    elif isinstance(var,str) and len(var)==0:
        return val
    else:
        return val_ret

def empty(value):
    try:
        value = float(value)
    except ValueError:
        pass
    return bool(value)

#class to convert excel files for import to xml files
#can be used for generic xl to xml conversion
def XL_to_XML(xml_root,xml_element,xl_file_name,xl_sheet_name,xml_file_name,xml_columns,seq,cnt):

    if seq == 1:
        xmlOut='<'+xml_root+'>\n'
    else:
        xmlOut=''

    #supports one to many
    def get_xml_field(xml_columns,xl_field):
        cols =[col['template_col_xml'] for col in xml_columns if col['template_col_xl']== xl_field]
        #if len(cols) == 1:
        #    return cols[0]
        #else:
        #   return None
        return cols


    def row_to_xml(row,xml_columns,xml_sheet_name):
        #print ('SHEET NAME: ' ,xml_sheet_name)
        xml = ['<' + xml_element + ' ']
        for i, col_name in enumerate(row.index):
            #print ("XL COL: " , col_name)
            #if col_name == 'Custody Trx Date':
            #    print ("XML COLS: " , xml_columns, col_name)
            xml_cols =get_xml_field(xml_columns,col_name) #xml column name
            for xml_col_name in xml_cols:
                if xml_col_name is not None:
                    #format date fields
                    val=ifnull(row.iloc[i],None)
                    if ('date' in xml_col_name or 'reference_day' in xml_col_name) and val is not None:
                        xml.append(' {0}="{1}" '.format(xml_col_name, 'T'.join(str(val).split())))
                    elif val is not None:
                        xml.append(' {0}="{1}" '.format(xml_col_name, row.iloc[i]))

        xml.append('/>')
        return ' '.join(xml)

    def to_xml(df,xml_columns):
        res='\n'.join(df.apply(row_to_xml, axis=1,args=(xml_columns,xl_sheet_name,)))
        return res

    df = pd.read_excel(xl_file_name,xl_sheet_name)
    index = df.index
    number_of_rows = len(index)
    if seq==cnt:
        xmlOut=xmlOut+to_xml(df,xml_columns)+'\n</'+xml_root+'>'
    else:
        xmlOut=xmlOut+to_xml(df,xml_columns)+'\n'

    #print(xmlOut)
    print("XML FILE TO OPEN: " +xml_file_name)
    
    #write to the file only if number od output rows > 0
    if number_of_rows > 0:
        with open(xml_file_name, "a+") as f:
            f.write(xmlOut)

def importK2Data(_prop,access_key,xml_file,logger,email_to,email_cc,email_subject,data_reader,data_writer,client):
    print("===========DATA IMPORT===============")
    imported = 0
    access_key=access_key.split('"')
    logger.debug("Access key===> "+access_key[1])


    XML_DATA = open(xml_file,"r").read()

    logger.debug(XML_DATA)
    logger.debug(client.service.GetXmlSchema(access_key[1]))
    imp_status=importStatus(xml_file,data_reader)
    if 'MISSING' in imp_status:
        imp_status = client.service.ImportData(access_key[1],XML_DATA)
        #imp_status='OK'
        if "OK" in imp_status:
            logger.debug ("IMPORT STATUS: "+imp_status)
            push_status = client.service.PushData(access_key[1])
            #push_status = 'OK'
            if "OK" in push_status:
                logger.debug("PUSH STATUS: " +push_status)
                importDataLog(xml_file,access_key[1],push_status,data_writer,logger)
                imported = 1
            else:
                logger.error ("PUSH STATUS: " +push_status)
                importDataLog(xml_file,access_key[1],"PUSH ERROR",data_writer,logger)
                m=Mail("Mail",'anatoliy.shor@vidrio.com',email_to,email_cc,email_subject)
                m.set_body(xml_file+'\n PUSH STATUS: '+push_status)
                m.send_text_email()
                sys.exit();
        else:
            logger.error ("IMPORT STATUS: " +imp_status)
            m=Mail("Mail",'anatoliy.shor@vidrio.com',email_to,email_cc, email_subject)
            m.set_body(xml_file+'\n IMPORT STATUS: '+imp_status)
            m.send_text_email()
            sys.exit();

    return imported

def lockProcAutoQ(module,src,data_writer,logger):
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_PROCESS$Add_To_Proc_Auto_Q]',(module,src,1,))
        data_writer.commit()
        if debug ==1:
            logger.debug("MODULE : " +module + " SRC: "+ src + " LOCK STSTUS: " + str(1))
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()

def unlockProcAutoQ(module,src,data_writer,logger):
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_PROCESS$Add_To_Proc_Auto_Q]',(module,src,0,))
        data_writer.commit()
        if debug ==1:
            logger.debug("MODULE : " +module + " SRC: "+ src + " LOCK STSTUS: " + str(0))
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()

def importDataLog(fName,accessKey,status,data_writer,logger):
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_IMPORT$Update_Import_Log]',(fName,accessKey,status,))
        data_writer.commit()
        logger.debug("IMPORT fName: " +fName + " IMPORT ACCESS Key: "+str(accessKey))
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()

def updateAppProcStatus(module,source,asof,step,stime_ind,etime_ind,status,data_writer,logger):
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_PROCESS$Save_Proc_Progress_Status]',(module,source,asof,step,stime_ind,etime_ind,status))
        data_writer.commit()
        if status is not None:
            logger.debug("APP PROC STATUS UPDATE module: " +module + " SOURCE: " + source + "  STEP: " + step + " STATUS: " + status  + " AS OF : "+asof)
        else:
            logger.debug("APP PROC STATUS UPDATE module: " +module + " SOURCE: " + source + "  STEP: " + step +  " AS OF : "+asof)
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()

def updateAppProcError(module,source,asof,step,err_msg,data_writer,logger):
    try:
        if err_msg is not None:
            data_writer.call_stored_proc('[dbo].[VIDRIO_PROCESS$Save_Proc_Progress_Error]',(module,source,asof,step,err_msg))
            data_writer.commit()
            logger.debug("APP PROC ERROR module: " +module + " SOURCE: " + source + "  STEP: " + step + " STATUS: ERROR "  + " ERROR MESSAGE: " + err_msg + " AS OF : "+asof)
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()

def autoProcLockStatus(module, src,data_reader):
    status = data_reader.get_query_result_field(f"select lock_status as st from PROC_AUTO_Q where module = '{module}' and src = '{src}'","st")
    return status

def importStatus(fName,data_reader):
    status = data_reader.get_query_result_field(f"select  [dbo].[VIDRIO_IMPORT$Get_Imp_Status]('{fName}') st","st")
    return status

def importStatusFinal(fName,data_reader):
    status = data_reader.get_query_result_field(f"select  [dbo].[VIDRIO_IMPORT$Get_Final_Imp_Status]('{fName}') st","st")
    return status


def procRunStatus(module,source,asof,data_reader):

    query = f"select * from [dbo].[VIDRIO_PROCESS$Get_Proc_Run_Status]('{module}','{source}','{asof}')"
    data_reader.exec_query(query)
    res=data_reader.get_result_row() # (status,step,err_msg)
    return res

def last_day_of_month(date_value):
    return date_value.replace(day = monthrange(date_value.year, date_value.month)[1])

def date_n_months_ago(d,mago):
    mb=mago*-1
    t=datetime.strptime(d, '%Y-%m-%d')
    nd=datetime.strftime(last_day_of_month(t+dateutil.relativedelta.relativedelta(months=mb)),'%Y-%m-%d')
    return nd

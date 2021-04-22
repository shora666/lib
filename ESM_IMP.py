import sys
import pandas as pd
import datetime as dt
import os
import platform
import math
import zeep
import shutil
from DataReader import DataReader
from Utils import *
from Mail import Mail

def ASI_GRM_XL_TO_IMP_XML(tstamp,OUTPUT_DIR,XML_DIR,source,strategies,grms):
    data_reader=DataReader("DrataReader")
    data_reader.init_connection("ESMPROD")

    for g in grms:
        query="select template_name,template_sheet, xl_sheet, "\
                "replace(replace(xl_output_file, '__cur_date__','_"+tstamp+"'),'__GRM____strategy__','"+g+"_Strategies_') xl_output_file,  "\
                "replace(replace(xml_output_file,'__cur_date__','_"+tstamp+"'),'__GRM____strategy__','"+g+"_Strategies_') xml_output_file, seq, "\
                "max(seq) over (partition by template_name order by template_name) cnt "\
                "from IMP_FILES_DEF where src = 'ASI_GRM' order by seq"
        data_reader.exec_query(query)
        files =data_reader.get_result_set()
        files_to_attach =[]
        for row in files:
            #print (row['template_name'],"|||",row['template_sheet'],"|||", row['xl_sheet'],"|||",row['xl_output_file'],"|||",row['xml_output_file'])
            xml_root =row['template_name']
            xml_element =row['template_sheet']
            xl_output_file =row['xl_output_file']
            xml_output_file =row['xml_output_file']
            xl_sheet_name=row['xl_sheet']
            seq=row['seq']
            cnt=row['cnt']
            xl_file_name = f"{OUTPUT_DIR}/{xl_output_file}"
            xml_file_name = f"{XML_DIR}/{xml_output_file}"
            print("XML FILE GEN REC: " + xml_root,"|||",xml_element,"|||",xl_file_name, "|||", xl_sheet_name,"|||",xml_file_name)
            if os.access(xl_file_name, os.R_OK):
                data_reader_in=DataReader("DrataReader")
                data_reader_in.init_connection("ESMPROD")
                query="select template_col_xl,template_col_xml " \
                      "from IMP_TEMPLATES_DEF where template_name = '"+xml_root+"' and template_sheet = '"+xml_element+"'"
                data_reader_in.exec_query(query)
                columns=data_reader_in.get_result_set()
                #print("IMP COLUMNS: ", columns)
                data_reader_in.close()
                XL_to_XML(xml_root,xml_element,xl_file_name,xl_sheet_name,xml_file_name,columns,seq,cnt)
                files_to_attach.append(xl_file_name)
                #if seq == 1 and os.path.exists(xml_file_name):
                #    os.remove(xml_file_name)

def K2_XL_TO_IMP_XML(as_of_date,cur_date,OUTPUT_DIR,XML_DIR,SRC,send_email,sent_invest_trx):
    data_reader=DataReader("DrataReader")
    data_reader.init_connection("ESMPROD")
    
    query="select template_name,template_sheet, xl_sheet, " \
        "replace(replace(xl_output_file,'__as_of_date__','"+as_of_date+"'),'__cur_date__','"+cur_date+"') xl_output_file,  "\
        "replace(replace(xml_output_file,'__as_of_date__','"+as_of_date+"'),'__cur_date__','"+cur_date+"') xml_output_file, seq, "\
        "max(seq) over (partition by template_name order by template_name) cnt "\
        "from IMP_FILES_DEF where src = 'K2 "+SRC+"' order by seq"
    data_reader.exec_query(query)
    files =data_reader.get_result_set()
    files_to_attach =[]
    for row in files:
        #print (row['template_name'],"|||",row['template_sheet'],"|||", row['xl_sheet'],"|||",row['xl_output_file'],"|||",row['xml_output_file'])
        xml_root =row['template_name']
        xml_element =row['template_sheet']
        xl_output_file =row['xl_output_file']
        xml_output_file =row['xml_output_file']
        xl_sheet_name=row['xl_sheet']
        seq=row['seq']
        cnt=row['cnt']
        xl_file_name = f"{OUTPUT_DIR}/{xl_output_file}"
        xml_file_name = f"{XML_DIR}/{xml_output_file}"
        if seq == 1 and os.path.exists(xml_file_name):
            os.remove(xml_file_name)
        print("XML FILE GEN REC: " + xml_root,"|||",xml_element,"|||",xl_file_name, "|||", xl_sheet_name,"|||",xml_file_name)
        if os.access(xl_file_name, os.R_OK):
            data_reader_in=DataReader("DrataReader")
            data_reader_in.init_connection("ESMPROD")
            query="select template_col_xl,template_col_xml " \
                "from IMP_TEMPLATES_DEF where template_name = '"+xml_root+"' and template_sheet = '"+xml_element+"'"
            data_reader_in.exec_query(query)
            columns=data_reader_in.get_result_set()
            #print("IMP COLUMNS: ", columns)
            data_reader_in.close()
            XL_to_XML(xml_root,xml_element,xl_file_name,xl_sheet_name,xml_file_name,columns,seq,cnt)
            if 'Investments - Transactions' in xl_file_name and 'UCITS' not in xl_file_name and sent_invest_trx == 1:
                #basename=os.path.basename(xl_file_name)
                files_to_attach.append(xl_file_name)

    if len(files_to_attach) > 0 and send_email == 1:
        subject=f"Investment Transactions Files to be reviewed for {as_of_date}"
        #m=Mail("Mail",'anatoliy.shor@vidrio.com','support@vidrio.com','anatoliy.shor@vidrio.com',subject)
        m=Mail("Mail",'anatoliy.shor@vidrio.com','support@vidrio.com','anatoliy.shor@vidrio.com,travis.cooper@vidrio.com,edward.chitonho@vidrio.com',subject)
        body=f"Investment Transaction Files to be imported manually for {as_of_date}"
        m.set_body(body)
        m.send_attach_email(files_to_attach)

def merge_two_trx_files(file1,file2,file_result):
    #file_result = file1.split()
    #del file_result[1:3]
    #file_result = ' '.join(str(e) for e in file_result)
    print("COMBINED TRANSACTIONS FILE: ",file_result)
    lines = []
    with open(file1) as fp:
        cnt = 1
        line = fp.readline()
        print("{}: {}".format(cnt,line.strip()))
        lines.append(line)
        while line:
            line = fp.readline()
            if '</VALUATION-TRANSACTIONS>' not in line:
                print("{}: {}".format(cnt,line.strip()))
                lines.append(line)
            cnt += 1
    fp.close()

    with open(file2) as fp:
        line = fp.readline() #header
        cnt = 1
        while line:
            line = fp.readline()
            print("{}: {}".format(cnt,line.strip()))
            lines.append(line)
            cnt += 1
    fp.close()

    with open(f"{file_result}", 'w') as fp:
        fp.writelines(lines)
    fp.close()

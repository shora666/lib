import sys
import os

def normalizeR2(R2):
    if R2>1:
        return 1
    elif R2<0:
        return 0
    return R2
##########################
#DB readers
##########################

def getRequestUid(req_type,step,data_reader):
    request_uid = None
    sql = f"select count(*) cnt from axioma_running_q where request_type = '{req_type}' and run_status = 0 and step='{step}'"
    run_status = data_reader.get_query_result_field(sql,'cnt')
    print("RUN STATUS: ", run_status)

    if run_status > 0:
        sql = f"select top(1) Vidrio_Request_Uid from axioma_running_q where request_type = '{req_type}' and run_status = 0 and step='{step}'"
        request_uid = str(data_reader.get_query_result_field(sql,'Vidrio_Request_Uid')).upper()
        print("REQUEST UID: ", request_uid)
    return request_uid

def getRunningReqCount(req_type,data_reader):
    sql = f"select count(*) cnt from axioma_running_q where request_type = '{req_type}'"
    req_cnt = data_reader.get_query_result_field(sql,'cnt')
    return req_cnt

def getProcRunStatus(req_uid,data_reader):
    sql = f"select count(*) cnt  from axioma_running_q where Vidrio_Request_Uid = '{req_uid}'"
    run_status = data_reader.get_query_result_field(sql,'cnt')
    if run_status > 0:
        sql = f"select [run_status] from axioma_running_q where Vidrio_Request_Uid = '{req_uid}'"
        run_status = data_reader.get_query_result_field(sql,'run_status')
    return run_status

def getProcRunStatusForProcStep(step,data_reader):
    sql = f"select count(*) cnt  from axioma_running_q where step = '{step}' and run_status = 1"
    run_status = data_reader.get_query_result_field(sql,'cnt')
    return run_status

def getProcRunCountForProcStep(step,data_reader):
    sql = f"select count(*) cnt  from axioma_running_q where step = '{step}'"
    run_status = data_reader.get_query_result_field(sql,'cnt')
    return run_status

#def autoProcLockStatus(module, src,data_reader):
#    status = data_reader.get_query_result_field(f"select lock_status as st from PROC_AUTO_Q where module = '{module}' and src = '{src}'","st")
#    return status

#def importStatus(fName,data_reader):
    #status = data_reader.get_query_result_field(f"select  [dbo].[VIDRIO_IMPORT$Get_Imp_Status]('{fName}') st","st")
    #return status

#def procRunStatus(module,source,asof,data_reader):
#    query = f"select * from [dbo].[VIDRIO_PROCESS$Get_Proc_Run_Status]('{module}','{source}','{asof}')"
#    data_reader.exec_query(query)
#    res=data_reader.get_result_row() # (status,step,err_msg)
#    return res

def getReqDetailAttr(req_det_uid,data_reader):
    sql = "select vd.[Risk_Mode], vr.Requester_Id "\
        " from [VEDI].[dbo].[Vidrio_Request_Detail]  vd "\
        " inner join [VEDI].[dbo].[Vidrio_Request]  vr on vr.Vidrio_Request_Uid = vd.Vidrio_Request_Uid "\
        " where vd.Vidrio_Request_Detail_Uid =  '"+req_det_uid+"'"
    data_reader.exec_query(sql)
    attr=data_reader.get_result_set()
    return attr

def get_portfolio_positions(req_uid,req_det_uid,data_reader):
    sql = f"select * from  [dbo].[VIDRIO_VEDI$Get_PT_Allocations] ('{req_uid}') where Vidrio_Request_Detail_Uid = '{req_det_uid}'"
    data_reader.exec_query(sql)
    portfolio_positions=data_reader.get_result_set()
    return portfolio_positions

def getJobPeriod(job_id,data_reader):
    sql = "select [dbo].[VIDRIO_UTILS$Sql_Date_To_Period]([Axioma_Analysis_Date],'M') [Period] "\
          " from [dbo].[RISK_CALCULATIONS_LOG] "\
          " where [Axioma_Job_Id] = '"+job_id+"'"
    period=data_reader.get_query_result_field(sql,'Period')
    return period

def are_there_calc_incompleted(req_uid,data_reader):
    sql = " select count(*) cnt "\
          " FROM [ESM_TS].[dbo].[RISK_CALCULATIONS_LOG] "\
          " where [Vidrio_Request_Uid] = '"+str(req_uid)+"' "\
          " and [Axioma_Job_Perc_Completed] <> 100 "
    incomplete_count =data_reader.get_query_result_field(sql,'cnt')
    return incomplete_count

def getFactorDecompDataFromDb(req_det_uid,period,data_reader):
    sql = f"select * from [dbo].[VIDRIO_RISK$Get_Risk_Factors_TS_Request_Output]  ('{req_det_uid}') where [PERIOD] ='{period}'"
    data_reader.exec_query(sql)
    data=data_reader.get_result_set()
    return data

def getFactorDecompRecordsFromDb(req_det_uid,data_reader):
    sql = " select distinct  entity_uid, requester_id , period, [Platform_Url],[Risk_Mode] "\
          " from [dbo].[VIDRIO_RISK$Get_Risk_Factors_TS_Request_Output]  ('"+str(req_det_uid)+"') rf "\
          " inner join [VEDI].[dbo].[ESM_Requesters_To_Process] rp on rp.Vidrio_Requester_Id = rf.requester_id"
    data_reader.exec_query(sql)
    records=data_reader.get_result_set()
    return records



def get_risk_templates(process_type,data_reader):
    sql = "SELECT [TEMPLATE_ID], rt.TEMPLATE_NAME , replace(rt.TEMPLATE_NAME,'Multi','') TEMPLATE_NAME_TS " \
            "FROM [ESM_TS].[dbo].[ESM_RISK_TEMPLATES] ert " \
            "inner join RISK_TEMPLATES rt  on rt.IDENT = ert.TEMPLATE_ID " \
            "inner join ESM_PROCESS_TYPE ept on ept.IDENT = ert.PROC_TYPE_ID " \
            "and ept.PROCESS_TYPE = '"+process_type+"' " \
            "where  ert.active  = 1 " \
            "order by TEMPLATE_ID"
    data_reader.exec_query(sql)
    risk_templates=data_reader.get_result_set()
    return risk_templates

def get_request_details(request_uid, data_reader):
    sql = " SELECT vd.*, [dbo].[VIDRIO_UTILS$Vidrio_Period_To_Serial_Date] ([Starting_period]) [sasof], "\
          " [dbo].[VIDRIO_UTILS$Vidrio_Period_To_Serial_Date] ([Ending_Period]) [easof], "\
          " (select max ([dbo].[VIDRIO_UTILS$Vidrio_Period_To_Serial_Date](Period)) as_of " \
          "    from [dbo].[VW_VIDRIO_TIME_SERIES_DATA] " \
          "    where Vidrio_Request_Detail_Uid = vd.Vidrio_Request_Detail_Uid " \
          " ) last_data_updated " \
          " from [dbo].[VIDRIO_VEDI$Get_Request_Details]('" + str(request_uid) + "',1000) vd"
    data_reader.exec_query(sql)
    req_details=data_reader.get_result_set()
    return req_details

def get_request_details_raw(request_uid,data_reader):
    sql = "select Vidrio_Request_Detail_Uid "\
          "from [VEDI].[dbo].Vidrio_Request_Detail "\
          "where vidrio_request_uid = '"+request_uid+"'"
    data_reader.exec_query(sql)
    req_details=data_reader.get_result_set()
    return req_details

def get_req_det_periods(start_asof,end_asof,data_reader,logger):
    psql = "select * from  [dbo].[VIDRIO_UTILS$Get_Month_Formatted_Rows] (120,"+str(start_asof)+","+str(end_asof)+")"
    data_reader.exec_query(psql)
    req_det_periods=data_reader.get_result_set()
    logger.debug("REQ DET PERIODS: " + str(req_det_periods)) 
    return req_det_periods


def get_time_series (req_det_uid,subitem_id, analysisDate, data_reader) :
    #ts_sql = "select * from [dbo].[VIDRIO_VEDI$Get_Time_Series_Info]('"+str(req_uid)+"',1000000000)"
    ts_sql = "SELECT top(84) vrd.Vidrio_Request_Uid, Vidrio_Ts_Data_Uid, vts.Vidrio_Request_Detail_Uid,SubItem_Id , "\
        "                                   Vidrio_Dictionary_Uid, Name, Column_Type, Period , eomonth([Month_Date]) [Month_Date],Value,  "\
        "                                   [dbo].[VIDRIO_UTILS$Vidrio_Period_To_Serial_Date](Period) as_of, Is_Backfilled "\
        "                                   from [dbo].[VW_VIDRIO_TIME_SERIES_DATA]   vts, [dbo].[VW_VIDRIO_REQUEST_DETAIL] vrd, [dbo].[VW_VIDRIO_REQUEST] vr "\
        "                                   where  Column_Type  = 'T'  "\
        "                                   and vr.Is_Ready_For_Processing = 1 and vr.Vidrio_Request_Uid = vrd.Vidrio_Request_Uid "\
        "                                   and vrd.Vidrio_Request_Detail_Uid  =  vts.Vidrio_Request_Detail_Uid "\
        "                           and vrd.Vidrio_Request_Detail_Uid = '"+ str(req_det_uid) +"' "\
        "                           and vts.Subitem_id = '"+subitem_id+"' "\
        "                           and vts.Month_Date <= '"+ analysisDate +"' "\
        "                           and vrd.VIDRIO_Process_Error_Detected = 0 and vrd.ESM_Process_Error_Detected = 0 and vrd.VIDRIO_Process_Error_Detected = 0 "\
        "                           and not exists (select * from [dbo].[VW_ESM_REQUESTERS_TO_PROCESS] where Vidrio_Requester_Id = vr.Requester_Id and Skip_It = 1)"\
        "                           order by Month_Date desc" 
        #"                          order by Vidrio_Ts_Data_Uid desc" 
    data_reader.exec_query(ts_sql)
    req_ts=data_reader.get_result_set()
    return req_ts

def get_Request_Details_To_Get_Output(request_uid,data_reader):
    #can g without count and group by, for additional info only just in case
    sql = "  select [Vidrio_Request_Uid], [Vidrio_Request_Detail_Uid],[Axioma_Template],[Axioma_Analysis_Date], "\
          " [Axioma_Job_Perc_Completed],[Axioma_Job_id] "\
          "  from [ESM_TS].[dbo].[RISK_CALCULATIONS_LOG] "\
          "  where [ESM_Init_End_Time] is not null and [ESM_Calc_Start_Time] is not null "\
          "  and (Axioma_Job_Perc_Completed is null or Axioma_Job_Perc_Completed < 100) "\
          "  and Esm_Current_Step = 'calc' and  [Vidrio_Request_Uid] = '"+request_uid+"'" 
    data_reader.exec_query(sql)
    req_details=data_reader.get_result_set()
    return req_details


def get_Request_Details_To_Calculate(request_uid,data_reader):
    #can g without count and group by, for additional info only just in case
    sql = "  select [Vidrio_Request_Uid], [Vidrio_Request_Detail_Uid],[Axioma_Template],[Axioma_Analysis_Date] "\
          "  from [ESM_TS].[dbo].[RISK_CALCULATIONS_LOG] "\
          "  where [ESM_Init_End_Time] is not null and [ESM_Calc_Start_Time] is null "\
          "  and  [Vidrio_Request_Uid] = '"+request_uid+"' and  [Vidrio_Request_Detail_Uid] is not NULL" 
    data_reader.exec_query(sql)
    req_details=data_reader.get_result_set()
    return req_details

def get_Request_Details_To_Process_Output(request_uid,data_reader):
    #can g without count and group by, for additional info only just in case
    sql = f"  select [Vidrio_Request_Uid], [Vidrio_Request_Detail_Uid], [Axioma_Job_Id], [Axioma_Template], [Axioma_Job_Perc_Completed], "\
          "  convert(varchar,[Axioma_Analysis_Date],120) [Axioma_Analysis_Date], "\
          "  [ESM_TS].[dbo].[VIDRIO_UTILS$Sql_Date_To_Period] ([Axioma_Analysis_Date],'M') [PERIOD] "\
          "  from [ESM_TS].[dbo].[RISK_CALCULATIONS_LOG] "\
          "  where [ESM_Init_End_Time] is not null and [ESM_Calc_End_Time] is not null and [ESM_Process_End_Time] is null "\
          "  and  [Vidrio_Request_Uid] = '"+request_uid+"' "\
          "  and Axioma_Job_Perc_Completed = 100 "\
          "  order by [Vidrio_Request_Detail_Uid] "
    data_reader.exec_query(sql)
    req_details=data_reader.get_result_set()
    return req_details

def getJobIds(req_uid,data_reader):
    sql = "select distinct [Axioma_Job_Id], [Axioma_Template] from RISK_CALCULATIONS_LOG where [Vidrio_Request_Uid] = '"+str(req_uid)+"'"
    data_reader.exec_query(sql)
    jobids=data_reader.get_result_set()
    return jobids

def getCalcResultsMetaData(req_uid,data_reader):
    sql = "select distinct [Axioma_Job_Id], [Axioma_Template], [Axioma_Job_Perc_Completed], "\
        " convert(varchar,[Axioma_Analysis_Date],120) [Axioma_Analysis_Date], " \
        " [ESM].[dbo].[VIDRIO_UTILS$Sql_Date_To_Period] ([Axioma_Analysis_Date],'M') [PERIOD] "\
        " from RISK_CALCULATIONS_LOG where [Vidrio_Request_Uid] = '"+req_uid+"' "\
        " and [Axioma_Job_Perc_Completed] = 100 and [ESM_Process_End_Time] is null"
    data_reader.exec_query(sql)
    jobids=data_reader.get_result_set()
    return jobids

def getOutputRiskColumns(proc_type_id,data_reader):
    sql = f"select * from [dbo].[VIDRIO_RISK$Get_Risk_Columns]({proc_type_id})"

    data_reader.exec_query(sql)
    columns=data_reader.get_result_set()
    return columns

def getOutputColumnsForTemplate(columns,template):
    return [cols for cols in columns if cols['TEMPLATE_NAME'] == template]

def get_analysis_dates_range(request_uid,data_reader):
    sql = "select min([VEDI].[dbo].[FN_GEN_Period_Get_End_Serial_Date](Starting_Period)) sasof, " \
			"		max([VEDI].[dbo].[FN_GEN_Period_Get_End_Serial_Date](Ending_Period)) easof "\
			"		from [VEDI].[dbo].Vidrio_Request_Detail " \
			"		where Vidrio_Request_Uid = '"+str(request_uid)+"'"
    data_reader.exec_query(sql)
    analysis_dates_range=data_reader.get_result_first_row()
    return analysis_dates_range

def getRiskResultsToPopulateInVedi(data_reader):
    sql="select count(*) cnt, Vidrio_Request_Detail_Uid "\
        "from [RISK_CALCULATIONS_RESULT] "\
        "where [Processed] = 0 "\
        "and [Value] is not null "\
        "group by vidrio_Request_Detail_Uid"
    data_reader.exec_query(sql)
    records_to_populate=data_reader.get_result_first_row()
    return records_to_populate


def getReqType (req_det_uid,data_reader):
    sql = f"select Risk_Mode from [VEDI].[dbo].[Vidrio_Request_Detail] where Vidrio_Request_Detail_Uid ='{req_det_uid}'"
    req_type =data_reader.get_query_result_field(sql,'Risk_Mode')
    return req_type

def get_request_details_esm_processed(request_uid,data_reader):
    sql = "select Vidrio_Request_Detail_Uid "\
          "from [VEDI].[dbo].Vidrio_Request_Detail "\
          "where vidrio_request_uid = '"+request_uid+"' and ESM_Processed = 1"
    data_reader.exec_query(sql)
    req_details=data_reader.get_result_set()
    return req_details


#################
#AXIOMA REST API 
#################

def get_generic_sec_json(tsLookup,tsId,template,logger):
    GenericSecurity_json = {
        "Name": tsLookup,
        "TermsAndConditions.PriceTimeSeriesId": tsId,
        "TermsAndConditions.PaymentCurrency": "USD",
        "ModelingAssumptions": template,
    }
    return GenericSecurity_json

def portfolio_exists(portfolios,portfolio_to_find,logger) :
    #print (portfolios)
    for item in portfolios['items']:
        portf_name = item['name']
        if portf_name == str(portfolio_to_find):
            logger.info(portfolio_to_find + " FOUND")
            return 1
    return 0

def portfolio_positions_cleanup(portfolio_to_clean,pos_dates,blue,logger):
    keys = pos_dates.keys()
    for analysisDate in keys:
        blue.delete_positions_for_date(portfolio_to_clean, analysisDate) 
        logger.info ("DELETED Positions for portfolio : " + str(portfolio_to_clean) + " and analysis date: " + analysisDate)

def get_pt_position_json(clientId,tsLookup,allocation) :
    pos = { "clientId": clientId,
            "description": clientId,
            "identifiers": [ {
                "type": "ClientGiven",
                "value": tsLookup
                }],
            "quantity": {
                "value": allocation,
                "scale": "MarketValue",
                "currency": "USD"
            } 
        }
    return pos

def get_pt_ccy_position_json(clientId,allocation) :
    pos = { "clientId": clientId,
            "description": clientId,
            "identifiers": [ {
                "type": "Currency",
                "value": "USD"
                }],
            "quantity": {
                "value": allocation,
                "scale": "MarketValue",
                "currency": "USD"
            } 
        }
    return pos

def get_ts_position_json(clientId,tsLookup) :
    pos = { "clientId": clientId,
            "description": clientId,
            "identifiers": [ {
                "type": "ClientGiven",
                "value": tsLookup
                }],
            "quantity": {
                "value": 1,
                "scale": "NumberOfInstruments",
                "currency": "USD"
            } 
        }
    return pos
        
def process_fof_portfolio (request_uid, template, blue,logger):
    fof_portfolio = str(request_uid)+"_"+template
    Portf_Json= {
            "Name":  fof_portfolio,
            "defaultDataPartition": "AxiomaUS",
			"defaultCurrency": "USD"
        }
    portfolios  = blue.get_portfolio_names()
    found = portfolio_exists(portfolios,fof_portfolio,logger)
    if found == 0:
        blue.create_portfolio(fof_portfolio,Portf_Json) 
    else:
        pos_dates=blue.get_position_date_info(fof_portfolio)
        logger.debug("POSITIONS DATES: "+ str(pos_dates))
        if len(pos_dates) > 0: 
            portfolio_positions_cleanup(fof_portfolio,pos_dates,blue,logger)
    return fof_portfolio

def get_fof_portfolio (request_uid, template, blue,logger):
    portfolio_name =str(request_uid)+"_"+template
    portfolio_id = blue.get_portfolio_id(portfolio_name)
    logger.debug("PORTFOLIO ID for " + portfolio_name + " is " + str(portfolio_id))
    if portfolio_id > 0:
        return portfolio_name
    else:
        return None

######################
#DB writers
######################
def init_risk_calc_log(req_uid,template,analysisDate,subitem_id,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Init_Risk_Log]',(str(req_uid),template,analysisDate,subitem_id,))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def update_risk_calc_log_start_time(req_uid,req_det_uid,template,analysisDate,step,job_id,data_writer,logger) :
    try:
        logger.debug("[dbo].[VIDRIO_VEDI$Update_Calc_Risk_Log] : %s, %s , %s, %s, %s, %d, %s ",str(req_uid),template,analysisDate,step,job_id,0,req_det_uid)
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Update_Calc_Risk_Log]',(str(req_uid),template,analysisDate,step,job_id,0,req_det_uid))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def update_risk_calc_log_step(req_uid,template,step,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Update_Calc_Risk_Step_Log]',(str(req_uid),template,step,))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def update_risk_calc_log_end_time(req_uid,template,analysisDate,step,job_id,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Update_Calc_Risk_Log]',(str(req_uid),template,analysisDate,step,job_id,1))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def update_risk_calc_log_end_time_all(req_uid,step,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Update_Calc_Risk_Log_End_Time]',(str(req_uid),step))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def update_axioma_running_q(req_uid,step,run_status,req_type,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Update_Axioma_Running_Q]',(req_uid,step,run_status,req_type,))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def remove_from_axioma_running_q(req_uid,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Remove_From_Axioma_Running_Q]',(req_uid,))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def set_request_start_time(req_det_uid,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Set_Request_Start_Time]',(str(req_det_uid),))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def log_created_axioma_object(req_uid,obj_name, obj_type,data_writer,logger) :
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Log_Axioma_Object]',(str(req_uid),obj_name,obj_type,))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def saveResultsIntoDb (df,req_uid,request_det_uid, period,columns,data_writer,logger):
    results = df.to_dict('index')
    #create a dictionary of risk values to load to db
    records_to_load_to_db = dict()
    for row_num in results.keys():
        rec=results[row_num]
        values = list()
        for header in rec.keys():
            if 'ClientId' not in header:
                col_attr = [col for col in columns if col['COL_NAME'] == header] #search for vidrio data col id based by name - col attr list of one dictionary
                if len(col_attr) > 0:
                    Vidrio_Risk_Data_Column_Uid = col_attr[0]['Vidrio_Risk_Data_Column_Uid']
                    val = rec[header]
                    values.append({"ColName": header, "Vidrio_Risk_Data_Column_Uid": Vidrio_Risk_Data_Column_Uid , 'Value': val})
            else:
                entity=rec[header]
                if entity == 'Total':
                    subitem_id = 'TOTAL'
                elif 'Cash' in entity:
                    subitem_id = 'CASH'
                else:
                    subitem_id = '|'.join(entity.split("_")[0:3])
                #logger.debug("SUBITEM_ID: %s",subitem_id)
        records_to_load_to_db[subitem_id] = values        

    #load a dictionary of risk values for each det uid to db
    for subitem_id in records_to_load_to_db.keys():
        values=records_to_load_to_db[subitem_id]
        #print ('REQ DET UID: ', request_det_uid)
        #print (values) 
        if len(values) > 0:
            for rec in values:
                print(req_uid, request_det_uid, subitem_id, period,rec)
                Vidrio_Risk_Data_Column_Uid=rec['Vidrio_Risk_Data_Column_Uid']
                val=rec['Value']
                #print("saveResultsIntoDb :",req_uid, request_det_uid, subitem_id, period, Vidrio_Risk_Data_Column_Uid, val)
                saveRiskValIntoDb (req_uid, request_det_uid, subitem_id, period, Vidrio_Risk_Data_Column_Uid, val,data_writer,logger)
 
def saveRiskValIntoDb (req_uid, req_det_uid, subitem_id, period, Vidrio_Risk_Data_Column_Uid, val,data_writer,logger):
    try:
        if subitem_id == 'CASH':
            val=0
        #print ('[dbo].[VIDRIO_RISK$Save_Risk_Calculation_Values] ==>',req_uid,req_det_uid,subitem_id,period,Vidrio_Risk_Data_Column_Uid,val)
        if val is not None and ('float' in str(type(val)) or 'int' in str(type(val))):
            logger.debug("saveRiskValIntoDb ==> req_uid %s, request_det_uid %s, Vidrio_Risk_Data_Column_Uid %s, Value %s: ",str(req_uid),str(req_det_uid),str(Vidrio_Risk_Data_Column_Uid),str(val))
            data_writer.call_stored_proc('[dbo].[VIDRIO_RISK$Save_Risk_Calculation_Values]',(req_uid,req_det_uid,subitem_id,period,Vidrio_Risk_Data_Column_Uid,val,))
            data_writer.commit()
        else:
            logger.debug("saveRiskValIntoDb VALUE INSERT ERROR ==> req_uid %s, request_det_uid %s, Vidrio_Risk_Data_Column_Uid %s, Value %s: ",str(req_uid),str(req_det_uid),str(Vidrio_Risk_Data_Column_Uid),str(val))
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()

def update_factdecomp_processed_flag(req_det_uid,processed,data_writer,logger):
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_RISK$Update_Calc_FactorDecomp_Processed_Status]',(str(req_det_uid),processed,))
        data_writer.commit()
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()

def updateAxiomaJobPercCompleted(job_id,perc_completed,data_writer,logger):
    try:
        if perc_completed is not None:
            data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Update_Calc_Risk_Log_Job_Completion]',(job_id,perc_completed))
            data_writer.commit()
            logger.debug("UPDATE AXIOMA JOB ID: " + job_id + " percentage completed: " + str(perc_completed))
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def updateAxiomaJobOutputFile (job_id,out_file,data_writer,logger):
    try:
        print ("JOB ID: ",job_id,"  OUTPUT FILE: ", out_file)
        if out_file is not None:
            data_writer.call_stored_proc('[dbo].[VIDRIO_VEDI$Update_Calc_Risk_Log_Output_File]',(job_id,out_file))
            data_writer.commit()
            logger.debug("UPDATE AXIOMA JOB ID: " + job_id + " output file : " + out_file)
    except:
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def saveFactorDecompValue(req_det_uid,subentity_id,period,dd_col_id,risk_factor,val,data_writer,logger):
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_RISK$Save_Esm_TS_Risk_Factors_Values_REST]',(req_det_uid,subentity_id,period,dd_col_id,float(val['Value']),risk_factor,1,))
        print (req_det_uid,subentity_id,period,dd_col_id,risk_factor,val)
        data_writer.commit()
    except:
        print(req_det_uid,subentity_id,period,dd_col_id,risk_factor,val['Value'])
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        os._exit(0)

def save_factdecomp_output(entity_uid, requester_id,period,url,req_type,xml,data_writer,logger):
    try:
        data_writer.call_stored_proc('[dbo].[VIDRIO_RISK$Save_Risk_Factor_Decomp_Output]',(entity_uid, requester_id,period,req_type,url,xml,))
        data_writer.commit()
        logger.debug(" save_factdecomp_output SUCCESS for %s, %s, %s, %s, %s, %s",entity_uid, requester_id,period,url,req_type,xml)
    except:
        logger.error(" save_factdecomp_output ERROR for %s, %s, %s, %s, %s, %s",entity_uid, requester_id,period,url,req_type,xml)
        logger.error("Database error:", sys.exc_info()[0])
        logger.error(sys.exc_info()[1])
        data_writer.rollback()
        sys.exit()



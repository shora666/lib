import blpapi
import sys
import platform as pt

OS=pt.system()
if (OS=='Windows'):
    sys.path.insert(0,'C:/src/git/python/lib')
    sys.path.insert(1,'C:/src/git/python/lib/blp')
else:
    sys.path.insert(0,'/usr/local/share/python/lib')
    sys.path.insert(1,'/usr/local/share/python/lib/blp')

from globvars import globvars
from HistoricalRequest import HistoricalRequest
from ReferenceDataRequest import ReferenceDataRequest
from optparse import OptionParser



SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")
AUTHORIZATION_SUCCESS = blpapi.Name("AuthorizationSuccess")
TOKEN_SUCCESS = blpapi.Names.TOKEN_GENERATION_SUCCESS

HISTORICAL_REQUEST="HistoricalDataRequest"
REFERENCE_DATA_REQUEST="ReferenceDataRequest"

REFDATA_SVC = "//blp/refdata"
AUTH_SVC = "//blp/apiauth"
APIFLDS_SVC = "//blp/apiflds"


class BLPSAPI:
    def __init__(self,d_host=None,d_port=None,d_authOption=None,d_name=None,debug=False,SQLPRocessor=None,globv=None,aws=False):
        self.globv = globv
        self.d_port= d_port
        self.d_authOption = d_authOption 
        self.d_name =  d_name
        self.d_session = None
        self.d_identity = None
        self.d_apiAuthSvc = None
        self.d_hosts = list()
        if d_host is not None:
            self.d_hosts.append(d_host) 
        self.d_sec_list=list()
        self.d_hist_flds_list=list()
        self.d_ref_flds_list=list()
        self.sql_fields = dict()
        self.d_refDataService =  None
        self.debug=debug
        self.SQLPRocessor=SQLPRocessor
        print("BLPSAPI DEBUG:",self.debug)
    
        if len(self.d_hosts) == 0:
            print("Missing host IP")
            self.printUsage()

        self.d_req_obj = None 
        if self.globv is None and not aws :
            self.globv = globvars(app='BLPSAPI',db_driver = 'pyodbc')

    def printUsage(self): 
        print("Usage:")
        print("	Retrieve History data ")
        print("		[-s			<security	= IBM US Equity>")
        print("		[-f			<field		= LAST_PRICE>")
        print("		[-sd		<startDateTime  = 20091026>")
        print("		[-ed		<endDateTime    = 20091030>")
        print("        [-ip        <ipAddress	= localhost>")
        print("        [-p         <tcpPort	= 8194>")
        print("        [-auth      <authenticationOption = LOGON (default) or NONE or APPLICATION or DIRSVC or USER_APP>]")
        print("        [-n         <name = applicationName or directoryService>]")
        print("Notes:")
        print(" -Specify only LOGON to authorize 'user' using Windows login name.")
        print(" -Specify DIRSVC and name(Directory Service Property) to authorize user using directory Service.")
        print(" -Specify APPLICATION and name(Application Name) to authorize application.")
    
    def parseCmdLine(self):
        parser = OptionParser(description="Retrieve reference data.")
        parser.add_option("-a",
                        "--ip",
                        dest="host",
                        help="server name or IP (default: %default)",
                        metavar="ipAddress",
                        default="localhost")
        parser.add_option("-p",
                        dest="port",
                        type="int",
                        help="server port (default: %default)",
                        metavar="tcpPort",
                        default=8194)
        parser.add_option("-r",
                        dest="req_type",
                        help="specify the request type (default: %(default)s)",
                        metavar="requestType",
                        default=REFERENCE_DATA_REQUEST)

        (options, args) = parser.parse_args()

        return options


    def _findColType(self, col_name) -> str:
        col_type = [x for x in self.db_columns if x['FIELD_NAME'] == col_name]
        return col_type[0]['COL_TYPE']

    def add_security(self,sec):
        self.d_sec_list.append(sec)

    def reset_securities(self):
        self.d_sec_list=list()

    def get_securities(self):
        return self.d_sec_list

    def reset_ref_fields(self):
        self.d_ref_flds_list=list()

    def reset_hist_fields(self):
        self.d_hist_flds_list=list()

    def add_field(self,fld,type):
        if type == 'HistoricalDataRequest':
            self.d_hist_flds_list.append(fld)
        else:
            self.d_ref_flds_list.append(fld)

    def get_static_fields(self):
        return self.d_ref_flds_list
    
    def get_historical_fields(self):
        return self.d_hist_flds_list

    def initData(self,db_columns:list=None,tickers:list=None,fields:list=None,type:str=None):
        print ("INIT DATA: ",db_columns)
        if self.SQLPRocessor  is None and self.globv is not None:
            self.SQLPRocessor = self.globv.get_SQL_Processor()
        print("initData ==> RESETTING TICKERS : {}".format(self.d_sec_list))
        self.tickers = tickers
        self.d_sec_list = [] if tickers is None else tickers 
        if type == 'HistoricalDataRequest':
            print("initData  ==> RESETTING HISTORICAL FIELDS : {}".format(fields))
            self.d_hist_flds_list = [] if fields is None else fields
        else:
            self.d_ref_flds_list = [] if fields is None else fields
        if db_columns is not None:
            self.db_columns = db_columns
        else:
            sp_name = '{call [dbo].[ESM_AUTOMATION$Get_Mrkt_Data_Fields]}'
            sp_args = ()
            self.db_columns = self.SQLPRocessor.execute_sp(sp_name=sp_name, sp_args=sp_args,get_result=True) 
        for cols in self.db_columns:
            if self.debug:
                print(cols)

    def getDbFields(self):
        return self.db_columns

    def setDbFields(self,db_columns):
        self.db_columns = db_columns

    def initTestData(self,type):
        self.d_sec_list=['IBM Equity','TSLA Equity']
        if type == 'HistoricalDataRequest':
            self.d_hist_flds_list=['PX_LAST','CUR_MKT_CAP']
        else:
            self.d_ref_flds_list=['ID_ISIN','TICKER','GICS_SUB_INDUSTRY_NAME']
    
    def getRefDataService(self):
        return self.d_refDataService

    def createSession(self) -> bool:
        #reset bbg session attributes
        self.d_refDataService =  None
        self.d_req_obj =  None
        self.d_request =  None

        if self.d_authOption =="APPLICATION":
            authOptions = "AuthenticationMode=APPLICATION_ONLY;"
            authOptions += "ApplicationAuthenticationType=APPNAME_AND_KEY;"
            authOptions += "ApplicationName=" + self.d_name
        print("d_authOption = " , self.d_authOption)
        print("authOptions = ",authOptions)
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(self.d_hosts[0])
        sessionOptions.setServerPort(self.d_port)

        print("CREATE SESSION ==> self.d_authOption: ",self.d_authOption," authOptions: ",authOptions)
 
        if self.d_authOption is not None and authOptions is not None: 
            sessionOptions.setAuthenticationOptions(authOptions)
	
	
        sessionOptions.setDefaultSubscriptionService(REFDATA_SVC)
        sessionOptions.setAutoRestartOnDisconnection(True)
		
        print("Connecting to port ",self.d_port," on server: ",self.d_hosts[0])
        self.d_session = blpapi.Session(sessionOptions)

        if self.d_session.start():
            if self.d_authOption is not None:
                self.d_identity = self.d_session.createIdentity()
                print ("IDENTITY ==> ",self.d_identity)
                if not self.authorize():
                    print("NOT AUTHORIZED!!")
                    return False
                else:
                    print("AUTHORIZED!!")
                    return True
        return False

    def authorize(self) -> bool:
        tokenEventQueue = blpapi.EventQueue()
        corrlationId = blpapi.CorrelationId(99)
        if self.debug:
            print("CORRELATION ID: ",corrlationId)
            print("blpapi.Event.TOKEN_STATUS: ", blpapi.Event.TOKEN_STATUS)
            print ("SESSION ==> ",self.d_session)
        self.d_session.generateToken(correlationId=corrlationId, eventQueue=tokenEventQueue)
        token = None
        timeoutMilliSeonds = 10000
        event = tokenEventQueue.nextEvent(timeoutMilliSeonds)
        if self.debug:
            print("event.eventType()", event.eventType())
        if event.eventType() == blpapi.Event.TOKEN_STATUS:
            iter = event.__iter__()
            for msg in iter:
                print("authorize ==> ",msg.toString())
                if msg.messageType() == TOKEN_SUCCESS:
                    token = msg.getElementAsString("token")
        if token is None:
            print("Failed to get token")
            return False

        if self.d_session.openService(AUTH_SVC): 
            authService = self.d_session.getService(AUTH_SVC)
            authRequest = authService.createAuthorizationRequest()
            authRequest.set("token", token)
	
            authEventQueue = blpapi.EventQueue()
	
            self.d_session.sendAuthorizationRequest(authRequest, self.d_identity,
					eventQueue=authEventQueue, correlationId=blpapi.CorrelationId(self.d_identity))
	
            while (True):
                event = authEventQueue.nextEvent()
                print("EVENT TYPE: ", event.eventType())
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE or event.eventType() == blpapi.Event.REQUEST_STATUS:
                    msgIter = event.__iter__()
                    for msg in msgIter: 
                        print("RESPONSE MSG:",msg, "MESSAGE TYPE: ",msg.messageType())
                        if msg.messageType() == AUTHORIZATION_SUCCESS:
                            return True
                        else: 
                            print("Not authorized")
                            return False
        return False

    def createRequest(self,type,periodicity='MONTHLY') -> None:
        self.d_request = None
        if self.debug:
            print ("BLPSAPI createRequest ==> REQUEST TYPE TO CREATE =====> ",type)
        if not self.d_session.openService(REFDATA_SVC):
            print ("Failed to open ",REFDATA_SVC)
            return None
        self.d_refDataService = self.d_session.getService(REFDATA_SVC)
        if type == 'HistoricalDataRequest' or type == 'HistoricalRequest':
            print ("DATA SERVICE OPENED =====> {}".format(type), "  FIELDS: ", self.d_hist_flds_list)
            self.d_req_obj= HistoricalRequest(periodicity=periodicity,start=self.start_date,end=self.end_date,sec_list=self.d_sec_list,flds_list=self.d_hist_flds_list,ccy=self.ccy,db_columns=self.db_columns,debug=self.debug)
        else:
            self.d_req_obj = ReferenceDataRequest(periodicity=periodicity,sec_list=self.d_sec_list,flds_list=self.d_ref_flds_list,debug=self.debug,SQLPRocessor=self.SQLPRocessor,globvars=self.globv)
            print ("DATA SERVICE OPENED =====> {}".format(type), "  FIELDS: ", self.d_ref_flds_list)

        #if self.debug:
        #    print("BLPSAPI createRequest ==> REQUEST OBJ :", self.d_req_obj)
        if self.d_req_obj is not None:
            self.d_request= self.d_req_obj.createRequest(self.d_refDataService)
        if self.debug:
            print("BLPSAPI createRequest ==> REQUEST:", self.d_request)
    
    def getRequest(self) -> blpapi.Request:
        return self.d_request

    def processRequest(self,type) -> None:
        # Send the request
        if self.d_request is not None:
            print ("REQUEST:",self.d_request)
            correlationId = self.d_session.sendRequest(self.d_request,identity=self.d_identity)
            #print("SEND REQUEST CORRELATION ID:",correlationId)
            ev = self.d_session.nextEvent(500) 
            #print("EVENT: ", ev, "  EVENT TYPE: ", ev.eventType(), "  REQUEST OBJECT TYPE: ", self.d_req_obj)
            self.data = self.d_req_obj.processResponseEvents(self.d_session,self.SQLPRocessor,self.tickers )
            #if type == 'HistoricalDataRequest':
            #    self.processHistoricalEvents()
            #else:
            #    self.processRefDataEvents()

    def processHistoricalEvents(self):
        self.d_req_obj.processResponseEvents(self.d_session,self.SQLPRocessor,self.tickers)

    def processRefDataEvents(self):
        self.d_req_obj.processResponseEvents(self.d_session,self.SQLPRocessor,self.tickers)

    def getResponseData(self):
        return self.d_req_obj.getResponseData()
        
   
    def set_host (self,host) -> None:
        self.d_hosts.add(host)
    
    def set_port(self,port) -> None:
        self.d_port = port
    
    def setAuthMethod(self,authMethod) -> None:
        self.d_authOption = authMethod
    
    def setUuid (self,d_uuid) -> None:
        self.d_uuid = d_uuid

    def setAppName(self,appName) -> None:
        self.d_name =  appName
    
    def set_start_date(self,sd):
        self.start_date=sd

    def set_end_date(self,ed):
        self.end_date=ed

    def set_ccy(self,ccy):
        self.ccy = ccy
    
    def getSession(self) -> blpapi.Session:
        return self.d_session
    
    def getIdentity(self) ->blpapi.Identity:
        return self.d_identity

if __name__ == "__main__":
    drvr = BLPSAPI(d_host='172.18.20.21',d_port=8194,d_authOption="APPLICATION", d_name="focus:blpclient")
    options = drvr.parseCmdLine()
    print (options.req_type)
    drvr.initData()
    drvr.initTestData('HistoricalDataRequest')
    print(drvr.createSession())
    drvr.createRequest('HistoricalDataRequest')
    drvr.processRequest('HistoricalDataRequest')
    drvr.initTestData('ReferencRequest')
    print(drvr.createSession())
    drvr.createRequest('ReferenceDataRequest')
    drvr.processRequest('ReferenceDataRequest')

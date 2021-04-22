# -*- coding: utf-8 -*-



import datetime
import requests
import six
from six.moves import urllib
from six import string_types
from urllib.parse import urljoin
#from datamodel.webservice import Struct


def urljoiner(baseurl, path_or_pathlist):
    if isinstance(path_or_pathlist, string_types):
        if(baseurl.rsplit('/', 1)[-1] == "rest"):
            return urljoin(baseurl, "rest" + path_or_pathlist)
        else:
            return urljoin(baseurl, path_or_pathlist)
    else:
        if(baseurl.rsplit('/', 1)[-1] == "rest"):
            return urljoin(
                baseurl, (('/'.join(["rest"] + path_or_pathlist))).replace("//", "/"))
        else:
            return urljoin(baseurl, '/'.join(path_or_pathlist))
        



class AxRiskConnector:
    def __init__(self,
                 host="ci2-enterprise.axioma.com",
                 port=None,
                 path="api/v1",
                 user="dvandenbussche",
                 passwd="Abcd1234!",
                 grant_type="password",
                 client_id="5BFC4998EC4E4B84A3924752DC04C290",
                 debug=False,
                 protocol='http'):

        if(port is None and protocol == "http"):
            port = str(8681)
            self.baseurl = '%s://%s:%s' % (protocol, host, port)
        elif(port is None and protocol == "https"):
            self.baseurl = '%s://%s' % (protocol, host)

        self.path = path
        self.user = user
        self.debug = int(debug)

        self.auth_data = {'password': passwd,
                          'grant_type': grant_type,
                          'client_id': client_id,
                          'username': user}
        print (self.auth_data)

        self.login()

    def handle_error_message(self, output):
        if(self.debug):
            print("Total seconds taken: " +
                  str(output.elapsed.total_seconds()))
        if(output.status_code > 399):
            print("Error in Axioma Risk Connector")
            print("Status code: " + str(output.status_code))
            print("Problem: " + output.content.decode("utf-8"))
            print("Reason: " + output.reason)
            raise Exception(
                "Exiting because: " +
                output.content.decode("utf-8"))

    def login(self):
        """
        login method to api
        """
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        token_url = urljoiner(self.baseurl, ["connect/token"])
        if(self.debug):
            print(token_url)
            print(self.auth_data)
        r = requests.post(token_url, data=self.auth_data, headers=headers)
        # New environemnts do not redirect /rest/connect/token to
        # /auth/connect/token so lets check this case explicitly
        if(r.status_code > 400):
            new_token_url = self.baseurl.rstrip(
                "/rest") + "/auth/connect/token"
            if(self.debug):
                print("cannot connect to: " + token_url)
                print("trying: " + new_token_url)
            r = requests.post(
                new_token_url,
                data=self.auth_data,
                headers=headers)
        self.handle_error_message(r)
        access_token = r.json().get('access_token')
        self.headers = {'Authorization': 'Bearer ' + access_token,
                        'Content-Type': 'application/json'}

    def get_portfolio_names(self, match_case=""):
        """Lists Portfolios. (Auth policies: Users)

        Endpoint:
            /portfolios

        Keyword arguments:
            match_case: passed to Odata filter

        """

        url = urljoiner(
            self.baseurl, [
                self.path, "portfolios?$filter=contains(name, '" + urllib.parse.quote_plus(match_case) + "')"])
        if(self.debug):
            print(url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return r.json()
        
    def get_portfolio_id(self, portfolio_name):
        pIds = self.get_portfolio_names(match_case=portfolio_name)
        portfolio_names = {i['id']: i['name'] for i in pIds['items']}
        names = []
        for key, value in six.iteritems(portfolio_names):
            if portfolio_name == value:
                names.append(key)
        if(len(names) == 0):
            raise LookupError("Error... cannot find portfolio name " +
                              portfolio_name)
        elif(len(names) > 1):
            raise LookupError("Error... multiple portfolios with name " +
                              portfolio_name)
        else:
            return names[0]

    def __convert_to_pid__(self, pID):
        if(isinstance(pID, string_types)):
            return self.get_portfolio_id(pID)
        else:
            return pID

    def __get_request_id__(self, headers):
        if(isinstance(headers, int)):
            return headers
        elif(isinstance(headers, string_types)):
            return (int(headers.split("/")[-1]))
        elif(hasattr(headers, "Location")):
            return int(headers.Location.split("/")[-2])
        elif(isinstance(headers, requests.structures.CaseInsensitiveDict) or isinstance(headers, dict)):
            # Due to error PA headers (PA-2179) lets try both locations to see
            # if we can parse endpoint data
            try:
                return int(headers['Location'].split("/")[-1])
            except ValueError:
                return int(headers['Location'].split("/")[-2])
        return None

    def get_analysis_id(self,
                        analysis_name,
                        owner=None,
                        team=None):
        analysis_ids = self.get_analysis_definition_names(match_case=analysis_name,
                                                          owner=owner,
                                                          team=team)
        analysis_names = {i['id']: i['name'] for i in analysis_ids['items']}
        names = []
        for key, value in six.iteritems(analysis_names):
            if analysis_name == value:
                names.append(key)
        if(len(names) == 0):
            raise LookupError("Error... cannot find analyses name " +
                              analysis_name)
        elif(len(names) > 1):
            raise LookupError("Error... multiple analyses with name " +
                              analysis_name)
        else:
            return names[0]
        
    def __convert_to_analysis_id__(self, analysis_id, owner=None, team=None):
        if(isinstance(analysis_id, string_types)):
            return self.get_analysis_id(analysis_id, owner=owner, team=team)
        else:
            return analysis_id

    def get_analysis_definition_names(self,
                                      match_case="",
                                      owner=None,
                                      team=None):
        """Lists Analysis Definitions. (Auth policies: Users)

        Endpoint:
            /analysis-definitions

        Keyword arguments:
            match_case: portfolio case passed to Odata filter
            owner: will return analysis def matching owner
            team: will return analysis def matching team

        """
        myfilter = "?$filter=contains(name, '" + \
            urllib.parse.quote_plus(match_case) + "')"
        if(owner is not None):
            myfilter = myfilter + " and owner eq '" + owner + "'"
        if(team is not None):
            myfilter = myfilter + " and team eq '" + team + "'"
        url = urljoiner(
            self.baseurl, [
                self.path, "analysis-definitions" + myfilter])
        if(self.debug):
            print(url)
        r = requests.get(url,
                         headers=self.headers)
        self.handle_error_message(r)
        return r.json()
        
    def get_position_date_info(self, portfolio_id):
        """Lists dates there are Positions for the Portfolio, latest first (Auth policies: Users)
        
        Endpoint:
            /portfolios/{id}/positions

        Keyword arguments:
            portfolio_id: portfolio name or id
        """
        portfolio_id = self.__convert_to_pid__(portfolio_id)
        portfolio_position_url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id),
                                                          'positions'])
        if(self.debug):
            print(portfolio_position_url)
        r = requests.get(portfolio_position_url,
                         headers=self.headers)
        self.handle_error_message(r)

        position_info = r.json().get("items")
        # Put the position date and position Count in a dict
        date_position_info = {}
        for posInfo in position_info:
            date_position_info[posInfo.get(
                "asOfDate")] = posInfo.get("positionsCount")
        return date_position_info
    
    def create_portfolio(self, name, args={}):
        """Create a new portfolio (Auth policies: Users)

        Endpoint:
            /portfolios

        Keyword arguments:
            name: portfolio name
            args(optional): arguments to pass to post
        """
        url = urljoiner(self.baseurl, [self.path, 'portfolios'])
        args['name'] = name
        if(self.debug):
            print(url)
            print(args)
        r = requests.post(url,
                          json=args,
                          headers=self.headers)
        self.handle_error_message(r)
        return r.headers
    
    def save_positions_for_date_post(self,
                                     portfolio,
                                     date,
                                     position_data):
        """Create a new Position in the Portfolio on the given date. (Auth policies: Users)

        Endpoint:
            /portfolios/{id}/positions/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            date: date to save position
            position_data: position to save

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        position_date = self.__convert_datetime_to_string__(date)
        portfolio_positions_url = urljoiner(self.baseurl, [self.path,
                                                           'portfolios',
                                                           str(portfolio_id),
                                                           'positions',
                                                           position_date])
        if(self.debug):
            print(portfolio_positions_url)
            print(position_data)
        r = requests.post(portfolio_positions_url,
                          json=position_data,
                          headers=self.headers)
        self.handle_error_message(r)
        return r

    def save_positions_for_date_patch(self,
                                      portfolio,
                                      date,
                                      position_data):
        """Patches the existing collection of Positions according to the supplied operations (Auth policies: Users)

        Endpoint:
            /portfolios/{id}/positions/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            date: date to save position
            position_data: list of positions to save

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        position_date = self.__convert_datetime_to_string__(date)
        portfolio_positions_url = urljoiner(self.baseurl, [self.path,
                                                           'portfolios',
                                                           str(portfolio_id),
                                                           'positions',
                                                           position_date])
        if(self.debug):
            print(portfolio_positions_url)
            print(position_data)
        data = {'upsert': position_data, 'remove': []}
        r = requests.patch(portfolio_positions_url,
                           json=data,
                           headers=self.headers)
        self.handle_error_message(r)
        return r    

    def __convert_datetime_to_string__(self, date):
        if(isinstance(date, string_types)):
            from dateutil.parser import parse
            return self.__convert_datetime_to_string__(parse(date))
        else:
            return date.strftime("%Y-%m-%d")
    
    def run_aggregation(self,
                        analysis,
                        portfolio,
                        analysis_date=None,
                        batch_name=None,
                        options=None,
                        show_benchmark=False,
                        owner=None,
                        team=None):
        """Creates a new portfolio risk analysis request and submits it for processing (Auth policies: Users)

        Endpoint:
            /analyses/risk/portfolios/{id}

        Keyword arguments:
            analysis: analysis name or id to run
            portfolio: portfolio name or id
            analysis_date: date (default=latestportfolio date)
            batch_name: batch name to run
            options: compute options for analysis
            show_benchmark: passed to bluepy
        """

        if isinstance(analysis, dict):
            analysis_definition = {
                "dynamicDefinition": analysis
            }
        else:
            analysis = self.__convert_to_analysis_id__(
                analysis, owner=owner, team=team)
            analysis_definition = {
                "Id": analysis
            }
        portfolio_id = self.__convert_to_pid__(portfolio)
        if(analysis_date is None):
            analysis_date = self.get_latest_portfolio_date(portfolio_id)
        else:
            analysis_date = self.__convert_datetime_to_string__(analysis_date)

        data = {
            "analysisDate": analysis_date,
            "analysisDefinition": analysis_definition,
            "includePositionsInBenchmark": show_benchmark
        }
        if(options is not None):
            data.update(options.to_dict() if isinstance(options,Struct) else options)
        
        if(batch_name is not None):
            data['batchName'] = batch_name
        url = urljoiner(self.baseurl, [self.path,
                                       "analyses",
                                       "risk",
                                       "portfolios",
                                       str(portfolio_id)])
        if(self.debug):
            print(url)
            print(data)
        r = requests.post(url,
                          json=data,
                          headers=self.headers)
        self.handle_error_message(r)
        return r.headers    

    def get_aggregation_status(self, request_id):
        """Get Analysis Status

        Endpoint:
            /analyses/{request_id}/status

        Keyword arguments:
            request_id: id of request
        """
        request_id = self.__get_request_id__(request_id)
        url = urljoiner(
            self.baseurl, [
                self.path, "analyses", str(request_id), "status"])
        if(self.debug):
            print(url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return r.json()

    def get_aggregation_results(self, request_id, csvOut=False):
        """Get Analysis Results

        Endpoint:
            /analyses/{request_id}

        Keyword arguments:
            request_id: id of request
            csvOut(optional): bool if results should be json or csv (default=json)
        """
        new_header = dict(self.headers)
        if(csvOut):
            new_header['Accept'] = "text/csv"

        request_id = self.__get_request_id__(request_id)
        url = urljoiner(self.baseurl, [self.path, "analyses", str(request_id)])
        if(self.debug):
            print(url)
        r = requests.get(url,
                         headers=new_header)
        self.handle_error_message(r)
        if(csvOut):
            return r.content
        else:
            return r.json()


    def get_template(self, template_name):
        """Returns a single template. (Auth)

        Endpoint:
            /api/v1/metadata/templates/{name}

        Keyword arguments:
            template_name: name of temp
        """
        url = urljoiner(self.baseurl, [self.path,
                                       "metadata",
                                       "templates",
                                       template_name])
        if(self.debug):
            print(url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return r.json()
    
    def post_entity_from_template(self, template_name, content):
        """Create a new entity instance, and it's underliers, using the template

        Endpoint:
            /templates/*/*/{template_name}

        Keyword arguments:
            template_name: name of template
            content: json

        """
        
        url = urljoiner(self.baseurl, [self.path,
                                       "templates",
                                       "*",
                                       "*",
                                       str(template_name)])
        if(self.debug):
            print(url)
        data = {
            "templateName": template_name,
            "content": content
        }
        r = requests.post(url,
                          json=data,
                          headers=self.headers)
        self.handle_error_message(r)
        return r.headers

    def get_entity_names(self,
                         match_case="",
                         typeName1="*",
                         typeName2="*"):
        """Gets entities for a given type. (Auth)

        Endpoint:
            /entities/{typeName1}/{typeName2}

        Keyword arguments:
            match_case: match all entities with a given string
            typeName1: filter entities for a given typename1
            typeName2: filter entities for a given typename2
        """

        myfilter = "?$filter=contains(name, '" + \
            urllib.parse.quote_plus(match_case) + "')"

        url = urljoiner(self.baseurl, [self.path,
                                       "entities",
                                       typeName1,
                                       typeName2 + myfilter])
        if(self.debug):
            print(url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return dict((i['xmlCupboardId'], i['name']) for i in r.json()["items"])

    def get_entity_id(self,
                      entity_name,
                      typeName1="*",
                      typeName2="*"):
        entities = self.get_entity_names(entity_name, typeName1, typeName2)
        for key, value in six.iteritems(entities):
            if entity_name == value:
                return key
        else:
            raise LookupError("Error... cannot find entity name " +
                              entity_name)

    def __convert_to_entity_id__(self, eID, typeName1="*", typeName2="*"):
        if(isinstance(eID, string_types)):
            return self.get_entity_id(eID, typeName1, typeName2)
        else:
            return eID    

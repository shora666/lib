# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 17:51:03 2020

@author: akashyap
"""


# blue.create_portfolio('Test_AS') 
# -- creates a portfolio with name Test_AS

# blue.save_positions_for_date_patch('Test_AS', '2020-08-16', pos) 
# -- adds positions to the portfolio Test_AS; the 'pos' object is defined as a list below

# blue.post_entity_from_template("GenericPriceTS", TS_json)
# -- posts a timeseries object; please refer TS_JSON below for the structure of this object

# blue.post_entity_from_template("GenericSecurity", GS_json)
# -- creates a generic security; please refer to GS_JSON below for the structure of this object

# blue.save_portfolio_group_data('Vidrio Portfolio Risk','Test_AS')
# -- adds Test_AS to portfolio group named Vidrio Portfolio Risk

# blue.run_aggregation('Test','Test_AS')
# -- runs a report named 'Test' on 'Test_AS' portfolio; the object returned containins aggregation status/results ID

# blue.get_aggregation_status('aggregation_id')
# -- gets status of the aggregation ID returned from run_aggregation method (see attached screenshot)

# blue.get_aggregation_results('aggregation_id')
# -- retrieves results for the aggregation ID returned from run_aggregation method (see attached screenshot)

import sys
import Build as b
from DataDriver import DataDriver
from DataReader import DataReader as dr
from EsmProperties import EsmProperties as prop
import pandas as pd

sys.path.insert(1,'/usr/local/share/python/lib')

data_reader=dr("DrataReader")
data_reader.init_connection("ESMPROD")
query = "select * from [ESM_TS].[dbo].[VIDRIO_VEDI$Get_Request_List] ('TS',1)"

frame = pd.read_sql(query, conn  )
print(frame.loc[0,'Vidrio_Request_Uid'])
for index, row in frame.iterrows():
   print (index, " ===> " ,row['Vidrio_Request_Uid'])

#blue = b.AxRiskConnector(host='vidrio-uat.axioma.com',
#                                 user='xxxxxx',
#                                 passwd='xxxxxx',
#                                 client_id='5B547589AC104E108293B3363104E8AF',
#                                 protocol="https",
#                                 debug=True)

TimeSeries_json = {
    "Name": "EX1_AS",
    "PricingCurrencyId": "USD",
    "Observations": obv,
    "ObservationPeriodicity.NumberOfUnits":1,
    "ObservationPeriodicity.Unit":"Month",
    }

GenericSecurity_json = {
    "Name": "API Generic Security_AS",
    "TermsAndConditions.PriceTimeSeriesId": "APIGTSEx",
    "TermsAndConditions.PaymentCurrency": "USD",
    "ModelingAssumptions": "TimeSeriesFXShockFinal"
    }

pos = [
      { "clientId": "AAPL",
       "description": "My AAPL Holding",
       "identifiers": [
           {
               "type": "Ticker",
               "value": "AAPL US Equity"
               }
           ],
       "quantity": {
           "value": 1234.56,
           "scale": "MarketValue",
           "currency": "USD"
           } },
      {  "clientId": "Twtr",
        "description": "My Twtr Holding",
        "identifiers": [
            {
                "type": "Ticker",
                "value": "TWTR US Equity"
                }
            ],
        "quantity": {
            "value": 1234.56,
            "scale": "MarketValue",
            "currency": "USD"
            } }
       ]

     
obv=[
     {
		"Key": "9/30/2010",
		"Value": 100
	  },
	  {
		"Key": "10/31/2010",
		"Value": 107
	  },
	  {
		"Key": "11/30/2010",
		"Value": 104.6353
	  },
	  {
		"Key": "12/31/2010",
		"Value": 116.0091571
	  },
	  {
		"Key": "1/31/2011",
		"Value": 120.7771335
	  },
	  {
		"Key": "2/28/2011",
		"Value": 123.9414944
	  },
	  {
		"Key": "3/31/2011",
		"Value": 123.5696699
	  },
	  {
		"Key": "4/30/2011",
		"Value": 129.3033026
	  },
	  {
		"Key": "5/31/2011",
		"Value": 128.8636713
	  },
	  {
		"Key": "6/30/2011",
		"Value": 122.2658514
	  },
	  {
		"Key": "7/31/2011",
		"Value": 118.7568214
	  },
	  {
		"Key": "8/31/2011",
		"Value": 107.6530586
	  },
	  {
		"Key": "9/30/2011",
		"Value": 89.66423253
	  },
	  {
		"Key": "10/31/2011",
		"Value": 93.33149964
	  },
	  {
		"Key": "11/30/2011",
		"Value": 92.50084929
	  },
	  {
		"Key": "12/31/2011",
		"Value": 90.46583061
	  },
	  {
		"Key": "1/31/2012",
		"Value": 94.79914389
	  },
	  {
		"Key": "2/29/2012",
		"Value": 101.3971643
	  },
	  {
		"Key": "3/31/2012",
		"Value": 102.2489005
	  },
	  {
		"Key": "4/30/2012",
		"Value": 101.2161866
	  },
	  {
		"Key": "5/31/2012",
		"Value": 99.77891674
	  },
	  {
		"Key": "6/30/2012",
		"Value": 94.71014777
	  },
	  {
		"Key": "7/31/2012",
		"Value": 95.98873477
	  },
	  {
		"Key": "8/31/2012",
		"Value": 97.97570158
	  },
	  {
		"Key": "9/30/2012",
		"Value": 98.03448508
	  },
	  {
		"Key": "10/31/2012",
		"Value": 99.37755946
	  },
	  {
		"Key": "11/30/2012",
		"Value": 101.7228714
	  },
	  {
		"Key": "12/31/2012",
		"Value": 107.3990117
	  },
	  {
		"Key": "1/31/2013",
		"Value": 114.4551244
	  },
	  {
		"Key": "2/28/2013",
		"Value": 115.9659294
	  },
	  {
		"Key": "3/31/2013",
		"Value": 120.0827231
	  },
	  {
		"Key": "4/30/2013",
		"Value": 123.6251682
	  },
	  {
		"Key": "5/31/2013",
		"Value": 126.0605847
	  },
	  {
		"Key": "6/30/2013",
		"Value": 124.6361023
	  },
	  {
		"Key": "7/31/2013",
		"Value": 131.9148513
	  },
	  {
		"Key": "8/31/2013",
		"Value": 130.7671908
	  },
	  {
		"Key": "9/30/2013",
		"Value": 134.8863573
	  },
	  {
		"Key": "10/31/2013",
		"Value": 135.1965929
	  },
	  {
		"Key": "11/30/2013",
		"Value": 139.495842
	  },
	  {
		"Key": "12/31/2013",
		"Value": 143.4854218
	  },
	  {
		"Key": "1/31/2014",
		"Value": 146.025111
	  },
	  {
		"Key": "2/28/2014",
		"Value": 157.0500029
	  },
	  {
		"Key": "3/31/2014",
		"Value": 147.0145053
	  },
	  {
		"Key": "4/30/2014",
		"Value": 148.4552482
	  },
	  {
		"Key": "5/31/2014",
		"Value": 151.3501245
	  },
	  {
		"Key": "6/30/2014",
		"Value": 160.400862
	  },
	  {
		"Key": "7/31/2014",
		"Value": 156.3427262
	  },
	  {
		"Key": "8/31/2014",
		"Value": 156.2636609
	  },
	  {
		"Key": "9/30/2014",
		"Value": 155.6229807
	  },
	  {
		"Key": "10/31/2014",
		"Value": 138.2398892
	  },
	  {
		"Key": "11/30/2014",
		"Value": 141.8617733
	  },
	  {
		"Key": "12/31/2014",
		"Value": 140.8971131
	  },
	  {
		"Key": "1/31/2015",
		"Value": 144.7436008
	  },
	  {
		"Key": "2/28/2015",
		"Value": 154.1229799
	  },
	  {
		"Key": "3/31/2015",
		"Value": 155.3713763
	  },
	  {
		"Key": "4/30/2015",
		"Value": 159.7529321
	  },
	  {
		"Key": "5/31/2015",
		"Value": 168.299719
	  },
	  {
		"Key": "6/30/2015",
		"Value": 162.7794882
	  },
	  {
		"Key": "7/31/2015",
		"Value": 168.5256042
	  },
	  {
		"Key": "8/31/2015",
		"Value": 156.610844
	  },
	  {
		"Key": "9/30/2015",
		"Value": 136.0008569
	  },
	  {
		"Key": "10/31/2015",
		"Value": 127.6912045
	  },
	  {
		"Key": "11/30/2015",
		"Value": 125.9673733
	  },
	  {
		"Key": "12/31/2015",
		"Value": 129.8093782
	  },
	  {
		"Key": "1/31/2016",
		"Value": 120.8525311
	  },
	  {
		"Key": "2/29/2016",
		"Value": 108.9364715
	  },
	  {
		"Key": "3/31/2016",
		"Value": 91.74629631
	  },
	  {
		"Key": "4/30/2016",
		"Value": 88.63609686
	  },
	  {
		"Key": "5/31/2016",
		"Value": 85.0108805
	  },
	  {
		"Key": "6/30/2016",
		"Value": 77.17287732
	  },
	  {
		"Key": "7/31/2016",
		"Value": 83.58594342
	  },
	  {
		"Key": "8/31/2016",
		"Value": 83.59430202
	  },
	  {
		"Key": "9/30/2016",
		"Value": 76.38011375
	  },
	  {
		"Key": "10/31/2016",
		"Value": 66.49652703
	  },
	  {
		"Key": "11/30/2016",
		"Value": 66.70266627
	  },
	  {
		"Key": "12/31/2016",
		"Value": 65.74214787
	  },
	  {
		"Key": "1/31/2017",
		"Value": 65.3082497
	  },
	  {
		"Key": "2/28/2017",
		"Value": 68.38426826
	  },
	  {
		"Key": "3/31/2017",
		"Value": 60.41750101
	  },
	  {
		"Key": "4/30/2017",
		"Value": 61.47480727
	  },
	  {
		"Key": "5/31/2017",
		"Value": 57.1285384
	  },
	  {
		"Key": "6/30/2017",
		"Value": 56.95715278
	  },
	  {
		"Key": "7/31/2017",
		"Value": 60.19801478
	  },
	  {
		"Key": "8/31/2017",
		"Value": 47.71896631
	  }
	]
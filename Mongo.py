import pymongo
import os
from datetime import date, datetime, time, timedelta

from pymongo import MongoClient
from bson.objectid import ObjectId

from optparse import OptionParser
from datetime import datetime

import pandas as pd

class Mongo :
    #server default is current ESM test server
    def __init__ (self, server='172.18.40.21',db='ESM',collection=None,debug=True):
        self.server = server
        self.debug=debug
        self.client = MongoClient(f"mongodb://{self.server}:27017/")
        self.db = self.client[db]
        self.collection=self.db[collection]
        if self.debug:
            dbs = self.client.list_database_names()
            print("DATABASES",list(dbs))
            cols =self.db.list_collections()
            print("COLLECTIONS:",list(cols))
    
    def set_collection(self,collection):
        self.collection=self.db[collection]

    def create_collection(self,name):
        exist = self.find_collection(name)
        if len(exist) == 0:
            self.db.create_collection(name)

    def change_db(self,db=None,collection=None):
        if collection is not None:
            if db is not None:
                self.db = self.client[db]
            self.collection=self.db[collection]

        if self.debug:
            print("CURRENT COLLECTION:",self.collection.name)
        
    def create_index(self,field,type='text',unique=False,name=None):
        if type=='text':
            idx_type = pymongo.TEXT
        elif type=='desc':
            idx_type = pymongo.DESCENDING
        else:
            idx_type = pymongo.ASCENDING
        self.collection.create_index([(field, idx_type)],unique = unique,background=True,name=name)

    def create_wildcard_text_index(self,name):
        self.collection.create_index([("$**", pymongo.TEXT)],background=True,name=name)

    def bulk_insert(self,data,debug=False):
        self.collection.insert_many(data)        
        if debug:
            print(data)

    def aggregate(self, field=None,max_cnt=0):
        field = f'${field}'
        pipeline= [{"$group" : { "_id" : f"{field}", "count": { "$sum": 1 } } },
                    {"$match": {"count" : {"$gt": max_cnt} } }]
        print("Pipeline:",pipeline)
        print("Aggregation:",list(self.collection.aggregate(pipeline)))

    def drop_index(self,name):
        self.collection.drop_index(name)

    def find_collection(self,col_name):
        col = [x for x in list(self.db.list_collections()) if x['name']==col_name]
        if self.debug:
            print("COLLECTION EXISTS:",col)
        return col

    def search(self,field=None,val=None,num_of_records=1):
        if field is not None and val is not None:
            cur = list(self.collection.find({field: val}))
        else:
            cur = list(self.collection.find())
        if self.debug:
            print ("For Collection:",self.collection)
            print (f"DATA FOUND for {field}:{val} ==>",cur)
        if num_of_records == 1 and len(cur) == 1:
            if self.debug:
                print("search res ==> {}".format(cur))
            return cur[0]
        elif len(cur) > 0 :
            return cur[0:num_of_records]
        else:
            return None

    def search_mult_fields(self,field1=None,field2=None, field3=None,val1=None,val2=None, val3=None,num_of_records=1):
        if field1 is not None and field2 is not None and field3 is not None and val1 is not None and val2 is not None and val3 is not None:
            cur = list(self.collection.find({
                "$and": [
                    {field1: val1},
                    {field2: val2},
                    {field3: val3}
                    ]
                }))
        if field1 is not None and field2 is not None and val1 is not None and val2 is not None:
            cur = list(self.collection.find({
                "$and": [
                    {field1: val1},
                    {field2: val2},
                    ]
                }))
        else:
            cur = list(self.collection.find())
        if self.debug:
            print ("For Collection:",self.collection)
            print ("DATA FOUND for {}:{} ==> {}".format((field1,field2,field3),(val1,val2,val3),cur[0]))
        if num_of_records == 1 and len(cur) == 1:
            if self.debug:
                print("search res ==> {}".format(cur))
            return cur[0]
        elif len(cur) > 0 :
            return cur[num_of_records-1]
        else:
            return None


    def clean_collection(self,filter={}):
        self.collection.delete_many(filter)

    def join(self,lf,ff,fcol,out,filter=None, fval=None,unwind = True):
        pipeline = list()
        if filter is not None:
            pipeline.append({
                '$match' : {f'{filter}':fval}
                })
        pipeline.append( 
                {
                 '$lookup': {
                    'from': fcol,
                    'localField': lf,
                    'foreignField': ff,
                    'as': out
                }
                }
        ) 
        if unwind:
            pipeline.append({
                '$unwind' : {'path':f'${out}'}
                })

        print (self.collection.name)
        print (pipeline)
        res= list(self.collection.aggregate(pipeline))
        return res
    
    def search_for_empty_array(self,fld_name):
        cur = self.collection.find({ fld_name: {'$size':0}})
        return cur

    def get_by_SYID_or_SYCODEFINAL(self, syid=None, sycodefinal=None, unwind = True):
        """use either syid or sycodefinal parameters, not both"""
        pipeline = list()
        if syid is not None:
            pipeline.append({
                '$match' : {f'SYID':syid}
                })
        pipeline.append( 
                {
                 '$lookup': {
                    'from': 'sec_master_global',
                    'localField': 'GLOBAL_SYID',
                    'foreignField': 'SYID',
                    'as': 'global_records'
                }
                }
        ) 
        if unwind:
            pipeline.append({
                '$unwind' : {'path':'$global_records'}
                })
        
        if sycodefinal is not None:
            pipeline.append({
                '$match' : {'SYCODEFINAL':sycodefinal}
                })
        pipeline.append(
                {
                '$lookup': {
                    'from': 'sec_master_global',
                    'localField': 'SYCODEFINAL',
                    'foreignField': 'SYCODEFINAL',
                    'as': 'global_records'
                    }
                }
        )
        if unwind:
            pipeline.append({
                '$unwind' : {'path': '$global_records'}
            })

        print (self.collection.name)
        print (pipeline)
        res= list(self.collection.aggregate(pipeline))
        return res
    def last_updated(self,filter=None):
        if filter is None:
            self.collection.update_many(
                {},
               { 
                   '$set': {'lastupdated': datetime.now().isoformat()}
                }, upsert= True
            )
        else:
            self.collection.update_one(
            filter, {
                '$set': {'lastupdated': datetime.now().isoformat()}
                }, upsert= True
        )

    def count(self,filter={}):
        res = self.collection.count_documents(filter)
        return res

    def build_field_value(self,value=None):
        val = dict()
        val['value']=value
        print("build_field_value ==> val type: {}".format(value))
        if 'list' in str(type(value)): #multiple results in the response
            if val['value'][0]['VAL'] == 'ERROR':
                val['dataerr']= f"{val['value'][0]['VAL']}:{val['value'][0]['ASOF'] }"
        else:
            if val['value']['VAL'] == 'ERROR':
                val['dataerr']= f"{val['value']['VAL']}:{val['value']['ASOF'] }"
        val['lastupdated'] = datetime.now()
        val['lastupdated']=val['lastupdated'].isoformat()
        return val

    def build_field_value_plus(self,value=None):
        val = dict()
        val['value']=value
        val['lastupdated'] = datetime.now()
        val['frequency']='quaterly'
        val['nextupdate']=val['lastupdated'] + timedelta (days=90)
        val['lastupdated']=val['lastupdated'].isoformat()
        val['nextupdate']=val['nextupdate'].isoformat() 
        val['updateerr']=None
        return val

    def update_field_value_plus(self,field=None,_id=None,value=None,plus=False):
        if plus:
            val = self.build_field_value_plus(value=value)
        else:
            val = self.build_field_value(value=value)
        if _id is not None and field is not None:
            myquery = { "_id": ObjectId(_id) }
            newvalues = { "$set": { f'{field}': val } }
            print("NEW VALUES ==>",newvalues)

            self.collection.update_one(myquery,newvalues)

if __name__ == '__main__':
    dvr = Mongo(db='ESM',collection='bbg_service')
    f_name = 'C:/Temp/bbg_pricing_out_test.xlsx'
    df=pd.read_excel(f_name)
    df = df.loc[df['VAL']!='ERROR']
    print (df)
    for idx,row in df.iterrows():
        tickers = [row['TICKER']]
        start_date = int(row['ASOF'].strftime('%Y%m%d') if  'str' not in str(type(row['ASOF'])) else row['ASOF'])
        end_date = int(row['ASOF'].strftime('%Y%m%d') if  'str' not in str(type(row['ASOF'])) else row['ASOF'])
        res = dvr.search_mult_fields(field1='tickers',field2='startdate',field3='enddate',val1=tickers,val2=start_date,val3=end_date) 
        if res is not None:
            print("RES ===>".format(res))
            id = res['_id']
            dvr.update_field_value_plus(field='response',_id=id,value=row.to_dict())
        print("=====> {}".format(res))
    #res=dvr.search('tickers',['LU2367661449 EQUITY'])
    #print(res)
    #dvr = Mongo(db='ESM',collection='sec_master_local')
    #dvr.clean_collection()
    #dvr.aggregate(field='syid',max_cnt=0)
    #dvr.create_collection('sec_master_global')
    #res = dvr.search("sycodefinal", "TSLA EQUITY",num_of_records=1)
    #id = res['_id']
    #val = res['COUNTRY_CODE_RISK']
    #print("COUNTRY_CODE_RISK ==>",val)
    #dvr.update_field_value_plus('COUNTRY_CODE_RISK',id,val)
    #res = dvr.search("sycodefinal", "TSLA EQUITY",num_of_records=1)
    #print("RESULT AFTER UPDATE ==>",res)
    #dvr.change_db(db='ESM',collection='sec_master_global')
    #dvr.search("syid", 2239138)
    #dvr.search(field="syid", val=2029)
    #dvr.drop_index('syid_1')
    #dvr.create_index('SYID',type='text',unique=True)
    #res = dvr.search()
    #dvr.create_wildcard_text_index(name='GENERAL_TEXT_IDX')
    #dvr.clean_collection()
    # res = dvr.join('GLOBAL_SYID','SYID','sec_master_global','global_records',filter='SYID',fval=26623)
    #print("RESULT START----------------------------------------")
    #syco_res = dvr.get_by_SYID_or_SYCODEFINAL(sycodefinal='TSLA EQUITY')
    #print(syco_res)
    # syid_res = dvr.get_by_SYID_or_SYCODEFINAL(syid=26623)
    # print(syid_res)
    print("RESULT END----------------------------------------")
    # print(res)

    #res = dvr.join('global_syid','syid','sec_master_global','global_records',filter='syid',fval=2239138)
    #print("JOIN:",res[0]['global_records']['_id'])
    #dvr.last_updated({'syid':2239138})
    #dvr.last_updated()
    #dvr.change_db(collection='sec_master_global')
    #res = dvr.search('sycodefinal','CORO LN Equity')
    #print(res)
    #res = dvr.count()
    #print("Count:",res)
    os._exit(0)
    

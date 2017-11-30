
from elasticsearch import Elasticsearch
es = Elasticsearch()
def dob_query_builder(query_date,filtered_list=None):
    #print(filtered_list)
    #print((type(filtered_list)==type([1])) and  (filtered_list.__len__())!=0)
    if( (type(filtered_list)==type([1])) and  ((filtered_list.__len__())!=0)):
        print("entering start of dob builder")
        query ={"query": {
          "bool": {
        "filter": {
          "terms": {
        "_id": filtered_list
          }
        }
        
        , "should": [
        {"match": {"DOB_1": query_date} },
        {"match": {"DOB_2": query_date}}
        ]
          }}}

                                        
    else:
        print("Entering Else of DOB")
        query= {"query": {"bool" : {"filter" : {"must": [{"match" : { "DOB_1" :  query_date  }},  {"match" : { "DOB_2" :  query_date  }}]}}}}
    return (query)
####################################


def Name_query_builder(query_name,filtered_list=None):
    if((type(filtered_list)==type([1])) and  ((filtered_list.__len__())!=0)):
                query = {
                        "query": 
                    {
                        "bool": 
                        {
                            "filter": 
                            {
                            "terms": {"_id": filtered_list}
                            },
                        "must": [
                                {"match": { "Name": query_name}}
                                ]
                        }
                    }
                }
    else:
        query={"query": {"match" : {"Name" : {"query" : query_name,"analyzer": "my_analyzer"}}}}
    return query
##################################################
def address_query_builder(query_address,filtered_list=None):
    if((type(filtered_list)==type([1])) and  ((filtered_list.__len__())!=0)):
        query ={
        "query": 
        {
        "bool": 
        {
        "filter": 
        {
        "terms": {"_id": filtered_list}
        },
        "must": [
        { "match" : {
        "Address" : {
        "query" : query_address

        }
        }}
        ]
        }
        }
        }
    else:
        query={
        "query": {
        "match" : {
        "Address" : {
        "query" : query_address

        }
        }
        }
        }
    return query
    
#print(NameModule("deepak",lis))
##########################################

###########################################3333
def filter_results(param_query):
    res=es.search(index="sl_list",body=param_query)
    l=[]
    for i in res['hits']['hits']:
        #print(i['_source'])
        l.append(i['_id'])
    return l


def final_query(id_list=None):
    if(id_list.__len__()):
        query ={
        "query": 
        {
        "bool": 
        {
        "filter": 
        {
        "terms": {"_id": id_list}
        }
        }
        }
        }
        final_list=[]
        res=es.search(index="sl_list",body=query)
        for i in res['hits']['hits']:
            #print(i['_source'])
            final_list.append((i['_source'],i['_id']))
    else:
        final_list.append("No Match-Clear")
    return final_list

import pandas as pd
import json
config=json.loads(open("_data/sampleAML.json").read())
blist=pd.read_csv('_data/BankCustList.csv')


for (i,j) in blist.iterrows():
    d = j.to_dict()
    interim_id_list = []
    interim_id_list_count = interim_id_list.__len__()
    next_module = config['FirstModuleToProcess']
    verdict=""
    print("*******************************StartProcesssing"+d['Name']+"***************")
    print(d)
    
    while((next_module!="FalsePositive")or next_module!="RiskEntity"):
        #if((interim_id_list_count==0) and next_module =="Country"):
         #   print("Country-Check")
        #    interim_id_list = filter_results(Country_query_builder(d['Country']))
        #    print(interim_id_list)
            
       # elif((interim_id_list_count!=0) and (next_module =="Country")):
       #     print("Country-Check")
        #    interim_id_list = filter_results(Country_query_builder(d['Country']),interim_id_list)
      #      print(interim_id_list)
            
            
            #=============================#
            
            
        if((interim_id_list_count==0) and next_module =="Address"):
            print("Address-Check")
            interim_id_list = filter_results(address_query_builder(d['Address']))
            print(interim_id_list)
        elif((interim_id_list_count!=0) and (next_module =="Address")):
            print("Address-Check")
            interim_id_list = filter_results(address_query_builder(d['Address']),interim_id_list)
            print(interim_id_list)
    
        if((interim_id_list_count==0) and next_module =="NameModule"):
            print("NameModule")
            interim_id_list = filter_results(Name_query_builder(d['Name']))
            print(interim_id_list)
        elif((interim_id_list_count!=0) and (next_module =="NameModule")):
            print("NameModule")
            interim_id_list = filter_results(Name_query_builder(d['Name']),interim_id_list)
            print(interim_id_list)
    
        if((interim_id_list_count==0) and next_module =="DOB"):
            print("DOBModule")
            #print("exit")
            #break
            interim_id_list = filter_results(dob_query_builder(query_date= d['DOB'],filtered_list=interim_id_list))
            print(interim_id_list)
        elif((interim_id_list_count!=0) and (next_module =="DOB")):
            print("DOBModule")
            interim_id_list = filter_results(dob_query_builder(d['DOB']),interim_id_list)
            print(interim_id_list)
        
        
        if(interim_id_list.__len__()):# ifYes
            verdict =config['Flow'][next_module][0]['IfYes']
            if(verdict in ['Address','DOB','Name','SSN','Country']):
                next_module = verdict
                print("we have hits and is moving to "+next_module)
            elif(verdict=="FalsePositive"):
                print("PErson is clear"+d["Name"])
                break
            elif (verdict=="RiskEntity"):
                print("the person is a risk"+d["Name"])
                print(final_query(interim_id_list))
                break
                #print("we have  hits and the verdict is "+verdict)
        else: # if No ; No hits
            verdict = config['Flow'][next_module][0]['IfNo']
            
            print("we have no hits and the verdict is "+verdict)
            next_module = verdict
            if(verdict=="FalsePositive"):
                print("PErson is clear--->"+d["Name"])
                break
            

#print(verdict)
    
    






























# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 14:03:58 2017

@author: Deepak.Joseph
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Nov 17 17:20:40 2017

@author: Deepak.Joseph
"""


from elasticsearch import Elasticsearch
es = Elasticsearch()


from datetime import datetime
def dob_query_builder(query_date,filtered_list=None):
    #print(filtered_list)
    #print((type(filtered_list)==type([1])) and  (filtered_list.__len__())!=0)
    if( (type(filtered_list)==type([1])) and  ((filtered_list.__len__())!=0)):
        #print("entering start of dob builder")
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
        #print("Entering Else of DOB")
        query = {
                "query":
                    {"bool":
                        {"should":
                            [
                                    {"match":
                                        {"DOB_1":query_date}
                                        },
                                    {"match":
                                        {"DOB_2":query_date
                                        }
                                    }
                            ]
                    }
                    }
                    }
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
        query={"query": {"match" : {"Name" : {"query" : query_name}}}}
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
def Country_query_builder(query_country,filtered_list=None,blacklist_countries=None):
    if((type(filtered_list)==type([1])) and  ((filtered_list.__len__())!=0)):
        query={          "query": {
        
        "bool": {
          
          "filter": {
        "terms": {
          "_id": filtered_list
        }
          },
          "must": [
        {"match": {
          "Country": query_country
        }}
          ]
          
        }
          }
          
          
        }
                   
    else:
        query = {"query": {"bool": {  "must": [{"match": {"Country": query_country}}]}}}

            
    return query

def SSN_query_builder(query_ssn,filtered_list=None):
    if((type(filtered_list)==type([1])) and  ((filtered_list.__len__())!=0)):
        query={
          "query": 
          {
          "bool": 
          {
        "filter": {
          "terms": {
        "_id": filtered_list
          }
        }, 
          "should": [
          { "match" : {
          "SSN" :query_ssn
          
          }
          }
          ]
          }
          }
          
          }
    else:
        query={
        "query": 
        {
        "bool": 
        {
      
        "should": [
        { "match" : {
        "SSN" :query_ssn
        
        }
        }
        ]
        }
        }
        
}
            
    return query
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
config=json.loads(open(r"C:\Project_WB\AML-Project-WB\_data\test\AML_Flow.json").read())
blist = pd.DataFrame(json.load(open(r'C:\Project_WB\AML-Project-WB\_data\test\BankData.json','r')),index={1})
#pd.read_json('_data/test/BankData.json', )
#blist=pd.read_csv('_data/BankCustList.csv')
final_df = []
for (i,j) in blist.iterrows():
    d = j.to_dict()
    interim_id_list = []
    interim_id_list_count = interim_id_list.__len__()
    next_module = config['FirstModueToProcess']
    d['DOB']=datetime.strptime(d['DOB'], "%d/%m/%Y").strftime("%Y/%m/%d")
    verdict=""
    print("*******************************StartProcesssing"+d['Name']+"***************")
    print(d)
    
    while( (next_module!="Clear") and  (next_module!="Escalate")):
        verdict=""
        final_list=[]
        interim_id_list_count = interim_id_list.__len__()
################################################################     
#        if((interim_id_list_count==0) and next_module =="Country"):
#            #continue
#            if(d['Country'] in ['Iraq', 'Palestine']):
#                interim_id_list = []
#            else:
#                print("Country-Check")# if cold start
#
#            #interim_id_list = filter_results(Country_query_builder(d['Country']))
#            #print(interim_id_list)
#            
#        elif((interim_id_list_count!=0) and (next_module =="Country")):
#            #continue
#            if(d['Country'] in ['Iraq', 'Palestine']):
#                interim_id_list = []
#            else:
#            #print("Country-Check")
#            #interim_id_list = filter_results(Country_query_builder(d['Country']),interim_id_list)
#                print(interim_id_list)
#            
#            
#            #=============================#
#        if((interim_id_list.__len__()!=0) and (next_module=='Country')):# ifYes
#            print("the country is not in the sanctioned list")
#            continue
#        elif((interim_id_list.__len__()==0) and next_module=='Country'):
#            print("THe countyr is sanctioned. Risk yser")
            
            #next_module =config['Flow'][next_module][0]['IfNo']
            #print("the person is a "+next_module+d["Name"])
            #if(next_module=="FalsePositive"):
                #print("PErson is clear--->"+d["Name"])
                
            #print(final_query(interim_id_list))
        if((d['Country'] in ['Iraq']) and next_module =="CountryCheck"):
            interim_id_list = []
        else:
            if((interim_id_list_count==0) and next_module =="CountryCheck"):
                print("Country-Check")
                interim_id_list = filter_results(Country_query_builder(d['Country']))
                print(interim_id_list)
            elif((interim_id_list_count!=0) and (next_module =="CountryCheck")):
                print("Country-Check")
                interim_id_list = filter_results(Country_query_builder((d['Country']),interim_id_list))
                print(interim_id_list)     
################################################################            
            
        if((interim_id_list_count==0) and next_module =="AddressCheck"):
            print("Address-Check")
            interim_id_list = filter_results(address_query_builder(d['Address']))
            print(interim_id_list)
        elif((interim_id_list_count!=0) and (next_module =="AddressCheck")):
            print("Address-Check")
            interim_id_list = filter_results(address_query_builder((d['Address']),interim_id_list))
            print(interim_id_list)
    
        if((interim_id_list_count==0) and next_module =="NameCheck"):
            print("NameCheck")
            interim_id_list = filter_results(Name_query_builder(d['Name']))
            print(interim_id_list)
        elif((interim_id_list_count!=0) and (next_module =="NameCheck")):
            print("NameCheck")
            interim_id_list = filter_results(Name_query_builder((d['Name']),interim_id_list))
            print(interim_id_list)
    
        if((interim_id_list_count==0) and next_module =="D.O.B.Check"):
            print("DOBModule")
            #print("exit")
            #break
            
            interim_id_list = filter_results(dob_query_builder(query_date= d['DOB'],filtered_list=interim_id_list))
            print(interim_id_list)
        elif((interim_id_list_count!=0) and (next_module =="D.O.B.Check")):
            print("DOBModule")
            interim_id_list = filter_results(dob_query_builder((d['DOB']),interim_id_list))
            print(interim_id_list)
        
        if((interim_id_list_count==0) and next_module =="SSNCheck"):
            print("SSNCheck")
            #print("exit")
            #break
            interim_id_list = filter_results(SSN_query_builder(query_ssn= d['SSN']))
            print(interim_id_list)
        elif((interim_id_list_count!=0) and (next_module =="SSNCheck")):
            print("SSNCheck")
            interim_id_list = filter_results(SSN_query_builder((d['SSN']),interim_id_list))
        
        
       #SSN_query_builder

            
            #Our Country is not blacklisted
        
        if(interim_id_list.__len__()):# ifYes
            verdict =config['Flow'][next_module][0]['IfYes']
            if(verdict in ['AddressCheck','D.O.B.Check','NameCheck','SSNCheck','CountryCheck']):
                next_module = verdict
                print("we have hits and is moving to "+next_module)
            elif(verdict=="Clear"):
                #print("PErson is clear"+d["Name"])
                break
            elif (verdict=="Escalate"):
                print("the person is a risk"+d["Name"])
                final_list = (final_query(interim_id_list))
                #print( final_list)
                break
                #print("we have  hits and the verdict is "+verdict)
        else: # if No ; No hits
            if(next_module=="Country"):
                verdict = config['Flow'][next_module][0]['IfYes']
                
            else:
                verdict = config['Flow'][next_module][0]['IfNo']
            
            print("we have no hits and the verdict is "+verdict)
            next_module = verdict
            if(verdict=="FalsePositive"):
                print("PErson is clear--->"+d["Name"])
                break
    
    #print(verdict)
    id_list=[]
    #final_data_frame=[]
    if(final_list.__len__()):
        id_list = [j for (i,j) in final_list]#[j for (i,j) in final_list]
    d['verdict'] = verdict
    d['matching_ids'] = str(id_list)
    #print(d)
    resultfile=open(r"C:\Project_WB\AML-Project-WB\_data\test\OutputResult.txt","w+")
    resultfile.write(verdict+'-'+str(id_list))
    resultfile.close()
    print(verdict,'-',str(id_list))
    #final_df.append(d)
    #print("*************************************")
    #print(final_df)


#print("After iteratoin")

#print(final_df)
#pd.DataFrame(final_df).to_csv(open("_data\out.csv",'w'),columns=['Name','Address','Country','SSN','DOB','Sex','verdict','matching_ids'])
#final_dataframe.to_csv(open("_data\est.csv",'w'))
            
            

#print(verdict)
    
    






























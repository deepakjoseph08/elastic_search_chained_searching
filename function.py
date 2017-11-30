# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 12:31:42 2017

@author: Deepak.Joseph
"""

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

def a_Country_query_builder(query_country,filtered_list=None,blacklist_countries=None):
    if((type(filtered_list)==type([1])) and  ((filtered_list.__len__())!=0)):
        query={
        "query": 
        {
        "bool": 
        {
        "filter": 
        {
        "terms": {"_id": filtered_list}
        },
        "must_not": [
        { "match" : {
        "Country" :"Iraq"
        
        }
        }
        ]
        }
        }
        }
    else:
        query={
        "query": {
          "bool": {
          "must_not": [
          {
          "match": {
          "Country": "Iraq USA"
        }
          }]
          
          
        
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

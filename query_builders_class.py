# -*- coding: utf-8 -*-
"""
Created on Fri Nov 24 11:35:02 2017

@author: Deepak.Joseph
"""
from elasticsearch import Elasticsearch
es = Elasticsearch()
import sys

class query_builders_class():
    def __init__(self):
        self.query = ""
        self.filtered_list=[]
        self.l = []
        self.final_list = []
        self.opts = {}
        
    def check_command_line_arguments(self,args):
        self.opts={}
        
        while args:
            if (args[0][0]=="-"):
                if(str(args[0]).replace("-","") in ['Name','DOB','Address','SSN','Country']):
           
                    self.opts[str(args[0]).replace("-","")] = args[1]
                    args = args[1:]
                else:
                    #@#print("Argument listing is corrupted: Please check and execute again. \nParameter Not found:",str(args[0]).replace("-",""))
                    return [False,"Argument listing is corrupted: Please check and execute again. \nParameter Not found:",str(args[0])]
            else:
                args = args[1:]
        return self.opts
        

        
    def final_query(self,id_list=None):
        if(id_list.__len__()):
            self.query ={
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
            self.final_list=[]
            res=es.search(index="sl_list",body=self.query)
            for i in res['hits']['hits']:
                #print(i['_source'])
                self.final_list.append((i['_source'],i['_id']))
        else:
            self.final_list.append("No Match-Clear")
        return self.final_list
    
    
    
    def filter_results(self,param_query):
        res=es.search(index="sl_list",body=param_query)
        self.l=[]
        for i in res['hits']['hits']:
            #print(i['_source'])
            self.l.append(i['_id'])
        return self.l
    def SSN_query_builder(self,query_ssn,filtered_list=None):
        if((type(self.filtered_list)==type([1])) and  ((self.filtered_list.__len__())!=0)):
            self.query={
              "query": 
              {
              "bool": 
              {
            "filter": {
              "terms": {
            "_id": self.filtered_list
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
            self.query={
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
                
        return self.query
    
    def Country_query_builder(self,query_country,filtered_list=None):
        if((type(self.filtered_list)==type([1])) and  ((self.filtered_list.__len__())!=0)):
            self.query ={          "query": {
            
            "bool": {
              
              "filter": {
            "terms": {
              "_id": self.filtered_list
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
            self.query  = {"query": {"bool": {  "must": [{"match": {"Country": query_country}}]}}}
    
                
        return self.query 
    def address_query_builder(self,query_address,filtered_list=None):
        if((type(self.filtered_list)==type([1])) and  ((self.filtered_list.__len__())!=0)):
            self.query ={
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
            self.query={
            "query": {
            "match" : {
            "Address" : {
            "query" : query_address
    
            }
            }
            }
            }
        return self.query
        
    def Name_query_builder(self,query_name,filtered_list=None):
        if((type(self.filtered_list)==type([1])) and  ((self.filtered_list.__len__())!=0)):
                    self.query = {
                            "query": 
                        {
                            "bool": 
                            {
                                "filter": 
                                {
                                "terms": {"_id": self.filtered_list}
                                },
                            "must": [
                                    {"match": { "Name": query_name}}
                                    ]
                            }
                        }
                    }
        else:
            self.query={"query": {"match" : {"Name" : {"query" : query_name}}}}
        return self.query
    def dob_query_builder(self,query_date,filtered_list=None):
        #print(filtered_list)
        #print((type(filtered_list)==type([1])) and  (filtered_list.__len__())!=0)
        if( (type(self.filtered_list)==type([1])) and  ((self.filtered_list.__len__())!=0)):
            #print("entering start of dob builder")
            self.query ={"query": {
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
            self.query = {
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
        return (self.query)
        




# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 17:59:40 2017

@author: Deepak.Joseph
"""
from elasticsearch import Elasticsearch
es = Elasticsearch()
from datetime import datetime
import pandas as pd
import json

class AML():
    def __init__(self,flow_path,input_json):
        self.flow_path = flow_path
        self.input_json = input_json
    
    ####################################
    
    
    
    ##################################################
    
    #print(NameModule("deepak",lis))
    ##########################################
    
    
    
    ###########################################3333
    
    
    
    

    def run():
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
        
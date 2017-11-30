# -*- coding: utf-8 -*-
"""
Created on Fri Nov 24 11:36:01 2017

@author: Deepak.Joseph
"""
import sys
from sys import argv
#mport test_class as tc
from datetime import datetime
import pandas as pd
import json
import query_builders_class


qbco = query_builders_class.query_builders_class()

opts = qbco.check_command_line_arguments(argv)
if(type(opts)==type([1]) and opts[0]==False):
    print(opts[1],opts[2])
    sys.exit()
else:
    config=json.loads(open(r"C:\Project_WB\AML-Project-WB\_data\test\AML_Flow.json").read())
    blist = pd.DataFrame(opts,index={1})
    
    #query_builder_Class_object (qbco)
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
                    interim_id_list = qbco.filter_results(qbco.Country_query_builder(d['Country']))
                    print(interim_id_list)
                elif((interim_id_list_count!=0) and (next_module =="CountryCheck")):
                    print("Country-Check")
                    interim_id_list = qbco.filter_results(qbco.Country_query_builder((d['Country']),interim_id_list))
                    print(interim_id_list)     
    ################################################################            
                
            if((interim_id_list_count==0) and next_module =="AddressCheck"):
                print("Address-Check")
                interim_id_list = qbco.filter_results(qbco.address_query_builder(d['Address']))
                print(interim_id_list)
            elif((interim_id_list_count!=0) and (next_module =="AddressCheck")):
                print("Address-Check")
                interim_id_list = qbco.filter_results(qbco.address_query_builder((d['Address']),interim_id_list))
                print(interim_id_list)
        
            if((interim_id_list_count==0) and next_module =="NameCheck"):
                print("NameCheck")
                interim_id_list = qbco.filter_results(qbco.Name_query_builder(d['Name']))
                print(interim_id_list)
            elif((interim_id_list_count!=0) and (next_module =="NameCheck")):
                print("NameCheck")
                interim_id_list = qbco.filter_results(qbco.Name_query_builder((d['Name']),interim_id_list))
                print(interim_id_list)
        
            if((interim_id_list_count==0) and next_module =="D.O.B.Check"):
                print("DOBModule")
                #print("exit")
                #break
                
                interim_id_list = qbco.filter_results(qbco.dob_query_builder(query_date= d['DOB'],filtered_list=interim_id_list))
                print(interim_id_list)
            elif((interim_id_list_count!=0) and (next_module =="D.O.B.Check")):
                print("DOBModule")
                interim_id_list = qbco.filter_results(qbco.dob_query_builder((d['DOB']),interim_id_list))
                print(interim_id_list)
            
            if((interim_id_list_count==0) and next_module =="SSNCheck"):
                print("SSNCheck")
                #print("exit")
                #break
                interim_id_list = qbco.filter_results(qbco.SSN_query_builder(query_ssn= d['SSN']))
                print(interim_id_list)
            elif((interim_id_list_count!=0) and (next_module =="SSNCheck")):
                print("SSNCheck")
                interim_id_list = qbco.filter_results(qbco.SSN_query_builder((d['SSN']),interim_id_list))
            
            
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
                    final_list = (qbco.final_query(interim_id_list))
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
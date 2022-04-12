# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 09:44:11 2021

@author: Sif-
"""
import requests
from pandas import json_normalize 
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

tableConfig = pd.read_json('tableConfig.json', orient='records')
########################################## connection strings #################################################################

def connection():
    conn = psycopg2.connect(
        host="162.215.212.70",
        database="mddw_db",
        user="postgres",
        password="Iju#YnGmG@Kq")

    engine = create_engine('postgresql://postgres:Iju#YnGmG@Kq@162.215.212.70:5432/mddw_db')

    return conn

##################### create the config file which will store all the credentails for API and database related information #######################
def extraction(url, startDate, endDate, fileType, surveyNumber, dataSource):
    startdate = startDate
    enddate = endDate
    type = "0"
    pagetotal = 1
    data = {'startdate':startdate,'enddate':enddate,'type':type }
    ##############################################################################################################################
    URL = url
    df_final  = pd.DataFrame([])
    response = requests.post(url= URL, data = data)

    json_data = json.loads(response.text)
    df_data=json_data['data']
    next_type = json_data['type']
    pagetotal = json_data['pagetotal']

    print("extracting")
    for x in range(int(pagetotal)):
        data = {'startdate':startdate,'enddate':enddate,'type':type,'pagecurrent':x }
        response = requests.post(url= URL, data = data)
        json_data = json.loads(response.text)
        df_data=json_data['data']
        df = json_normalize(df_data)
        df_final = df_final.append(df)

    fileName = f"{surveyNumber}_{startDate.split(' ')[0]}_{endDate.split(' ')[0]}"
    df_final['survey_id'] = surveyNumber
    df_final['datasource'] = dataSource
    df_final['filename'] = fileName
    if fileType == "CSV":
        df_final.to_csv(fr".\{tableConfig.project_name[0]}\{fileName}.csv",  index=False, sep=',')
    elif fileType == "JSON":
        df_final.to_json(fr".\{tableConfig.project_name[0]}\{fileName}.json",  orient='records')
####################### load the dataframe to table stg_vestergaard_api_data ############################################
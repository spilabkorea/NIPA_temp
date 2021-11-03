from model import reg_prophet_preprocessing, reg_prophet_result
from get_data_v2 import Compressor
from mongodb import connect_mongodb_mahcine_collection, input_data_to_collection

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime as dt
import time
import pytz

import requests
import json
import datetime as dt
import math

from pymongo import MongoClient


if __name__ == "__main__" :
    now_unix = time.time()  ## final.py 실행시간 

    #####----------------- Get Data  -----------------------#####
    compressor_lst = [364]

    for i in compressor_lst :
        # Compressor
        compressor = Compressor(compressorId= i, now_unix=now_unix, minute = 60*24*14, limit=60*60*24*14, print_time=True)

        # 수집할 정보 리스트 설정
        metricName_stat_lst = ['compressorpressure', 'compressortemperature', 'compressorexternaltemperature', 'compressorexternalhumidity', 'grmsx', 'grmsy', 'grmsz']
        metricName_output_lst = ['compressorstatus', 'compressorrunstop']

        # 데이터 수집 및 저장
        df_comp = compressor.compressor_all_data(metricName_stat_lst, metricName_output_lst)
        print('데이터 수집 완료')
        
        
        #####----------------- Regression-Prophet---------------------#####
        df_preprocessed = reg_prophet_preprocessing(df_comp)

        # MinMaxScaler
        scaler = MinMaxScaler()
        df_preprocessed_scaled = scaler.fit_transform(df_preprocessed)
        df_preprocessed_scaled = pd.DataFrame(df_preprocessed_scaled, index=df_preprocessed.index , columns=df_preprocessed.columns)
        # print('데이터 전처리 완료')

        df_result = reg_prophet_result(df_preprocessed_scaled, scaler)
        # print('데이터 훈련 완료')

        # 결과 데이터를 MongoDB 입력을 위해서 전처리
        df_result = df_result.reset_index()
        df_result.rename(columns={'ds' : 'time'}, inplace=True)
        df_result['time'] = df_result['time'].apply(lambda x : int(x.timestamp()*1000))
        df_result['compressorId'] = str(i)
        df_result = df_result[['compressorId', 'time', 'compressorpressure', 'compressortemperature',
                                'compressorexternaltemperature', 'compressorexternalhumidity', 'grmsx',
                                'grmsy', 'grmsz', 'compressorstatus']]


        ####---------------- Input MongoDB -----------------------------####
        collection = connect_mongodb_mahcine_collection('compressor', i, regression=True)
        input_data_to_collection(collection, df_result)



# Library Load
import pandas as pd
import numpy as np
import requests
import json
import datetime as dt
import time
import math
import pytz

# test.py, model.py가 공유하는 시간/ TimeZone 설정
now_unixTime = time.time()

class Compressor():
    
    def __init__(self, compressorId, now_unix, minute, limit, print_time =True) :
        """[summary] compressor에 대한 데이터를 받아오는 클래스

        Args:
            compressorId ([string]): 콤프레셔 Id
            now_unix ([unixtime, int]): 현재 시간(unixtime)
            minute ([int]): 몇 분 전의 데이터를 수집할지 결정
            limit ([int]): 최대 몇개의 데이터를 수집 해 올지 결정
            print_time (bool, optional): 조회 시간에 대한 정보를 출력할지 결정. Defaults to True.
        
        endTime, unix_endTime     : startTime에서 1분단위 내림(ex. 10:55:00 -> 10:50:00)
        startTime, unix_startTime : endTime - (minute * min) (10:50:00 -> 10:40:00) 
        """
        ## Time setting        
        self.now_unix = now_unix
        self.minute = minute
        self.now_time = dt.datetime.fromtimestamp(now_unix)
        self.endTime = dt.datetime(self.now_time.year, self.now_time.month, self.now_time.day, self.now_time.hour, (math.floor(self.now_time.minute/10)*10))
        self.unix_endTime = int(self.endTime.timestamp()*1000)
        self.startTime = self.endTime + dt.timedelta(minutes= -self.minute)
        self.unix_startTime = int(self.startTime.timestamp()*1000)
        if print_time == True :
            print('Crontab get_data.py 실행 시간 : ', self.now_time, self.now_unix)
            print('조회 시작시간 : ', self.startTime, self.unix_startTime)
            print('조회 종료시간 : ', self.endTime, self.unix_endTime)

        # compressorId and limit setting
        self.compressorId = compressorId
        self.limit = limit


    def get_api(self, url, path, metricName) : 
        """[summary] Inforzia REST API 서버에 접근 후 GET요청을 통해서 데이터를 가져오는 함수

        Args:
            url ([string]): 'http://49.254.79.85:8080'
            path ([string]): [description]
            metricName ([string]): 수집할 데이터의 명칭

        Returns:
            [type]: [description]
        """
        '''

        url : 'http://49.254.79.85:8080'
        metricName : 수집할 데이터의 명칭
        unix_startTime, unix_endTime : 데이터 조회 시작, 종료 시점
        limit : 데이터 최대 조회 수 (default = 10000000)
        '''
        # url, path, parameter, header
        parameter = f'?metricName={metricName}&startTime={self.unix_startTime}&endTime={self.unix_endTime}&limit={self.limit}'
        headers = {'Authorization' : 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJuaXBhc3BpbGFiIiwiaWF0IjoxNjMyOTg1ODMxLCJzY29wZXMiOiJhcHAiLCJzeXNpZCI6IjRjMTVmZTM3LWE1ZDctNDkxYy05Y2FjLWQ1MjQ2ZWVmNDg5NiIsImp0aSI6IjJlNWI3OGI5LTRiZmEtNDkwYy05ZjVhLTIyMTQyYWRhNDJlZCJ9.qhQtxhDOVFey1wNvufPDuFH7HIYgk2TvOCAbP7Rq7r98ml_SKz8tyA1ucEo59geB7T4wlPRqfDFR75Us8CsLWg'}
        
        # GET & json -> DF & columns name change
        response = requests.get(url+path+ parameter, headers = headers)
        df_compressor_stat = pd.DataFrame(response.json())
        df_compressor_stat.columns = ['time', metricName]
        return df_compressor_stat

    def get_static_data(self, metricName_stat_lst):
        '''
        compressor 통계 정보를 가져오는 함수.
        '''
        # url, path, metricName_lst setting
        url = 'http://49.254.79.85:8080'
        path = f'/api/factory/compressors/{self.compressorId}/stat'
        
        ### compressor 통계 데이터 수집
        for idx, metricName in enumerate(metricName_stat_lst):
            if idx == 0 :
                df_compressor_stat = self.get_api(url, path, metricName)
            else :
                df_compressor_merge_before = self.get_api(url, path, metricName)
                df_compressor_stat = df_compressor_stat.merge(df_compressor_merge_before, on='time', how='outer')

        return df_compressor_stat


    def get_output_data(self, metricName_output_lst):
        '''
        Compressor 출력 데이터 조회
        '''
        ### url, path, parameter
        url = 'http://49.254.79.85:8080'
        path = f'/api/factory/compressors/{self.compressorId}/output'
        
        ### compressor 통계 출력 수집
        for idx, metricName in enumerate(metricName_output_lst):
            if idx == 0 :
                df_compressor_output = self.get_api(url, path, metricName)
            else :
                df_compressor_merge_before = self.get_api(url, path, metricName)
                df_compressor_output = df_compressor_output.merge(df_compressor_merge_before, on='time', how='outer')

        return df_compressor_output


    def compressor_all_data(self, metricName_stat_lst, metricName_output_lst) :
        '''
        Compressor의 모든 정보를 통합    
        '''
        df_compressor_stat   = self.get_static_data(metricName_stat_lst)
        df_compressor_output = self.get_output_data(metricName_output_lst)

        df_compressor_all = df_compressor_stat.merge(df_compressor_output, on='time', how='outer')
        return df_compressor_all

    def save_to_csv(self, df) :
        '''
        ## 받아온 DataFrame을 csv파일로 저장하는 함수
        machine : compressor or rectifier
        startTime : 조회 시작시간(unixTime) --> 파일 명칭에는 realTime으로 변환해서 입력 
        '''
        file_date = self.startTime.strftime("%Y%m%d_%H%M%S")                                  # unixtime -> time
        df.to_csv(f'./data/comp/compressor_{self.compressorId}_{file_date}.csv', index=False)            # save file to .csv 
        
        print(f'./data/compressor/compressor_{self.compressorId}_{file_date}.csv 저장완료') 



if __name__ == '__main__':
    now_unix = time.time()
    compressor_lst = [364]

    for i in compressor_lst :
        # Compressor
        compressor = Compressor(compressorId= i, now_unix=now_unix, minute = 60*24*50, limit=60*24*50*60, print_time=True)

        # 수집할 정보 리스트 설정
        metricName_stat_lst = ['compressorpressure', 'compressortemperature', 'compressorexternaltemperature', 'compressorexternalhumidity', 'grmsx', 'grmsy', 'grmsz']
        metricName_output_lst = ['compressorstatus', 'compressorrunstop']

        # 데이터 수집 및 저장
        df_compressor = compressor.compressor_all_data(metricName_stat_lst, metricName_output_lst)
        compressor.save_to_csv(df_compressor)

    # Rectifier
    # rectifier = Rectifier(now_unix,  print_time=True)
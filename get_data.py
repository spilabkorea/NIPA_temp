# Library Load
import pandas as pd
import numpy as np
import requests
import json
import datetime as dt
import time
import math

resourcekey_lst = []                                            # {edgeId : edgeId, compressorId : compressorId} 정보를 담는 부분 
edgeId = 2                                                      # edgeId의 수는 적으므로 직접 작성

####------ 시간 설정 관련 부분 ------####
now_unix = time.time()                                          # 현재 시간 출력(unixtime)
now_time = dt.datetime.fromtimestamp(now_unix)                  # unixtime -> time

# endTime : startTime에서 1분단위 내림(ex. 10:55:00 -> 10:50:00)
# startTime : endTime - 10min (10:50:00 -> 10:40:00) 
endTime = dt.datetime(now_time.year, now_time.month, now_time.day, now_time.hour, (math.floor(now_time.minute/10)*10))
unix_endTime = int(endTime.timestamp()*1000)
startTime = endTime+dt.timedelta(minutes=-10)
unix_startTime = int(startTime.timestamp()*1000)
print('Cron get_data.py 실행 시간 : ', now_time, now_unix)
print('조회 시작시간 : ', startTime, unix_startTime)
print('조회 종료시간 : ', endTime, unix_endTime)



####------------데이터 수집 함수 --------------####
def get_hourly_data(url, path, metricName, unix_startTime, unix_endTime, limit) : 
    # 수집 정보 확인
    print(f'{unix_startTime}부터 {unix_endTime}기간의 compressor {metricName}정보 수집시작')
    # parameter, header
    parameter = f'?metricName={metricName}&startTime={unix_startTime}&endTime={unix_endTime}&limit={limit}'
    headers = {'Authorization' : 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJuaXBhc3BpbGFiIiwiaWF0IjoxNjMyOTg1ODMxLCJzY29wZXMiOiJhcHAiLCJzeXNpZCI6IjRjMTVmZTM3LWE1ZDctNDkxYy05Y2FjLWQ1MjQ2ZWVmNDg5NiIsImp0aSI6IjJlNWI3OGI5LTRiZmEtNDkwYy05ZjVhLTIyMTQyYWRhNDJlZCJ9.qhQtxhDOVFey1wNvufPDuFH7HIYgk2TvOCAbP7Rq7r98ml_SKz8tyA1ucEo59geB7T4wlPRqfDFR75Us8CsLWg'}
    # GET
    response = requests.get(url+path+ parameter, headers = headers)
    # json -> DF
    df_compressor_stat = pd.DataFrame(response.json())
    df_compressor_stat.columns = ['time', metricName]
    return df_compressor_stat

####--------- 데이터 저장함수 ------------- ####
def save_to_csv(machine, startTime, df) :
    # machine : comp or rect
    # startTime : unixtime
    # df : 저장할 DataFrame
    filename = startTime.strftime("%Y%m%d_%H%M%S")
    df.to_csv(f'./data/{machine}/{machine}_{filename}.csv', index=False)
    print(f'./data/{machine}/{machine}_{filename}.csv 저장완료')


####---- Compressor 목록 조회 ----####
### URL, path, header, parameter
url = 'http://49.254.79.85:8080'
path = '/api/factory/compressors'
headers = {'Authorization' : 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJuaXBhc3BpbGFiIiwiaWF0IjoxNjMyOTg1ODMxLCJzY29wZXMiOiJhcHAiLCJzeXNpZCI6IjRjMTVmZTM3LWE1ZDctNDkxYy05Y2FjLWQ1MjQ2ZWVmNDg5NiIsImp0aSI6IjJlNWI3OGI5LTRiZmEtNDkwYy05ZjVhLTIyMTQyYWRhNDJlZCJ9.qhQtxhDOVFey1wNvufPDuFH7HIYgk2TvOCAbP7Rq7r98ml_SKz8tyA1ucEo59geB7T4wlPRqfDFR75Us8CsLWg'}
param_item = f'?edgeId={edgeId}'
### GET
response = requests.get(url+path+param_item, headers = headers)
compressorId = response.json()[0]['compressorId']
print(f'edgeId={edgeId} : ', response.json()) 
### json -> DF
compressor_information = pd.DataFrame(response.json()[0], index= [1])
### edgeId, compressor 정보 저장
resourcekey = {'edgeId' : edgeId, 'compressorId' : compressorId}
resourcekey_lst.append(resourcekey)
edgeId = resourcekey['edgeId']
compressorId = resourcekey['compressorId']


####----- 통계 데이터 조회 -----####
### url, path, parameter
url = 'http://49.254.79.85:8080'
path = f'/api/factory/compressors/{compressorId}/stat'
metricName_lst = ['compressorpressure', 'compressortemperature', 'compressorexternaltemperature', 'compressorexternalhumidity', 'grmsx', 'grmsy', 'grmsz']
limit=3600
### 데이터 수집
for idx, metricName in enumerate(metricName_lst):
    if idx == 0 :
        df_compressor_stat = get_hourly_data(url, path, metricName, unix_startTime, unix_endTime, limit)
    else :
        df_compressor_merge_before = get_hourly_data(url, path, metricName, unix_startTime, unix_endTime, limit)
        df_compressor_stat = df_compressor_stat.merge(df_compressor_merge_before, on='time', how='outer')


####----- 출력 데이터 조회 -----####
### url, path, parameter
url = 'http://49.254.79.85:8080'
path = f'/api/factory/compressors/{compressorId}/output'
metricName_lst = ['compressorstatus', 'compressorrunstop']
limit=3600
### 데이터 수집
for idx, metricName in enumerate(metricName_lst):
    if idx == 0 :
        df_compressor_output = get_hourly_data(url, path, metricName, unix_startTime, unix_endTime, limit)
    else :
        df_compressor_merge_before = get_hourly_data(url, path, metricName, unix_startTime, unix_endTime, limit)
        df_compressor_output = df_compressor_output.merge(df_compressor_merge_before, on='time', how='outer')


#### ----- 출력 DataFrame 한개의 DataFrame으로 병합 ----- ####
df_compressor_all = df_compressor_stat.merge(df_compressor_output, on='time', how='outer')
for i in range(len(compressor_information.columns)) :
    df_compressor_all[compressor_information.columns[i]] = compressor_information.values[0][i]

####---- 파일 저장 ----####
save_to_csv('comp', startTime, df_compressor_all)
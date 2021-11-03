import pandas as pd
import numpy as np
from datetime import datetime as dt
import time
import pytz
from fbprophet import Prophet


####--------------------- regression model prophet------------------------------####
def reg_prophet_preprocessing(df) :
    """prophet 학습 전 전처리 함수

    Args:
        df ([Dataframe]): get_data_v2를 통해서 받아온 df를 입력

    Returns:
        [DataFrame]: 전처리된 DF를 출력
    """
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df = df.set_index('time')
    df['compressorstatus'] = df['compressorstatus'].apply(lambda x : True if x != -1 else False)
    df = df[df['compressorstatus']]
    df = df.resample('1Min').mean()
    df_preprocessed = df.dropna()
    return df_preprocessed


def reg_prophet_result(df, scaler):
    """일 단위 prophet regression 모델

    Args:
        df (DataFrame): df_preprocessed, 전처리 된 df를 입력
        scaler ([sklearn-scaler-model]): 적용한 scaler 모델 입력, Min-Max Scaler입력

    Returns:
        [DataFrame]: regression 결과 DF로 출력
    """
    means = df.groupby(pd.Grouper(freq='1D')).mean()
    means = means.dropna()

    total_dict = {}
    for feature in means.columns:
        temp = means[[feature]]
        temp = temp.reset_index()
        temp.columns = ['ds','y']

        m = Prophet()
        m.fit(temp)

        future = m.make_future_dataframe(periods=14)
        forecast = m.predict(future)
        total_dict[feature] = forecast[['ds', 'yhat']]

    total_list = []
    for feature, temp_df in total_dict.items():
        temp_df = temp_df.set_index(['ds'])
        temp_df.columns = [feature]
        total_list.append(temp_df)

    y_pred_df = pd.concat(total_list,axis=1)
    y_pred_df = pd.DataFrame(scaler.inverse_transform(y_pred_df), index= y_pred_df.index, columns= y_pred_df.columns)
    return y_pred_df

























def result_comp(data):
    return data

def result_rect(data):
    return data






# 임시로 넣어1
rect_data =[{'rectifierId':"366", 'time':1602130230000, 'anomaly_score':11.77, 'anomaly':0}, 
            {'rectifierId':"366",  'time':1602130231000, 'anomaly_score':327.66, 'anomaly':1},
            {'rectifierId':"366",  'time':1602130232000, 'anomaly_score':1.324, 'anomaly':0}, 
            {'rectifierId':"366",  'time':1602130233000, 'anomaly_score':0.6446, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130234000, 'anomaly_score':0.6446, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130235000, 'anomaly_score':0.6446, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130236000, 'anomaly_score':0.6446, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130237000, 'anomaly_score':327.6436, 'anomaly':1},
            {'rectifierId':"366",  'time':1602130238000, 'anomaly_score':1.324, 'anomaly':0}, 
            {'rectifierId':"366",  'time':1602130239000, 'anomaly_score':0.6446, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130240000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130241000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130242000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130243000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130244000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130245000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130246000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130247000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130248000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130249000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130250000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130251000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130252000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130253000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130254000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130255000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130256000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130257000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130258000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130259000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130260000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130261000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130262000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130263000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130264000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130265000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130266000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130267000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130268000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130269000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130270000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130271000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130272000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130273000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130274000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130275000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130276000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130277000, 'anomaly_score':0.6426, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130278000, 'anomaly_score':0.6436, 'anomaly':0},
            {'rectifierId':"366",  'time':1602130279000, 'anomaly_score':0.6426, 'anomaly':0}]



comp_data =[{'compressorId':"366", 'time':1602130230000, 'anomaly_score':11.77, 'anomaly':0}, 
            {'compressorId':"366",  'time':1602130231000, 'anomaly_score':327.66, 'anomaly':1},
            {'compressorId':"366",  'time':1602130232000, 'anomaly_score':1.324, 'anomaly':0}, 
            {'compressorId':"366",  'time':1602130233000, 'anomaly_score':0.6446, 'anomaly':0},
            {'compressorId':"366",  'time':1602130234000, 'anomaly_score':0.6446, 'anomaly':0},
            {'compressorId':"366",  'time':1602130235000, 'anomaly_score':0.6446, 'anomaly':0},
            {'compressorId':"366",  'time':1602130236000, 'anomaly_score':0.6446, 'anomaly':0},
            {'compressorId':"366",  'time':1602130237000, 'anomaly_score':327.6436, 'anomaly':1},
            {'compressorId':"366",  'time':1602130238000, 'anomaly_score':1.324, 'anomaly':0}, 
            {'compressorId':"366",  'time':1602130239000, 'anomaly_score':0.6446, 'anomaly':0},
            {'compressorId':"366",  'time':1602130240000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130241000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130242000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130243000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130244000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130245000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130246000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130247000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130248000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130249000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130250000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130251000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130252000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130253000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130254000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130255000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130256000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130257000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130258000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130259000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130260000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130261000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130262000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130263000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130264000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130265000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130266000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130267000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130268000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130269000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130270000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130271000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130272000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130273000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130274000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130275000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130276000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130277000, 'anomaly_score':0.6426, 'anomaly':0},
            {'compressorId':"366",  'time':1602130278000, 'anomaly_score':0.6436, 'anomaly':0},
            {'compressorId':"366",  'time':1602130279000, 'anomaly_score':0.6426, 'anomaly':0}] 
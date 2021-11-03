import pandas as pd
import numpy as pd
from pymongo import MongoClient
import model 


def connect_mongodb_mahcine_collection(machine, machineId, regression=False):
    '''
    MongoDB machine collection에 연결하는 함수
    input
        machine = collection 이름 ('compressor' or 'rectifier'), dtype = str
        machine_id = machine_id : compressorId, rectifierId, dtype = int
    output : collection에 연결된 객체

    '''
    # Client 연결 --> DB 연결 --> compressor collection 연결
    mongo_client = MongoClient("mongodb://nipa:nipa@54.180.30.0")
    nipadb = mongo_client['NIPA']
    
    if regression == True :
        machine = machine+str(machineId)+'_regression' # machine = "compressor364_regression"
    else :
        machine = machine+str(machineId)               # machine = "compressor364", "rectifier365"
    collection = nipadb[machine]
    return collection


def input_data_to_collection(collection, data):
    '''
    연결된 collection에 데이터를 input하는 함수
    collection : collection에 연결된 객체
    data : DataFrame
    '''
    collection.insert_many([row.to_dict() for _, row in data.iterrows()])
    print(f'{collection.name} Data가 입력 되었습니다.')


# reneal

# def input_data_to_collection(collection, data):
#     '''
#     연결된 collection에 데이터를 input하는 함수
#     collection : collection에 연결된 객체
#     data : DataFrame
#     '''
#     data_dict_lst = [row.to_dict() for _, row in data.iterrows()]

#     # update or insert data
#     for data in data_dict_lst :
#         if collection.find_one({'$and' : [{'date' : data['date'], 'sensor_id' : data['sensor_id'] }] }) == None :
#             collection.insert_one(data)
#             # if you don't want --> remove print
#             print(data['date'],data['sensor_id'],  "new data, input new data")
            
#         else :
#             collection.update_one({'date' : data['date'], 'sensor_id' : data['sensor_id']}, {'$set' : {'prediction_usage' : data['prediction_usage']}})
#             # if you don't want --> remove print
#             print(data['date'],data['sensor_id'], "already exist data, change data")


if __name__ == '__main__' :
    
    comp_collection = connect_mongodb_mahcine_collection('compressor', 366)
    data_comp = model.result_comp(model.comp_data)
    input_data_to_collection(comp_collection, data_comp)

    rect_anomaly_collection = connect_mongodb_mahcine_collection('rectifier', 366)
    data_rect_anomaly = model.result_rect(model.rect_data)
    input_data_to_collection(rect_anomaly_collection, data_rect_anomaly)
    


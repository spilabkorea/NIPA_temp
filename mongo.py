import pandas as pd
import numpy as pd
from pymongo import MongoClient
import model 

# MongoDB atlas 연결 --> 상대 계정 정보 + 비밀번호 
mongo_client = MongoClient('mongodb+srv://admin:07MtKCRHfuBNokOUk@nipa.tvum8.mongodb.net/NIPA?retryWrites=true&w=majority')
print(mongo_client)
# DataBase 접근 = 제 DB 이름은 NIPA입니다.
nipadb = mongo_client["NIPA"]
print(nipadb)

# collection 접근 = 제 collection 이름은 compressor364 입니다.
# collection = nipadb['compressor364']

# print(collection.find())
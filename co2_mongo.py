from pymongo import MongoClient
import datetime
import time
import random

def get_db():
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    return client["co2"]

my_db = get_db()

my_collection = my_db["co2_concentration"]

my_second_collection = my_db["average_co2_concentration"]

datas = []

for i in range(3600):
    now = datetime.datetime.now()
    now = str(now)
    concentration = random.randint(200, 1000)
    datas.append({"time": now, "concentration": concentration})

my_collection.insert_many(datas)

while True: 
    now = datetime.datetime.now()
    now = str(now)

    average_1_min = 0
    for item in range(1, 61):
        average_1_min += datas[-item]["concentration"]
    average_1_min = average_1_min/60
    
    average_30_min = 0
    for item in range(1, 1801):
        average_30_min += datas[-item]["concentration"]
    average_30_min = average_30_min/1800

    average_1_hour = 0
    for item in range(1, 3601):
        average_1_hour += datas[-item]["concentration"]
    average_1_hour = average_1_hour/3600

    concentration = random.randint(200, 1000)
    datas = datas[1:]
    datas.append({"time": now, "concentration": concentration})
    my_collection.insert_one({"time": now, "concentration": concentration})
    my_second_collection.insert_one({"time": now, "average_1_min": average_1_min, "average_30_min": average_30_min, "average_1_hour": average_1_hour})
    time.sleep(1)

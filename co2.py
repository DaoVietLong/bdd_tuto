import redis
import time
import datetime
import random

r = redis.Redis(host = "localhost", port = 6379, decode_responses= True)

CO2 = []

for i in range(3600):
    CO2.append(random.randint(300, 500))

while True: 
    now = datetime.datetime.now()
    now = str(now)
    concentration = random.randint(300, 500)
    CO2 = CO2[1:]
    CO2.append(concentration)
    if len(CO2) >= 60:
        average_1 = sum(CO2[-60:])/60
        r.publish(channel = "CO2_1_minute", message = now)  
        r.publish(channel = "CO2_1_minute", message = average_1)
    if len(CO2) >= 1800:
        average_30 = sum(CO2[-1800:])/1800
        r.publish(channel = "CO2_30_minute", message = now)
        r.publish(channel = "CO2_30_minute", message = average_30)
    if len(CO2) >= 3600:
        average_60 = sum(CO2)/3600
        r.publish(channel = "CO2_1_hour", message = now)
        r.publish(channel = "CO2_1_hour", message = average_60)
    time.sleep(1)
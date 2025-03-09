from pymongo import MongoClient
from dateutil import parser
import pymongo


def get_db():
    #define server host
    CONNECTION_STRING = "mongodb://localhost:27017/"
    
    myClient = MongoClient(CONNECTION_STRING)

    return myClient['info']

db_name = get_db()

collection_name = db_name['myinfo']

#find(): return a cursor points to all docs in the collection matching the query
third = collection_name.find({"name": { "$in": ["whoever"]}})

print(*third, sep="\n")

#find_one: return docs of the first element matching query
forth = collection_name.find_one({"name": "whoever"})

print(forth)

collection_name.create_index([("name", pymongo.DESCENDING)])

print(collection_name.index_information())
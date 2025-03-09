from pymongo import MongoClient
from dateutil import parser
def get_db():
    # define server host
    CONNECTION_STRING = "mongodb://localhost:27017/"
    
    myClient = MongoClient(CONNECTION_STRING)

    return myClient['info']
# connect to db or create db if not exist
db_name = get_db()

# connect to a collection or create collection if not exist
collection_name = db_name['myinfo']

# element 1
moi = {
    "_id": 1,
    "name": "Long",
    "age": 20,
    "specialise": "ict"
}

# element 2
copine = {
    "_id": 2,
    "name": "Ha",
    "age": 20,
    "specialise": "commercial"
}

# insert elements to connected collection
collection_name.insert_many([moi, copine])

# define date in text
expiry_date = '2024-10-10T00:00:00.0000Z'

#define date in date format
expiry = parser.parse(expiry_date)

third_one = {
    "name": "whoever",
    "expiry_date": expiry
}

collection_name.insert_one(third_one)

info_details = collection_name.find()

somebody = {
    "_id": "idontcare",
    "name": "someone"
}

collection_name.insert_one(somebody)
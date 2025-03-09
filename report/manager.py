import redis
from redisbloom.client import Client
from pymongo import MongoClient, ASCENDING
import datetime
import bcrypt  # For hashing password
import time
import random

# Function to connect to MongoDB and create database "game_database" if not exist
def get_db():
    CONNECTION_STRING = "mongodb://localhost:27017/"
    myClient = MongoClient(CONNECTION_STRING)
    return myClient["game_database"]

# Connect to RedisBloom and MongoDB
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
rb = Client() 

r.delete("usernames_bloom")
r.delete("ingame_bloom")

my_db = get_db()
player_collection = my_db["players"]
gacha_collection = my_db["gacha_items"]

# Create an index on the 'ingame' field in MongoDB for optimized search
player_collection.create_index([("ingame", ASCENDING)])

def initialize_bloom_filters():
    # Set error rate and capacity based on estimated number of users
    rb.bfCreate("usernames_bloom", errorRate=0.01, capacity=100000000)
    rb.bfCreate("ingame_bloom", errorRate=0.01, capacity=100000000)

def register_player(username, ingame, email, password):
    # Check if the username and ingame are unique in the Bloom filter
    if rb.bfExists("usernames_bloom", username):
        return f"Username '{username}' is already taken. Please choose another."
    if rb.bfExists("ingame_bloom", ingame):
        return f"Ingame name '{ingame}' is already taken. Please choose another."

    # Hash the password for security
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    # Add to Bloom filters and MongoDB if unique
    rb.bfAdd("usernames_bloom", username)
    rb.bfAdd("ingame_bloom", ingame)
    create_player_profile(username, ingame, email, hashed_password)
    return "Player registered successfully!"

def create_player_profile(username, ingame, email, hashed_password):
    # Construct and insert player data into MongoDB
    player_data = {
        "username": username,
        "ingame": ingame,
        "email": email,
        "password": hashed_password,  # Store the hashed password
        "created_at": datetime.datetime.now()
    }
    player_collection.insert_one(player_data)

def get_player_profile(username):
    # Retrieve player profile by username from MongoDB
    player = player_collection.find_one({"username": username})
    return player

def register_player_with_mongodb(username, ingame):
    # Direct MongoDB check for uniqueness (without Redis)
    start_time = time.time()
    
    # Check if username or ingame name already exists in MongoDB
    if player_collection.find_one({"username": username}):
        return f"Username '{username}' is already taken."
    if player_collection.find_one({"ingame": ingame}):
        return f"Ingame name '{ingame}' is already taken."

    # Insert if unique
    player_data = {"username": username, "ingame": ingame}
    player_collection.insert_one(player_data)

    duration = time.time() - start_time
    return f"Player registered (MongoDB-only check) in {duration:.4f} seconds."

def register_player_with_redis(username, ingame):
    # Redis check for uniqueness with Bloom filters
    start_time = time.time()
    
    if rb.bfExists("usernames_bloom", username):
        return f"Username '{username}' is already taken (Redis)."
    if rb.bfExists("ingame_bloom", ingame):
        return f"Ingame name '{ingame}' is already taken (Redis)."

    # Add to Bloom filter and MongoDB
    rb.bfAdd("usernames_bloom", username)
    rb.bfAdd("ingame_bloom", ingame)
    player_data = {"username": username, "ingame": ingame}
    player_collection.insert_one(player_data)

    duration = time.time() - start_time
    return f"Player registered (Redis check) in {duration:.4f} seconds."

def measure_load_test():
    # Initialize Bloom filters in Redis
    rb.bfCreate("usernames_bloom", errorRate=0.01, capacity=1000000)
    rb.bfCreate("ingame_bloom", errorRate=0.01, capacity=1000000)

    print("Starting load test...\n")
    usernames = [f"user{i}" for i in range(1000)]
    ingames = [f"gamer{i}" for i in range(1000)]

    # Measure without Redis
    mongo_times = []
    for username, ingame in zip(usernames, ingames):
        result = register_player_with_mongodb(username, ingame)
        print(result)
        if "in " in result:
            mongo_times.append(float(result.split("in ")[1].split(" seconds")[0]))

    # Measure with Redis
    redis_times = []
    for username, ingame in zip(usernames, ingames):
        result = register_player_with_redis(username, ingame)
        print(result)
        if "in " in result:
            redis_times.append(float(result.split("in ")[1].split(" seconds")[0]))

    # Calculate average times
    if mongo_times:
        avg_mongo_time = sum(mongo_times) / len(mongo_times)
        print(f"\nAverage registration time with MongoDB only: {avg_mongo_time:.4f} seconds.")
    if redis_times:
        avg_redis_time = sum(redis_times) / len(redis_times)
        print(f"Average registration time with Redis check: {avg_redis_time:.4f} seconds.")

    if mongo_times and redis_times:
        print("Redis reduced registration time by {:.2f}%.".format(
            ((avg_mongo_time - avg_redis_time) / avg_mongo_time) * 100
        ))

measure_load_test()

def initialize_gacha_items():
    # Define items with levels and success rates
    gacha_items = [
        {"item_name": "Legendary Sword", "level": "S", "success_rate": 0.01, "description": "A powerful sword with unique abilities."},
        {"item_name": "Epic Bow", "level": "A", "success_rate": 0.1, "description": "A bow with increased accuracy and range."},
        {"item_name": "Common Sword", "level": "B", "success_rate": 0.89, "description": "A basic sword with standard attributes."}
    ]

    # Insert items into the collection
    gacha_collection.insert_many(gacha_items)
    print("Gacha items initialized successfully.")

initialize_gacha_items()

# Function to get item details by ID
def get_item_by_id(item_id):
    """
    Retrieve item details from MongoDB by its ID.

    Parameters:
        - item_id (ObjectId): The unique ID of the gacha item.

    Returns:
        - dict: The item details.
    """
    item = gacha_collection.find_one({"_id": item_id})
    return item if item else "Item not found."

# Function to log the gacha attempt in the player's gacha history
def log_gacha_attempt(username, item_id, item_level):
    """
    Logs a gacha attempt for a given player. If 'gacha_history' does not exist, it initializes it.

    Parameters:
        - username (str): The username of the player.
        - item_id (ObjectId): The ID of the item won from the gacha.
        - item_level (str): The level of the item (e.g., "S", "A", "B").
    """
    # Check if player exists
    player = player_collection.find_one({"username": username})
    if not player:
        print(f"Player '{username}' not found. Gacha attempt not logged.")
        return

    # Initialize 'gacha_history' if it doesn't exist
    if "gacha_history" not in player:
        init_result = player_collection.update_one({"username": username}, {"$set": {"gacha_history": []}})
        if init_result.modified_count == 1:
            print("Initialized gacha_history for player:", username)
        else:
            print("Failed to initialize gacha_history for player:", username)
            return

    # Create the gacha attempt record
    gacha_attempt = {
        "item_id": item_id,
        "item_level": item_level,
        "timestamp": datetime.datetime.now()
    }

    # Append the gacha attempt to 'gacha_history'
    result = player_collection.update_one(
        {"username": username},
        {"$push": {"gacha_history": gacha_attempt}}
    )

    if result.modified_count == 1:
        print("Gacha attempt logged successfully for player:", username)
    else:
        print("Gacha attempt logging failed for player:", username)

# Global counters to track attempts for S and A guarantees
s_attempt_counter = 0
a_attempt_counter = 0

def draw_gacha(username):
    """
    Simulate a gacha draw with success rates, guarantee logic, and automatic logging to gacha_history.

    Parameters:
        - username (str): The username of the player.

    Returns:
        - dict: The selected gacha item with its '_id' and 'level'.
    """
    global s_attempt_counter, a_attempt_counter  # Use global counters to keep track across multiple calls
    
    # Increment the counters
    s_attempt_counter += 1
    a_attempt_counter += 1

    # Guarantee conditions
    if s_attempt_counter >= 90:  # Guarantee S item if 90 attempts have passed
        s_item = gacha_collection.find_one({"level": "S"})
        s_attempt_counter = 0  # Reset S guarantee counter
        # Log the gacha attempt for the player
        log_gacha_attempt(username, s_item["_id"], s_item["level"])
        return {"_id": s_item["_id"], "level": s_item["level"]}
    elif a_attempt_counter >= 10:  # Guarantee A item if 10 attempts have passed
        a_item = gacha_collection.find_one({"level": "A"})
        a_attempt_counter = 0  # Reset A guarantee counter
        # Log the gacha attempt for the player
        log_gacha_attempt(username, a_item["_id"], a_item["level"])
        return {"_id": a_item["_id"], "level": a_item["level"]}

    # Random draw based on success rates
    random_value = random.random()
    cumulative_probability = 0.0
    for item in gacha_collection.find():
        cumulative_probability += item["success_rate"]
        if random_value < cumulative_probability:
            # Reset counters if the player draws an S or A item
            if item["level"] == "S":
                s_attempt_counter = 0
            elif item["level"] == "A":
                a_attempt_counter = 0
            # Log the gacha attempt for the player
            log_gacha_attempt(username, item["_id"], item["level"])
            return {"_id": item["_id"], "level": item["level"]}

    # Default to B item if no other condition is met
    b_item = gacha_collection.find_one({"level": "B"})
    # Log the gacha attempt for the player
    log_gacha_attempt(username, b_item["_id"], b_item["level"])
    return {"_id": b_item["_id"], "level": b_item["level"]}

# Perform gacha draws and log results
username = "user1"
for i in range(100):  # Simulate 10 gacha attempts for easier debugging
    result = draw_gacha(username)

# Fetch player profile to confirm gacha_history was updated
player_profile = get_player_profile("user1")
print("Player Profile with Gacha History:", player_profile)

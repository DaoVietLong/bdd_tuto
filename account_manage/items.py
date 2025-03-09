from pymongo import MongoClient
import random

# Connect to MongoDB and create the gacha database
def get_gacha_db():
    CONNECTION_STRING = "mongodb://localhost:27017/"
    client = MongoClient(CONNECTION_STRING)
    return client["game_database"]

gacha_db = get_gacha_db()
gacha_collection = gacha_db["gacha_items"]

def initialize_gacha_items():
    # Clear any existing data in the collection
    gacha_collection.delete_many({})

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

# Function to log the gacha attempt in the player's gacha history
def log_gacha_attempt(username, item_id, item_level):
    """
    Logs a gacha attempt for a given player.

    Parameters:
        - username (str): The username of the player.
        - item_id (str): The ID of the item won from the gacha.
        - item_level (str): The level of the item (e.g., "S", "A", "B").
    """
    # Find the player document in MongoDB
    player = player_collection.find_one({"username": username})
    if not player:
        return f"Player '{username}' not found."

    # Create gacha attempt record
    gacha_attempt = {
        "item_id": item_id,
        "item_level": item_level,
        "timestamp": datetime.datetime.now()
    }

    # Update player document by appending to gacha_history array
    player_collection.update_one(
        {"username": username},
        {"$push": {"gacha_history": gacha_attempt}}
    )

    return "Gacha attempt logged successfully."

# Example usage of the gacha draw with logging to gacha_history
username = "player1"  # Example player
for i in range(1, 100):  # Simulate 100 gacha attempts
    result = draw_gacha(username)
    item_details = get_item_by_id(result["_id"])
    print(f"Attempt {i}: Won {item_details['item_name']} (Level {item_details['level']})")

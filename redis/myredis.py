import redis
from redisbloom.client import Client
import time
import random

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
rb = Client()
# Load the dictionary data
file_path = "DEM-1_1.csv"
words = []
with open(file_path, newline='', encoding='utf-8') as csvfile:
    for row in csvfile:
        words.append(row)

# Example set of random words for membership tests
sample_words = [words[_] for _ in range(100000)]

r.delete("bloom_filter")
r.delete("word_list")
r.delete("cuckoo_filter")

# 1. Bloom Filter: Add words and test membership
def bloom_filter_test(my_word):
    rb.bfCreate("bloom_filter", errorRate = 0.001, capacity=1000000)

    start_time = time.time()
    for word in words:
        rb.bfAdd("bloom_filter", word)
    rb.bfExists("bloom_filter", my_word)
    bloom_filter_time = time.time() - start_time
    
    print(f"Bloom Filter Test Time: {bloom_filter_time:.2f} seconds")
    return bloom_filter_time    

# 2. List: Add words and test membership
def list_test(my_word):
    start_time = time.time()
    for word in words:
        r.lpush("word_list", word)
    r.exists("word_list", my_word)
    list_time = time.time() - start_time
    
    print(f"List Test Time: {list_time:.2f} seconds")
    return list_time

# 3. Cuckoo Filter: Add words and test membership
def cuckoo_filter_test(my_word):
    rb.cfCreate("cuckoo_filter", capacity=1000000)
    
    start_time = time.time()
    for word in words:
        rb.cfAdd("cuckoo_filter", word)
    rb.cfExists("cuckoo_filter", my_word)
    cuckoo_filter_time = time.time() - start_time
    
    print(f"Cuckoo Filter Test Time: {cuckoo_filter_time:.2f} seconds")
    return cuckoo_filter_time

# 4. JSON: Store and test membership
def json_test(my_word):
    start_time = time.time()
    for word in words:
        r.execute_command("JSON.SET", f"json_dict:{word}", ".", '"1"')  # Set a simple JSON value for each word
    r.execute_command("JSON.GET", f"json_dict:{my_word}")
    json_time = time.time() - start_time
    
    print(f"JSON Test Time: {json_time:.2f} seconds")
    return json_time

# Run tests
my_word = sample_words[random.randint(0, len(sample_words))]
print("Running Bloom Filter Test...")
bloom_filter_test(my_word)
print("Running Cuckoo Filter Test...")
cuckoo_filter_test(my_word)
print("Running JSON Test...")
json_test(my_word)
print("Running List Test...")
list_test(my_word)


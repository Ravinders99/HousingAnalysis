import pymongo
import json

# MongoDB connection setup
client = pymongo.MongoClient("mongodb://localhost:27017/")  # Connect to local MongoDB instance

# Create a database (if it doesn't exist) called 'housing_db'
db = client['housing_db']

# Create a collection (if it doesn't exist) called 'canadian_housing'
collection = db['canadian_housing']

# Load the JSON data
with open("canadian_housing_data_with_anomalies_10MB.json", "r") as f:
    housing_data = json.load(f)

# Insert the data into MongoDB collection
# The data structure is a list of provinces, each with nested areas and other details.
# We use 'insert_many' to insert all data at once.
result = collection.insert_many(housing_data)

# Check how many documents were inserted
print(f"Inserted {len(result.inserted_ids)} documents into MongoDB collection.")

# Verify by counting the documents in the collection
doc_count = collection.count_documents({})
print(f"Total documents in the collection: {doc_count}")

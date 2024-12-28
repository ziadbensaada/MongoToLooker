from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
import json
from nltk.stem import WordNetLemmatizer

# Initialize the lemmatizer for singularizing words
lemmatizer = WordNetLemmatizer()

# MongoDB Connection String
connection_string = "mongodb+srv://laamiriouail:XrgeZ1PTd01phHVB@cluster0.5kdba.mongodb.net/articles_bi?retryWrites=true&w=majority&appName=Cluster0"

try:
    # Connect to MongoDB
    client = MongoClient(connection_string)
    client.admin.command('ping')  # Test the connection
    print("Connected to MongoDB!")

    # Access the database and collection
    db = client["articles_bi"]
    collection = db['articles']
    
    # Update documents where "universities" field is missing
    # result = collection.update_many(
    #     {"universities": {"$exists": False}},  # Match documents without the "universities" field
    #     {"$set": {"universities": ["Private School"]}}  # Set "universities" to ["Ecole Privee"]
    # )
    # Retrieve all documents
    documents = collection.find()
    documents_list = list(documents)
    
    # # Save the data to a JSON file
    # with open("articles_data.json", "w") as file:
    #     json.dump(documents_list, file, default=str, indent=4)
    # print("Data saved to 'articles_data.json'.")
    
    # Read data from articles_data.json
    with open("articles_data.json", "r") as file:
        documents_list = json.load(file)
    # Update documents in MongoDB
    for record in documents_list:
        if "_id" in record:
            # Convert `_id` string back to ObjectId
            record["_id"] = ObjectId(record["_id"])
            
            # Update the document based on `_id`
            result = collection.replace_one({"_id": record["_id"]}, record)
            if result.matched_count > 0:
                print(f"Document with _id {record['_id']} updated.")
            else:
                print(f"No matching document found for _id {record['_id']}.")
        else:
            print("Record missing '_id', skipping update.")

    print("All documents updated in MongoDB.")

except ConnectionFailure as e:
    print(f"Could not connect to MongoDB: {e}")
except Exception as e:
    print(f"An error occurred: {e}")

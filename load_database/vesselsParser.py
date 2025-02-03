import pandas as pd
import numpy as np
from pymongo import MongoClient
import json
import yaml

# Load configuration from YAML file
def load_config(config_path: str) -> dict:
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

# Check if document size exceeds 16MB and split if necessary
def split_documents(documents: list) -> list:
    # Define the max size for a document (16MB in bytes)
    MAX_SIZE = 16 * 1024 * 1024  # 16MB
    
    split_docs = []
    current_doc = []
    current_size = 0
    
    for document in documents:
        doc_size = len(json.dumps(document).encode('utf-8'))  # Calculate document size in bytes
        if current_size + doc_size > MAX_SIZE:
            # If adding this document exceeds the max size, push the current document and reset
            split_docs.append(current_doc)
            current_doc = [document]
            current_size = doc_size
        else:
            # Add the document to the current batch
            current_doc.append(document)
            current_size += doc_size
    
    # Add any remaining documents
    if current_doc:
        split_docs.append(current_doc)
    
    return split_docs

# Insert data into MongoDB
def insert_data_to_mongo(collection, data: list):
    # Split documents if they exceed the 16MB size limit
    split_docs = split_documents(data)
    
    for batch in split_docs:
        try:
            result = collection.insert_many(batch, ordered=False)
            print(f"Inserted {len(result.inserted_ids)} documents.")
        except Exception as e:
            print(f"An error occurred during insertion: {e}")

# Function to process the vessel data and insert it into MongoDB
def process_vessel_data(vessel_data_path: str, type_codes_path: str, collection):
    # Load and clean raw data
    vessels_df = pd.read_csv(vessel_data_path)
    types_df = pd.read_csv(type_codes_path)

    # Ensure same data type on vessels "shiptype" with types "type_code"
    vessels_df["shiptype"] = vessels_df["shiptype"].fillna(0).astype(int)
    
    # Ensure proper column naming
    types_df.columns = ["type_code", "description"]
    
    # Map type codes to descriptions
    type_code_to_description = dict(zip(types_df["type_code"].astype(str), types_df["description"]))
    
    # Add the 'description' column to the vessels DataFrame
    vessels_df["type_code"] = vessels_df["shiptype"].astype(str)  # Ensure type_code is string  
    vessels_df["description"] = vessels_df["type_code"].map(type_code_to_description)
    vessels_df["type_code"] = vessels_df["shiptype"] # turn the field again into an int 
    
    # Select and reorder columns to match MongoDB schema
    mongo_data = vessels_df[["vessel_id", "country", "type_code", "description"]]
    
    # Convert the DataFrame to a list of dictionaries for MongoDB insertion
    mongo_data_list = mongo_data.to_dict(orient="records")

    # Insert data into MongoDB
    insert_data_to_mongo(collection, mongo_data_list)

# Main execution function
def main():
    # Load config from YAML file
    config = load_config("vessel.yaml")

    # Connect to MongoDB
    client = MongoClient(config["mongo_uri"])
    db = client[config["database"]]
    collection = db[config["collection"]]

    # Process vessel data and insert into MongoDB
    process_vessel_data(config["vessel_data_path"], config["type_codes_path"], collection)
    client.close()
    
if __name__ == "__main__":
    main()

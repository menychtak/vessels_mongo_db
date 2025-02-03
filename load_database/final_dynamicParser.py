import pandas as pd
from pymongo import MongoClient
import yaml
from typing import Dict, List
from datetime import timedelta
from bson import BSON
import time

# Load configuration
def load_config(config_path: str) -> Dict:
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

# Connect to MongoDB
def connect_to_mongo(uri: str, database: str, collection: str):
    client = MongoClient(uri)
    db = client[database]
    return db[collection]

# Load and clean raw data
def load_data(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def insert_data_to_mongo(collection, data: List[Dict]):
    """
    Insert data into the MongoDB collection using insert_many.
    
    Args:
        collection (pymongo.collection.Collection): MongoDB collection.
        data (List[Dict]): List of data dictionaries to insert.
    """
    try:
        result = collection.insert_many(data, ordered=False)  # Set ordered=False for better performance
        print(f"Inserted {len(result.inserted_ids)} documents.")
    except Exception as e:
        print(f"An error occurred during insertion: {e}")

# Check and split large documents
def split_large_documents(doc, max_doc_size=16 * 1024 * 1024):
    doc_size = len(BSON.encode(doc))
    if doc_size > max_doc_size:
        positions = doc.pop('positions')
        chunk_size = len(positions) // (doc_size // max_doc_size + 1)
        return [dict(doc, **{
            "_id": f"{doc['_id']}_chunk_{i // chunk_size}",
            "positions": positions[i:i + chunk_size],
        }) for i in range(0, len(positions), chunk_size)]
    return [doc]

# Create bucketed documents with fixed 1-hour buckets
def create_hourly_buckets_for(df, max_doc_size=16 * 1024 * 1024):
    df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
    df['bucket'] = df['timestamp'].dt.floor('1h')

    documents = []
    
    # Group by vessel_id and bucket manually
    grouped = df.groupby(['vessel_id', 'bucket'])
    for (vessel_id, bucket), group in grouped:
        positions = []
        for _, row in group.iterrows():
            positions.append({
                "timestamp": row['timestamp'].isoformat(),
                "geometry": {
                    "type": "Point",
                    "coordinates": [row['lon'], row['lat']],
                },
                "speed": row['speed'],
                "heading": row['heading'],
                "course": row['course'],
            })

        document = {
            "_id": f"{vessel_id}_{bucket.isoformat()}",
            "vessel_id": vessel_id,
            "timestamp_start": bucket.isoformat(),
            "timestamp_end": (bucket + timedelta(hours=1) - timedelta(seconds=1)).isoformat(),
            "positions": positions
        }
        documents.extend(split_large_documents(document, max_doc_size))

    return documents

def create_hourly_buckets(df, max_doc_size=16 * 1024 * 1024):
    # No need to convert timestamp again, it's already handled before passing to this function.
    
    # Define 1-hour buckets using 'h' instead of 'H'
    df['bucket'] = df['timestamp'].dt.floor('1h')

    # Group by vessel_id and hourly bucket
    grouped = df.groupby(['vessel_id', 'bucket'])
    documents = grouped.apply(lambda group: {
        "vessel_id": group['vessel_id'].iloc[0],
        "timestamp_start": group['bucket'].iloc[0],
        "timestamp_end": (group['bucket'].iloc[0] + timedelta(hours=1) - timedelta(seconds=1)),
        "positions": group[['timestamp', 'lon', 'lat', 'speed', 'heading', 'course']].apply(
            lambda row: {
                "timestamp": row['timestamp'],  # Keep as datetime object for MongoDB ISODate
                "geometry": {
                    "type": "Point",
                    "coordinates": [row['lon'], row['lat']],
                },
                "speed": row['speed'],
                "heading": row['heading'],
                "course": row['course'],
            },
            axis=1,
        ).tolist()
    }).tolist()

    # Split large documents if necessary
    documents = [doc for d in documents for doc in split_large_documents(d, max_doc_size)]
    return documents

# Main execution
def main():

    start_time = time.time()  # Start the timer

    # Load configuration
    config = load_config("dynamic_config.yaml")
    collection = connect_to_mongo(config["mongo_uri"], config["database"], config["collection"])
    
    # Iterate over all files in the configuration
    for file_entry in config["files"]:
        file_path = file_entry["file_path"]
        print(f"Processing file: {file_path}")
        try:
            # Load raw data
            dynamic_df = load_data(file_path)
            # dynamic_df['timestamp'] = pd.to_datetime(dynamic_df['t'], unit='ms')
            if 't' in dynamic_df.columns:
                dynamic_df['timestamp'] = pd.to_datetime(dynamic_df['t'], unit='ms')
            elif 'timestamp' in dynamic_df.columns:
                dynamic_df['timestamp'] = pd.to_datetime(dynamic_df['timestamp'], unit='ms')
            else:
                raise ValueError("Neither 't' nor 'timestamp' column found in the dataset.")

            # Create documents with fixed 1-hour buckets
            documents = create_hourly_buckets(dynamic_df)

            # Insert documents into MongoDB
            insert_data_to_mongo(collection, documents)

            print(f"Successfully processed and inserted data from {file_path}")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    end_time = time.time()  # End the timer 12.06
    print(f"Total Execution Time: {end_time - start_time:.2f} seconds")  # Print elapsed time


if __name__ == "__main__":
    main()
# Execution Time: 1155.21 seconds with apply. 


import geopandas as gpd
import pandas as pd
from shapely.geometry import mapping
from pymongo import MongoClient
from bson import BSON
import time
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
        doc_size = len(BSON.encode(document)) #len(json.dumps(document).encode('utf-8'))  # Calculate document size in bytes
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

# Define file paths from YAML
def define_file_paths(config):
    return config["file_paths"]

# Connect to MongoDB
def mongo_connect(config):
    # Connect to MongoDB using credentials from the YAML file
    client = MongoClient(config["mongo_uri"])
    db = client[config["database"]]
    collection = db[config["collection"]]
    return client, collection

# Parse and insert data from shapefiles
def parse_insert(file_paths, collection):
    # Merge month files into one geodataframe
    combined_gdf = gpd.GeoDataFrame()
    for file in file_paths:
        # Parse the file
        gdf = gpd.read_file(file, encoding='ISO-8859-1')  # Encoding of .cpg file

        # Properly define timestamp columns
        gdf['timestamp'] = pd.to_datetime(gdf['timestamp'])
        #gdf['timestamp'] = gdf['timestamp']#.apply(lambda time: time.isoformat())
        #gdf['timestamp_'] = pd.to_datetime(gdf['timestamp_'], unit='s')  # UNIX timestamp in seconds
        #gdf['timestamp_'] = gdf['timestamp_']#.apply(lambda time: time.isoformat())

        # Stack the geodataframes
        combined_gdf = pd.concat([combined_gdf, gdf], ignore_index=True)

    # Drop unwanted columns
    combined_gdf.drop(columns=['lon', 'lat'], inplace=True)

    # Group the data based on timestamp range
    combined_gdf['timestamp_start'] = combined_gdf['timestamp'].min()
    combined_gdf['timestamp_end'] = combined_gdf['timestamp'].max()
    grouped = combined_gdf.groupby(['geometry', 'timestamp_start', 'timestamp_end'])

    # Create a bucket-pattern list of dictionaries
    bucket_doc = []
    for (geometry, timestamp_start, timestamp_end), group in grouped:
        measurements = group.drop(columns= ['geometry', 'timestamp_start', 'timestamp_end']) # exclude unwanted from measurements
        bucket = {
            'geometry': mapping(geometry),  # Ensure geometry is in GeoJSON format
            'timestamp_start': timestamp_start,
            'timestamp_end': timestamp_end,
            'measurements': measurements.to_dict(orient='records')
        }
        bucket_doc.append(bucket)

    # Insert the documents into MongoDB
    insert_data_to_mongo(collection, bucket_doc)

    return len(bucket_doc)

# Main execution function
def main():
    start = time.time()
    
    # Load config from YAML file
    config = load_config("weather_config.yaml")

    # Define the file paths
    file_paths = define_file_paths(config)

    # Connect to MongoDB
    client, collection = mongo_connect(config)

    # Parse the files and insert final documents to MongoDB
    total_inserts = 0
    for file_path_quarter in file_paths:
        inserts = parse_insert(file_path_quarter, collection)  # Each iteration is a year's quarter (3 files/iteration)
        total_inserts += inserts

    client.close()  # Close MongoDB connection
    print('--------------')
    print(f'Total documents inserted: {total_inserts}.')
    print(f'Executed in {time.time() - start:.4f} seconds.')

if __name__ == '__main__':
    main()
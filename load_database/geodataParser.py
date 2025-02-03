import geopandas as gpd
from pymongo import MongoClient, InsertOne
from shapely.geometry import mapping
import yaml
import time

def load_config(config_path: str) -> dict:
    with open(config_path, "r") as file:
        return yaml.safe_load(file)
    
def create_documents(gdf):
    # convert gdf to a list of dictionaries. Creates one record per document
    documents = gdf.loc[:, ~gdf.columns.isin(['lon', 'lat'])].to_dict(orient='records')
    
    # map properly 'geometry' column
    for doc in documents:
        doc['geometry'] = mapping(doc['geometry'])
    return documents

def parse_file(file_path, encoding):
    gdf = gpd.read_file(file_path, encoding= encoding)
    gdf.columns = gdf.columns.str.lower() #lowercase all columns names
    
    # Add 'loc_type' column based on the location type
    if "harbours" in file_path:
        gdf.insert(0, 'loc_type', 'harbour') 
        gdf.rename(columns={'port name': 'port_name' }, inplace=True) #rename column 'port name'
    elif "islands" in file_path:
        gdf.insert(0, 'loc_type', 'island')
    elif "piraeus_port" in file_path:
        gdf.insert(0, 'loc_type', 'piraeus port') 
    elif "receiver_location" in file_path:
        gdf.insert(0, 'loc_type', 'receiver')
    elif "regions" in file_path:
        gdf.insert(0, 'loc_type', 'region')
    elif "spatial_coverage" in file_path:
        gdf.insert(0, 'loc_type', 'spatial coverage') 
    elif "territorial_waters" in file_path:
        gdf.insert(0, 'loc_type', 'territorial waters')

    return create_documents(gdf)

def geodata_insert(documents, collection):
    # Insert documents to collection
    try:
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} documents of type '{documents[0]['loc_type']}'.")
        return len(result.inserted_ids)
    except Exception as e:
        print(f'An error occured during insertion: {e}')
        return 0

def main():
    # Start time
    start = time.time()

    # Load config from YAML file
    config = load_config("geo_config.yaml")

    # Connect to MongoDB
    client = MongoClient(config["mongo_uri"])
    db = client[config["database"]]
    collection = db[config["collection"]]

    total_inserts = 0 
    # Process each shapefile specified in the config
    for shapefile_config in config["shapefiles"]:
        file_path = shapefile_config["file_path"]
        encoding = shapefile_config["encoding"]
        print(f"Processing shapefile: {file_path} ...")

        documents = parse_file(file_path, encoding)
        inserts = geodata_insert(documents, collection)
        total_inserts += inserts

    client.close()
    # Total count of inserts
    print('---------------------------------------')
    print(f'Inserted {total_inserts} documents in total.')
    
    # Execution time
    print(f'Executed in {time.time() - start:.4f} seconds.')

if __name__=='__main__':
    main()
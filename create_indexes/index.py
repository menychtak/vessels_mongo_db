from pymongo import MongoClient


def mongo_connect():
    """
    Connect to the MongoDB instance and return the database and client.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client.mongo_db_project
    return db, client

def list_indexes(db, collection_name):
    collection = db[collection_name]
    indexes = collection.index_information()
    for name, index in indexes.items():
        print(f"Index Name: {name}, Index Info: {index}")

def delete_all_indexes_except_id(db, collection_name):
    """
    Deletes all indexes in a collection except the default _id index.
    """
    collection = db[collection_name]
    indexes = collection.index_information()
    
    for name in indexes:
        if name != "_id_":  # Keep the default _id index
            collection.drop_index(name)
            print(f"Dropped index: {name}")
    print("All non-_id indexes have been deleted.")

def create_indexes(db, collection_name, columns, order="ascending"):
    """
    Creates individual indexes for each column provided.

    Args:
        db: MongoDB database connection.
        collection_name: Name of the collection.
        columns: List of column names for which indexes will be created.
        order: "ascending" (default) or "descending" for index order.
    """
    try:
        collection = db[collection_name]
        direction = 1 if order == "ascending" else -1

        for column in columns:
            index_name = collection.create_index([(column, direction)])
            print(f"Index created for column: {column}, Order: {order}, Index Name: {index_name}")
    except Exception as e:
        print(f"Error creating indexes: {e}")

def create_geo_index(db, collection_name, field_name="positions.geometry"):
    """
    Creates a 2dsphere index for geospatial queries on the specified field.

    Args:
        db: MongoDB database connection.
        collection_name: Name of the collection.
        field_name: The geospatial field to index (default: "positions.geometry").
    """
    try:
        collection = db[collection_name]
        index_name = collection.create_index([(field_name, "2dsphere")])
        print(f"2dsphere Index created on: {field_name}, Index Name: {index_name}")
    except Exception as e:
        print(f"Error creating geospatial index: {e}")

def create_compound_index(db, collection_name, columns, orders):
    """
    Creates a compound index based on the provided columns and their orders.

    Args:
        db: MongoDB database connection.
        collection_name: Name of the collection.
        columns: List of column names for the compound index.
        orders: List of corresponding orders ("ascending" or "descending") for each column.

    Raises:
        ValueError: If the number of columns and orders do not match.
    """
    if len(columns) != len(orders):
        raise ValueError("The number of columns must match the number of orders.")

    collection = db[collection_name]
    index_spec = [
        (column, 1 if order == "ascending" else -1)
        for column, order in zip(columns, orders)
    ]

    collection.create_index(index_spec)
    print(f"Compound index created for columns: {columns} with orders: {orders}")

def main():
    collection_vessels = "vessels_collection"
    collection_dynamic = "dynamic_collection"
    collection_geodata = "geodata_collection" 
    collection_weather = "weather_collection" 

    db, client = mongo_connect()
    print("Checking existing indexes...")

    list_indexes(db, collection_vessels)
    delete_all_indexes_except_id(db, collection_vessels)
    list_indexes(db, collection_dynamic)
    delete_all_indexes_except_id(db, collection_dynamic)
    list_indexes(db, collection_geodata)
    delete_all_indexes_except_id(db, collection_geodata)
    list_indexes(db, collection_weather)
    delete_all_indexes_except_id(db, collection_weather)

    # create indexes
    create_compound_index(
        db,
        collection_vessels,
        ["country", "ascending"],
        ["ascending", "ascending"]
    )

    create_geo_index(db, collection_dynamic, "positions.geometry")
    create_indexes(db, collection_geodata , "loc_type")
    create_indexes(db, collection_weather , "timestamp_start")
    create_indexes(db, collection_weather , "timestamp_end")

    print("Final list of Indexes on ", collection_vessels)
    list_indexes(db, collection_vessels)
    print("Final list of Indexes on ", collection_dynamic)
    list_indexes(db, collection_dynamic)
    print("Final list of Indexes on ", collection_geodata)
    list_indexes(db, collection_geodata)
    print("Final list of Indexes on ", collection_weather)
    list_indexes(db, collection_weather)

    client.close()


if __name__ == "__main__":
    main()

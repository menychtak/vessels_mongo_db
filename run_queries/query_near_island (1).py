from pymongo import MongoClient, GEOSPHERE
from shapely.geometry import Polygon
from bson import json_util
import time
import json


def mongo_connect():
    """
    Connect to the MongoDB instance and return the database and client.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client.mongo_db_project
    return db, client


def ensure_geospatial_index_on_positions(db):
    """
    Ensure a geospatial index is created on the `positions.geometry` field of the `dynamic_collection` collection.
    """
    collection = db.dynamic_collection
    try:
        indexes = collection.index_information()
        if not any("positions.geometry" in index["key"][0] and index["key"][0][1] == "2dsphere" for index in indexes.values()):
            print("Creating geospatial index on `positions.geometry` field...")
            collection.create_index([("positions.geometry", GEOSPHERE)])
            print("Geospatial index created successfully.")
        else:
            print("Geospatial index already exists on `positions.geometry`.")
    except Exception as e:
        print(f"Error creating geospatial index: {e}")


def close_polygon(coordinates):
    """
    Ensure that a polygon's coordinates are closed (i.e., the last point matches the first point).
    """
    if coordinates[0] != coordinates[-1]:
        coordinates.append(coordinates[0])  # Close the polygon
    return coordinates

# not used / unoptimized query kept for documentation purposes
# def find_islands_with_vessels(db, radius, start_time=None, end_time=None):
#     """
#     Find islands (`fid`) that have vessels within a specified radius.
#     """
#     island_collection = db.geodata_collection
#     vessel_collection = db.dynamic_collection

#     fids = island_collection.distinct("fid", {"loc_type": "island"})
#     print(f"Found {len(fids)} islands in the database.")

#     islands_with_vessels = []

#     for fid in fids:
#         island_doc = island_collection.find_one({"loc_type": "island", "fid": fid})
#         if not island_doc:
#             print(f"No island found with FID {fid}.")
#             continue

#         geometry_type = island_doc['geometry']['type']
#         geometry_coordinates = island_doc['geometry']['coordinates']

#         if geometry_type != "Polygon":
#             print(f"The geometry type of the island with FID {fid} is not a Polygon.")
#             continue

#         # Close the polygon
#         geometry_coordinates[0] = close_polygon(geometry_coordinates[0])

#         polygon = Polygon(geometry_coordinates[0])
#         if not polygon.is_valid:
#             print(f"Polygon for island FID {fid} is invalid even after closing.")
#             continue

#         centroid = polygon.centroid
#         centroid_coords = [centroid.x, centroid.y]

#         geo_query = {
#             "$geoNear": {
#                 "near": {"type": "Point", "coordinates": centroid_coords},
#                 "distanceField": "distance.calculated",
#                 "maxDistance": radius,
#                 "spherical": True
#             }
#         }

#         pipeline = [geo_query]
#         if start_time and end_time:
#             timestamp_filter = {
#                 "$match": {
#                     "timestamp": {
#                         "$gte": start_time,
#                         "$lte": end_time
#                     }
#                 }
#             }
#             pipeline.append(timestamp_filter)

#         cursor = vessel_collection.aggregate(pipeline)
#         results = list(cursor)

#         if results:
#             print(f"Island with FID {fid} has {len(results)} vessel(s) within {radius} meters.")
#             islands_with_vessels.append(fid)
#         else:
#             print(f"Island with FID {fid} has no vessels within {radius} meters.")

#     return islands_with_vessels

# optimized code: it calculates and stores the centroid in the collection documents
def find_islands_with_vessels(db, radius, start_time=None, end_time=None):
    """
    Find islands (`fid`) that have vessels within a specified radius.
    Optimized to minimize redundant computations and memory usage.
    
    Args:
        db: MongoDB database connection.
        radius (int): Radius in meters to check for vessels.
        start_time (datetime, optional): Start of the time range for filtering.
        end_time (datetime, optional): End of the time range for filtering.
    
    Returns:
        List[int]: List of FIDs of islands with vessels within the specified radius.
    """
    island_collection = db.geodata_collection
    vessel_collection = db.dynamic_collection

    # Fetch all FIDs for islands
    fids = island_collection.distinct("fid", {"loc_type": "island"})
    print(f"Found {len(fids)} islands in the database.")

    islands_with_vessels = []

    # Construct time filter for the $geoNear query if applicable
    time_filter = {}
    if start_time and end_time:
        time_filter = {
            "timestamp": {
                "$gte": start_time,
                "$lte": end_time
            }
        }

    for fid in fids:
        # Fetch island document by FID
        island_doc = island_collection.find_one({"loc_type": "island", "fid": fid})
        if not island_doc:
            print(f"No island found with FID {fid}.")
            continue

        geometry_type = island_doc['geometry']['type']
        geometry_coordinates = island_doc['geometry']['coordinates']

        if geometry_type != "Polygon":
            print(f"The geometry type of the island with FID {fid} is not a Polygon.")
            continue

        # Check if centroid is precomputed and stored in the database
        centroid_coords = island_doc.get("centroid")
        if not centroid_coords:
            # Compute centroid if not stored
            geometry_coordinates[0] = close_polygon(geometry_coordinates[0])
            polygon = Polygon(geometry_coordinates[0])
            if not polygon.is_valid:
                print(f"Polygon for island FID {fid} is invalid even after closing.")
                continue
            
            centroid = polygon.centroid
            centroid_coords = [centroid.x, centroid.y]
            
            # Store computed centroid in the database
            island_collection.update_one({"_id": island_doc["_id"]}, {"$set": {"centroid": centroid_coords}})
            print(f"Stored centroid for island FID {fid}: {centroid_coords}")

        # Use $geoNear with time filtering directly in query parameter
        geo_query = {
            "$geoNear": {
                "near": {"type": "Point", "coordinates": centroid_coords},
                "distanceField": "distance.calculated",
                "maxDistance": radius,
                "spherical": True,
                "query": time_filter
            }
        }

        # Add a $limit stage to the aggregation pipeline
        pipeline = [geo_query, {"$limit": 1}]
        cursor = vessel_collection.aggregate(pipeline)
        results = list(cursor)  # Convert cursor to a list

        if results:
            # print(f"Island with FID {fid} has at least one vessel within {radius} meters.")
            islands_with_vessels.append(fid)
        else:
            print(f"Island with FID {fid} has no vessels within {radius} meters.")

    return islands_with_vessels


def query_vessels_near_island(db, fid, radius, start_time=None, end_time=None):
    """
    Find vessels within a specified radius from the centroid of an island
    and return their exact distance from the centroid.
    """
    island_collection = db.geodata_collection
    vessel_collection = db.dynamic_collection

    island_doc = island_collection.find_one({"loc_type": "island", "fid": fid})
    if not island_doc:
        print(f"No island found with FID {fid}.")
        return

    geometry_type = island_doc['geometry']['type']
    geometry_coordinates = island_doc['geometry']['coordinates']

    if geometry_type != "Polygon":
        print("The geometry type of the island is not a Polygon.")
        return

    # Close the polygon
    geometry_coordinates[0] = close_polygon(geometry_coordinates[0])

    polygon = Polygon(geometry_coordinates[0])
    if not polygon.is_valid:
        print(f"Polygon for island FID {fid} is invalid even after closing.")
        return

    centroid = polygon.centroid
    centroid_coords = [centroid.x, centroid.y]

    print(f"Centroid of island (FID={fid}): {centroid_coords}")

    geo_query = {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": centroid_coords},
            "distanceField": "distance.calculated",
            "maxDistance": radius,
            "spherical": True
        }
    }

    pipeline = [geo_query]
    if start_time and end_time:
        timestamp_filter = {
            "$match": {
                "timestamp": {
                    "$gte": start_time,
                    "$lte": end_time
                }
            }
        }
        pipeline.append(timestamp_filter)

    print("Executing query...")
    start = time.time()
    cursor = vessel_collection.aggregate(pipeline)
    end = time.time()

    results = list(cursor)
    print(f"Query executed in {end - start:.2f} seconds. Found {len(results)} vessels.")

    # Display vessel distances
    vessel_distances = []
    for vessel in results:
        distance = vessel['distance']['calculated']
        vessel_id = vessel['_id']
        vessel_distances.append({"vessel_id": vessel_id, "distance": distance})
        print(f"Vessel ID: {vessel_id}, Distance from centroid: {distance:.2f} meters")

def find_closest_vessels_per_island(db, max_vessels=1, radius_step=1000, max_radius=10000):
    """
    Find the closest vessel(s) for each island and the radius it was found within.
    Searches for the specified number of vessels (`max_vessels`) and iteratively increases the radius.
    
    Args:
        db: MongoDB database connection.
        max_vessels (int): Number of closest vessels to find for each island.
        radius_step (int): Incremental radius to check in meters.
        max_radius (int): Maximum search radius in meters.

    Returns:
        List[dict]: List containing island FID, vessel ID(s), and the radius.
    """
    island_collection = db.geodata_collection
    vessel_collection = db.dynamic_collection

    fids = island_collection.distinct("fid", {"loc_type": "island"})
    print(f"Found {len(fids)} islands in the database. Searching for vessels...")

    closest_vessels = []

    for fid in fids:
        # Fetch only the required fields
        island_doc = island_collection.find_one(
            {"loc_type": "island", "fid": fid},
            {"geometry": 1}
        )
        if not island_doc:
            print(f"No island found with FID {fid}.")
            continue

        geometry_type = island_doc["geometry"]["type"]
        geometry_coordinates = island_doc["geometry"]["coordinates"]

        if geometry_type != "Polygon":
            print(f"The geometry type of the island with FID {fid} is not a Polygon.")
            continue

        # Close the polygon
        geometry_coordinates[0] = close_polygon(geometry_coordinates[0])

        polygon = Polygon(geometry_coordinates[0])
        if not polygon.is_valid:
            print(f"Polygon for island FID {fid} is invalid even after closing.")
            continue

        centroid = polygon.centroid
        centroid_coords = [centroid.x, centroid.y]

        # Iteratively increase the radius until `max_vessels` are found or `max_radius` is reached

            # Incremental radius increases avoid running unnecessarily large-radius queries if vessels can be found within a smaller radius. 
            # This approach optimizes the search by starting small and expanding only as needed reducing computational overhead for large queries.

        current_radius = radius_step
        found_vessels = []
        while current_radius <= max_radius:
            geo_query = {
                "$geoNear": {
                    "near": {"type": "Point", "coordinates": centroid_coords},
                    "distanceField": "distance.calculated",
                    "maxDistance": current_radius,
                    "spherical": True,
                    "includeLocs": "distance.location"
                }
            }

            # Use a $limit stage after $geoNear
            pipeline = [
                geo_query,
                {"$limit": max_vessels}
            ]

            cursor = vessel_collection.aggregate(pipeline)
            found_vessels = list(cursor)

            if found_vessels:
                break  # Stop searching once vessels are found

            current_radius += radius_step  # Increase the radius

        if found_vessels:
            closest_vessels.append({
                "island_fid": fid,
                "vessels": [
                    {
                        "vessel_id": vessel["_id"],
                        "distance": vessel["distance"]["calculated"],
                        "location": vessel["distance"]["location"]
                    }
                    for vessel in found_vessels
                ],
                "radius_found": current_radius
            })
            # print(f"Island FID {fid}: Found {len(found_vessels)} vessel(s) within {current_radius} meters.")
        else:
            print(f"Island FID {fid}: No vessels found within {max_radius} meters.")

    return closest_vessels


def list_indexes(db):
    collection = db.dynamic_collection
    indexes = collection.index_information()
    for name, index in indexes.items():
        print(f"Index Name: {name}, Index Info: {index}")

def drop_2dsphere_indexes(db):
    """
    Drop only the `2dsphere` indexes on the `dynamic_collection` collection.
    """
    collection = db.dynamic_collection
    try:
        indexes = collection.index_information()
        for index_name, index_info in indexes.items():
            # Check if the index is a 2dsphere index
            if any(key[1] == "2dsphere" for key in index_info["key"]):
                collection.drop_index(index_name)
                print(f"Dropped 2dsphere index: {index_name}")
        print("All 2dsphere indexes have been dropped successfully.")
    except Exception as e:
        print(f"Error dropping 2dsphere indexes: {e}")

if __name__ == "__main__":
    db, client = mongo_connect()

    # Check existing indexes
    print("Checking existing indexes...")
    list_indexes(db)

    # Do the below if there is an issue with Multiple 2dsphere Indexes: 
        # Multiple Index Ambiguity: If multiple 2dsphere indexes exist, MongoDB cannot determine which one to use, causing an error: There is more than one 2dsphere index; unsure which to use for $geoNear.
    # Drop all 2dsphere indexes
    # drop_2dsphere_indexes(db)

    # Why positions.geometry: 
        # Vessel position coordinates are stored in positions.geometry, making it the correct field for geospatial queries.
    # Ensure the geospatial index exists
    ensure_geospatial_index_on_positions(db)

    # Queries run
    radius = 1000  # Radius in meters (e.g., 5 km)
    start_time = None  # Replace with a timestamp if filtering is needed
    end_time = None  # Replace with a timestamp if filtering is needed

    # Measure time for `find_islands_with_vessels`
    print("\n\n\nRunning `find_islands_with_vessels`...")
    start = time.time()
    islands_with_vessels = find_islands_with_vessels(db, radius, start_time, end_time)
    end = time.time()
    print(f"Islands with vessels nearby: {islands_with_vessels}")
    print(f"`find_islands_with_vessels` completed in {end - start:.2f} seconds.")

    # Measure time for `query_vessels_near_island`
    if islands_with_vessels:
        fid = islands_with_vessels[0]  # Get the first FID from the list of islands returned
        print(f"\n\n\nRunning `query_vessels_near_island` for FID {fid}...")
        start = time.time()
        query_vessels_near_island(db, fid, radius, start_time, end_time)
        end = time.time()
    print(f"`query_vessels_near_island` completed in {end - start:.2f} seconds.")

    # Measure time for `find_closest_vessels_per_island`
    print("\n\n\nRunning `find_closest_vessels_per_island`...")
    start = time.time()
    closest_vessels = find_closest_vessels_per_island(db)
    end = time.time()
    
    print("Closest vessels per island:")
    for vessel_info in closest_vessels:
        print(vessel_info)

    print(f"`find_closest_vessels_per_island` completed in {end - start:.2f} seconds.")

    client.close()

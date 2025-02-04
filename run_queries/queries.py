from pymongo import MongoClient, GEOSPHERE, ASCENDING
import time
import json
import geojson
from shapely.geometry import Polygon
from bson import json_util
import random
from datetime import datetime
from geopy.distance import geodesic


def mongo_connect():
    """
    Connect to the MongoDB instance and return the database and client.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client.mongo_db_project
    return db, client

def documents_output(cursor):
    """
    Print the results of cursor object (fetch size=5)
    """
    count = 0
    if cursor.alive:
        for doc in cursor:
            print(json_util.dumps(doc, indent=4))
            count += 1
            if count == 5:
                break
    else:
        print("No documents found!")

def explain_query(db, collection, pipeline):
    """
    Explain command
    """
    explain_plan = db.command("explain", {
            "aggregate": collection.name,
            "pipeline": pipeline,
            "cursor": {}
            }, verbosity="executionStats")
    
    # Explain results
    print(json.dumps(explain_plan, indent=4))

def close_polygon(coordinates):
    """
    Ensure that a polygon's coordinates are closed (i.e., the last point matches the first point).
    """
    if coordinates[0] != coordinates[-1]:
        coordinates.append(coordinates[0])  # Close the polygon
    return coordinates

def ensure_geospatial_index(db):
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


def query2_vessels_by_country(db):
    """
    εύρεση των πλοίων με σημαία Ελλάδας και όνομα που περιέχει κάποιο αλφαριθμητικό
    """
    print("Input the variables for query 'Find vessels from a specific country that contain a given alphanumeric in their ship type description'")
    country = input("Give the country name to check for ex. Malta:")
    alphanumeric = input("Give alphanumeric you want to check for in description ex. all:")
    collection = db.vessels_collection
    # Aggregate pipeline definition
    pipeline = [{"$match": {"country": country}}, # Match the country flag
                {"$match": {"description":
                                {"$regex": str(".*"+ alphanumeric + ".*"), "$options": "i"}  # Match descriptions contain (.*__ .*) "all" (case-insensitive)
                                }
                    }
                ]

    # Start timer
    start = time.time()
    # Fetch all aggregation results
    cursor = collection.aggregate(pipeline)
    documents_output(cursor)
    # End timer
    end = time.time()

    print(f"Execution time: {end - start:.4f} seconds")

def query3a_find_vessels_in_radius(db):
    """
    εύρεση των πλοίων εντός κύκλου με δοθέν κέντρο και ακτίνα X
    """
    collection = db.dynamic_collection
    
    print("Input the variables for query 'Find vessels within the circle of given point and radius")
    lon = float(input("Insert longitude of center point ex. 23.3699798:"))
    lat = float(input("Insert latitude of center point ex. 37.6972956:"))
    radius = float(input("Insert radius of circle (km) ex. 1:"))
    # Pipeline definition
    pipeline = [{"$match": {"positions.geometry": 
                                {"$geoWithin": 
                                        {"$centerSphere": [ [ lon, lat], radius/6378.1 ] # Center of circle and radius definition
                                        }
                                }
                            }
                }] # Maybe check {"$project" : {"vessel_id": 1, "positions.geometry.coordinates": 1}
    
    # Fetch results and calculate execution time
    start = time.time()
    cursor = collection.aggregate(pipeline)
    end = time.time()

    # Output the documents
    documents_output(cursor)

    # Execution time
    print(f"Execution time: {end - start:.4f} seconds")

def query3b_K_closest_vessels_to_point(db, K=10, point=[23.3699798, 37.6972956]):
    """
    K closest vessels to a given point
    """
    collection = db.dynamic_collection
    
    #collection.create_index([("vessel_id", ASCENDING)])
    #collection.create_index([("positions.geometry", GEOSPHERE)]) # 2dsphere index creation on "geometry" key
    #collection.create_index([("positions.geometry", GEOSPHERE), ("vessel_id", ASCENDING)])

    # Pipeline definition
    pipeline = [{"$geoNear":
                    {
                        "near": {"type": "Point", "coordinates": point}, # Point to calculate distance
                        "distanceField": "distance.calculated", # Show the calculated distance on document "distance"
                        "includeLocs": "distance.location", # Show the point that is near to "near" point
                        "spherical": "True" # Use spherical geometry
                    }
                },
                    {"$limit": K},
                    {"$project": {"vessel_id": 1, "distance": 1}} # Get the K closest points only
                ]
    
    # Fetch all aggregation results. + Execution time calculation
    start = time.time()
    cursor = collection.aggregate(pipeline)
    end = time.time()

    # Print the results
    #for doc in cursor:
    #    print(json_util.dumps(doc, indent=4))

    # Execution time
    print(f"Execution time: {end - start:.4f} seconds")
    #explain_query(collection, pipeline)'''

def find_islands_with_vessels(db, radius=1000, start_time=None, end_time=None):
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

def query3c_vessels_near_island(db, fid=1, radius=1000, start_time=None, end_time=None):
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

def query4_vessel_proximity_in_time_range(db):
    """
    εύρεση των πλοίων που πλησίασαν μεταξύ τους σε απόσταση Χ εντός χρονικού διαστήματος Τ.
    """
    collection = db.dynamic_nov
    time_start = datetime.strptime("2017-11-06T08:00:00.000+00:00", "%Y-%m-%dT%H:%M:%S.%f%z")
    time_end = datetime.strptime("2017-11-06T08:59:59.000+00:00", "%Y-%m-%dT%H:%M:%S.%f%z")

    start = time.time()
    # Query vessels that have positions within the given time range
    vessels_in_time_range = collection.find({
        "positions.timestamp": {
            "$gte": time_start,
            "$lte": time_end
        }
    })
    vessels = list(vessels_in_time_range)
    print(f"Found {len(vessels)} in timerange [{time_start}, {time_end}].")
    documents = []
    for vessel in vessels:
        vessel_id = vessel['vessel_id']  # Current vessel ID
        
        for pos in vessel['positions']:
            pos_timestamp = pos['timestamp']
            coord1 = pos['geometry']['coordinates']


            # Query for other vessels with the same timestamp, excluding the current vessel
            nearby_vessels = collection.find({
                "vessel_id": {"$ne": vessel_id},  # Exclude self
                "positions": {
                    "$elemMatch": {
                        "timestamp": pos_timestamp,  # Match exact timestamp
                        "geometry.coordinates": {
                            "$geoWithin": {
                                "$centerSphere": [coord1, 4/ 6378.1]  # Convert radius to radians
                            }
                        }
                    }
                }
            })

            for near_vessel in nearby_vessels:
                for near_pos in near_vessel['positions']:
                    if near_vessel['vessel_id'] != vessel_id and near_pos['timestamp'] == pos_timestamp:
                        coord2 = near_pos['geometry']['coordinates']

                        # Skip if the coordinates are identical (same position)
                        if coord1 == coord2:
                            continue  

                        distance = geodesic(coord1[::-1], coord2[::-1]).meters  # Lat/Lon swap required
                        locations = {"timestamp": pos_timestamp,
                                    "vessel_1": {"vessel_id": vessel_id,
                                                  "coordinates": coord1},
                                    "vessel_2": {"vessel_id": near_vessel['vessel_id'],
                                                  "coordinates": coord2},
                                    "distance(m)": distance}
                        documents.append(locations)

    print(f"Executed in {start - time.time():.4f} seconds")
    return documents      

def main():
    db, client = mongo_connect()
    
    ensure_geospatial_index(db)

    # queries
    print("\n\n\nRunnin query vessels_by_country")
    query2_vessels_by_country(db)
    print("\n\n\nRunnin query find_vessels_in_radius")
    query3a_find_vessels_in_radius(db)
    print("\n\n\nRunnin query K_closest_vessels_to_point")
    query3b_K_closest_vessels_to_point(db)

    print("\n\n\nRunnin query find_islands_with_vessels")
    islands_with_vessels = find_islands_with_vessels(db)
    fid = islands_with_vessels[0]

    print("\n\n\nRunnin query find_vessels_near_island")
    query3c_vessels_near_island(db, fid)

    print("\n\n\nRunnin query find_closest_vessels_per_island")
    closest_vessels = find_closest_vessels_per_island(db)
    print("Closest vessels per island:")
    for vessel_info in closest_vessels:
        print(vessel_info)

    print("\n\n\nRunnin query vessel_proximity_in_time_range")
    documents = query4_vessel_proximity_in_time_range(db)
    print(documents)
    client.close()

if __name__ == "__main__":
    main()

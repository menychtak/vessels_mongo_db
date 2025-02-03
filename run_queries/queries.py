#%%
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
    client = MongoClient("mongodb://localhost:27017/")
    db = client.mongo_db_project
    return db, client

def documents_output(cursor):
    # Print the results of cursor object (fetch size=5)
    count = 0
    if cursor.alive:
        for doc in cursor:
            print(json_util.dumps(doc, indent=4))
            count += 1
            if count == 5:
                break # Use if you want to fetch specific count of documents
    else:
        print(f"No documents found!")

def explain_query(collection, pipeline):
    # Explain command
    explain_plan = db.command("explain", {
            "aggregate": collection.name,
            "pipeline": pipeline,
            "cursor": {}
            }, verbosity="executionStats")
    
    # Explain results
    print(json.dumps(explain_plan, indent=4))

def query_2(db):
    # εύρεση των πλοίων με σημαία Ελλάδας και όνομα που περιέχει κάποιο αλφαριθμητικό
    print("Input the variables for query 'Find vessels from a specific country that contain a given alphanumeric in their ship type description'")
    country = input("Give the country name to check for: ")
    alphanumeric = input("Give alphanumeric you want to check for in description: ")
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
    
def query_3a(db):
    # εύρεση των πλοίων εντός κύκλου με δοθέν κέντρο και ακτίνα Χ
    collection = db.dynamic_collection
    
    print("Input the variables for query 'Find vessels within the circle of given point and radius")
    lon = float(input("Insert longitude of center point: "))
    lat = float(input("Insert latitude of center point: "))
    radius = float(input("Insert radius of circle (km): "))
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

def query_3b(db):
    collection = db.dynamic_collection
    K = 10
    #collection.create_index([("vessel_id", ASCENDING)])
    #collection.create_index([("positions.geometry", GEOSPHERE)]) # 2dsphere index creation on "geometry" key
    #collection.create_index([("positions.geometry", GEOSPHERE), ("vessel_id", ASCENDING)])

    # Pipeline definition
    pipeline = [{"$geoNear":
                    {
                        "near": {"type": "Point", "coordinates": [23.3699798, 37.6972956]}, # Point to calculate distance
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

def query_3c(db):
    #εύρεση των πλοίων που βρέθηκαν σε απόσταση Χ από κάποιο νησί

    collection = db.geodata_collection
    # Find distinct FIDs of islands in geodata_collection
    pipeline = [{"$match": 
                    {"loc_type": "island"}
                    },
                {"$group":
                    {"_id": "$fid"}
                    },
                {"$sort": 
                    {"_id": 1}
                    }
                ]
    cursor = collection.aggregate(pipeline)

    # Create a list with islands FIDs
    islands_fid = [doc['_id'] for doc in cursor]
    print(islands_fid)

    # FID random selection
    fid = islands_fid[random.randrange(len(islands_fid))]

    pipeline = [{"$match":
                    {"$and": [
                        {"loc_type": "island"},
                        {"fid": 42}
                        ]
                    }
                },
                {"$unwind": "$geometry.coordinates"},
                {"$project":
                    {"_id": 0}
                }]
    cursor = collection.aggregate(pipeline)
    if cursor.alive:
        results = list(cursor)
        #if len(results) == 1:
            #print(results[0])
        # Get island's geometry
        #print(results[0])
        geometry_type = results[0]['geometry']['type']
        geometry_coordinates = results[0]['geometry']['coordinates']
        geometry_coordinates.append(geometry_coordinates[0])
        #print(geometry_coordinates)

        island_geometry = {"type": geometry_type, 
                           "coordinates": geometry_coordinates}
        island_geometry = geojson.dumps(island_geometry, indent=4)
        print(type(island_geometry))
        #polygon = Polygon(island_geometry)
        #centroid = polygon.centroid
        #collection = db.dynamic_nov

        # Vessel IDs in distance X of an island 
        '''pipeline = [{"$geoNear":
                    {
                        "near": {"$or": [
                                    {"type": "Point", "coordinates": [23.0879522, 37.8663437]},
                                    {"type": "Poin" , "coordinates": [23.0834872, 37.8683788]}], # Point to calculate distance
                        "maxDistance": 7000, # Distance in meters
                        "distanceField": "distance.calculated", # Show the calculated distance on document "distance"
                        "includeLocs": "distance.location", # Show the point that is near to "near" point
                        "spherical": "True" # Use spherical geometry
                    }
                },
                #{"$project": {"vessel_id": 1, "distance": 1}
                #} 
                }]
    
        # Fetch all aggregation results. + Execution time calculation
        start = time.time()
        cursor = collection.aggregate(pipeline)
        end = time.time()
        #else:
        #   print(f"More than one islands with FID {fid}.")'''
    else:
        print(f"No documents returned.")
    
def query_4(db):
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
    query_2(db)
    query_3a(db)
    query_3b(db)
    documents = query_4(db)
    # print(documents)
    client.close()

if __name__ == "__main__":
    main()
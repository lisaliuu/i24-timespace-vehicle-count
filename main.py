from pymongo import MongoClient
from bson.objectid import ObjectId
import json

def connect_to_collection(config: str=None,
                    client_username: str=None, 
                    client_password: str=None, 
                    client_host: str=None, 
                    client_port: int=None, 
                    database: str=None, 
                    collection: str=None,):
    """
    Connects to a MongoDB instance.
    
    :param config: Optional config file containing the following params in JSON form.
    :param username: Database authentication username.
    :param password: Database authentication password.
    :param host: Database connection host name.
    :param port: Database connection port number.
    :param database: Name of database to connect to (do not confuse with collection name).
    :param collection: Name of collection to connect to.
    """
    if config:
        with open(config) as f:
            config_params = json.load(f)
            client_host=config_params['host']
            client_username=config_params['username']
            client_password=config_params['password']
            client_port=config_params['port']
            database=config_params['read_database_name']
            collection=config_params['read_collection_name']

    client=MongoClient(host=client_host,
                port=client_port,
                username=client_username,
                password=client_password,
                connect=True,
                connectTimeoutMS=5000)
    db=client[database]
    col=db[collection]
    return col

collection=connect_to_collection(config="config.json")

#query parameters
t1=1623877088
t2=1623877110

x1=1000
x2=800
y1=100
y2=20

#Method 1: Processing and iterating entirely in Python
def python_loops():
    """
    Returns a find cursor containing 2 arrays (position and id array) matching 
    time query and loops through position array to match documents according 
    to position query and stores into a cache (set) unique vehicle IDs
    """
    unique_veh_set=set()
    cursor = collection.find({"timestamp":{"$gt":t1, "$lt": t2}})
    for doc in cursor:
        for index in range(len(doc['position'])):

            #doc['position][index][0] = x coordinate
            #doc['position][index][1] = y coordinate
            if (doc['position'][index][0]<x1 and doc['position'][index][0]>x2 and 
                doc['position'][index][1]<y1 and doc['position'][index][1]>y2):
                
                #doc['id'][index] = vehicle id
                unique_veh_set.add(doc['id'][index])

    print ("python loops count: "+str(len(unique_veh_set)))

#Method 2: Processing through aggregation first then iterating in Python
def aggregate_and_python_loops():
    """"
    Returns an aggregation cursor matching time query that contains 1 array that has 
    been merged to contain both position and id which Python loops through to match 
    position query and stores corresponding unique vehicle ID into unique cache (set)
    """
    unique_veh_set=set()
    cursor=collection.aggregate([
        # matches time query
        {
            "$match":{"$and":[
                            {"timestamp":{"$gt":t1}},
                            {"timestamp":{"$lt":t2}}
                            ]}
        },

        # "zips" id and position array into 1 array to be returned
        {
            "$project": {
                "veh_pos": {
                    "$zip": {"inputs": [
                                        "$id",
                                        "$position"
                                        ]}
                            }
                        }
        }
    ])

    for doc in cursor:
        for vehicle_pos in doc['veh_pos']:

            #vehicle_pos[1][0] = x coordinate
            #vehicle_pos[1][1] = y coordinate
            if (vehicle_pos[1][0]<x1 and vehicle_pos[1][0]>x2 and 
                vehicle_pos[1][1]<y1 and vehicle_pos[1][1]>y2):

                #vehicle_pos[0] = vehicle id
                unique_veh_set.add(vehicle_pos[0])

    print("aggregate with python loop count: "+str(len(unique_veh_set)))

#Method 3: Processing and iterating entirely in MongoDB aggregation
def full_aggregate():
    """
    Returns an aggregation cursor that contains a document with the number of unique vehicles
    that lives int he time-space grid. All processing and iterating is done in the aggregation
    pipeline
    """
    pipeline = [
    
        # matches time query
        {
            "$match": { "$and": [
                {"timestamp": { "$gt": t1 }},
                {"timestamp": { "$lt": t2 }},
            ]}
        },

        # "zips" id and position array into 1 array
        {
            "$project": {
            "veh_pos": { "$zip": { "inputs": [
                "$id",
                "$position"
                ]}}
            }
        },

        # make each element of veh_pos its own document to be processed
        {
            "$unwind": "$veh_pos"
        },

        # create fields for the position coordinates to be processed
        {
            "$project": {
            "veh": {
                "$first": "$veh_pos",
            },
            "pos": {
                "$last": "$veh_pos", 
            }
            }
        },
        {
            "$project": {
            "veh":1,
            "x": { "$first": "$pos"},
            "y": { "$last": "$pos"}
            }
        },
        
        # apply time query
        {
            "$match": { "$and": [
                { "x": { "$gt": x2 } },
                { "x": { "$lt": x1 } },
                { "y": { "$lt": y1 } },
                { "y": { "$gt": y2 }}
            ]}
        },
        
        # add to set to count number of unique docs
        {
            "$group": {
                "_id": None,
                "uniqueValues": { "$addToSet": "$veh" }
            }
        },

        # count number of unique docs
        {
            "$project": {"count":{"$size":"$uniqueValues"}}
        }
    ]

    cursor=collection.aggregate(pipeline)
    for doc in cursor:
        print("aggregate only: "+str(doc))

python_loops()
aggregate_and_python_loops()
full_aggregate()
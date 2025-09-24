import os, pymongo
from azure.storage.blob import BlobServiceClient

STORAGE_CONNECTION_STRING = os.getenv("storage_connection_string")
MONGODB_CONNECTION_STRING = os.getenv("mongodb_connection_string")

def get_marketplaces():
    client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    
    db = client['cookies']
    collection = db['marketplaces']
    marketplaces = list(collection.find())
    
    client.close()
    return marketplaces

def add_marketplace(marketplace_name, has_cookies, cookies):
    client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    
    db = client['cookies']
    collection = db['marketplaces']
    
    if has_cookies == "true":
        try:
            cookies = eval(cookies)
        except:
            cookies = {}
    else:
        cookies = {}
    
    new_marketplace = {
        "name": marketplace_name,
        "cookie": has_cookies,
        "cookies": cookies
    }
    
    collection.insert_one(new_marketplace)
    client.close()
    
def get_marketplace_results(marketplace_name):
    client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    
    db = client['url_db']
    collection = db['urls']
    
    results = list(collection.find({"marketplace": marketplace_name}))
    
    client.close()
    return results
    
def remove_marketplace(marketplace_name):
    pass

def add_cookie_to_marketplace(marketplace_name, cookie_name, cookie_value):
    client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    
    db = client['cookies']
    collection = db['marketplaces']
    
    collection.update_one(
        {"name": marketplace_name},
        {"$set": {f"cookies.{cookie_name}": cookie_value, "cookie": True}}
    )
    
    client.close()
    
def remove_cookie_from_marketplace(marketplace_name, cookie_name):
    client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    
    db = client['cookies']
    collection = db['marketplaces']
    
    collection.update_one(
        {"name": marketplace_name},
        {"$unset": {f"cookies.{cookie_name}": ""}}
    )
    
    marketplace = collection.find_one({"name": marketplace_name})
    if not marketplace.get("cookies"):
        collection.update_one({"name": marketplace_name}, {"$set": {"cookie": False}})
    
    client.close()
    
def get_storage_structure():
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container = blob_service_client.get_container_client("crawling-results")
    
    blobs = container.list_blobs()
    structure = {}

    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) >= 3:  # marketplace/depth/file
            marketplace, depth, filename = parts[0], parts[1], parts[2]
            structure.setdefault(marketplace, {}).setdefault(depth, []).append(filename)
            
    return structure

def get_blob_content(marketplace, depth, filename):
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container = blob_service_client.get_container_client("crawling-results")
    
    blob_path = f"{marketplace}/{depth}/{filename}"
    blob_client = container.get_blob_client(blob_path)
    
    content = blob_client.download_blob().readall()
    return content
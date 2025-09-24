import os, pymongo

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
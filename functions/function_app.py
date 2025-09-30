import azure.functions as func
import azure.durable_functions as df
from azure.data.tables import TableServiceClient, UpdateMode
from azure.storage.queue import QueueServiceClient, QueueClient
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging, os, json, requests, hashlib, datetime, time

app = df.DFApp(http_auth_level = func.AuthLevel.FUNCTION)

# Storage connection
STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
COSMOS_CONNECTION_STRING = os.getenv("CosmosDBConnectionString")
PROXY_URL = os.getenv("proxy_url")
QUEUE_PREFIX = "queue-"
TABLE_NAME = "crawlerconfig"

mongo_client = MongoClient(COSMOS_CONNECTION_STRING)

@app.route(route = "local_test")
def local_test(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Local test function triggered.")
    try:
        logging.info(f"STORAGE_CONNECTION_STRING: {STORAGE_CONNECTION_STRING}")
        logging.info(f"COSMOS_CONNECTION_STRING: {COSMOS_CONNECTION_STRING}")
        
        # Test connection to Azure Storage
        queue_service = QueueServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        logging.info("Connected to Azure Storage successfully.")
        
        # Test connection to Cosmos DB
        client = MongoClient(COSMOS_CONNECTION_STRING)
        client.admin.command("ping")
        logging.info("Connected to Cosmos DB successfully.")
        
        return func.HttpResponse("Local test completed successfully!", status_code = 200)

    except Exception as e:
        logging.error(f"Error in local test function: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code = 500)

@app.route(route = "crawling_setup")
def crawling_setup(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Setup Function triggered.")

    try:
        req_body = req.get_json()
        root_url = req_body.get("root_url")
        marketplace = req_body.get("marketplace")
        max_workers = int(req_body.get("max_workers", 1))
        max_depth = int(req_body.get("max_depth", 3))
        max_links = int(req_body.get("max-links"))
       
        if not root_url:
            return func.HttpResponse("Missing root_url", status_code = 400)

        queue_service = QueueServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

        logging.info(f"Creating {max_depth + 1} queues for BFS tree traversal...")
        for i in range(max_depth + 1):
            queue_name = f"{QUEUE_PREFIX}{i}"
            try:
                queue_service.create_queue(queue_name)
                logging.info(f"Created queue for level: {queue_name}")
            except:
                logging.info(f"Queue {QUEUE_PREFIX}{i} already exists.")
                
        logging.info("Creating init queue...")
        try:
            queue_service.create_queue("queue-init")
            logging.info("Created init queue.")
        except:
            logging.info("Init queue already exists.")
            
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        table_client = table_service.get_table_client(TABLE_NAME)

        try:
            table_client.create_table()
            logging.info(f"Created table.")
        except:
            logging.info(f"Table {TABLE_NAME} already exists.")

        entity = {
            "PartitionKey": "Config",
            "RowKey": "GlobalSettings",
            "marketplace": marketplace,
            "max_workers": max_workers,
            "max_depth": max_depth,
            "max_links": max_links
        }
        table_client.upsert_entity(entity, mode = UpdateMode.REPLACE)
        logging.info("Configuration saved in Table Storage.")

        # First message goes in queue-0 and init message in queue-init
        queue_client_clear = QueueClient.from_connection_string(conn_str = STORAGE_CONNECTION_STRING, queue_name = f"{QUEUE_PREFIX}0", message_encode_policy=None)
        first_message = json.dumps({"url": root_url, "depth": 0})
        queue_client_clear.send_message(first_message, visibility_timeout = 0)
        logging.info(f"Inserted root URL into queue-0: {root_url}")
        
        queue_client_init = QueueClient.from_connection_string(conn_str = STORAGE_CONNECTION_STRING, queue_name = "queue-init", message_decode_policy = None)
        result = queue_client_init.send_message(root_url, visibility_timeout = 0)
        logging.info(f"Inserted init message into queue-init.")
        
        return func.HttpResponse("Setup completed successfully!", status_code = 200)

    except Exception as e:
        logging.error(f"Error in setup function: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code = 500)
    
@app.route(route = "crawling_clean")
def crawling_clean(req: func.HttpRequest) -> func.HttpResponse:
    try:
        partition_key = "Config"
        row_key = "GlobalSettings"
        
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        table_client = table_service.get_table_client(TABLE_NAME)
        
        entity = table_client.get_entity(partition_key = partition_key, row_key = row_key)
        max_depth = int(entity["max_depth"])

        logging.info(f"max_depth found: {max_depth}")

        queue_service = QueueServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        
        logging.info(f"Deleting queues...")
        for i in range(max_depth + 1):
            queue_name = f"{QUEUE_PREFIX}{i}"
            logging.info(f"Eliminazione della coda: {queue_name}")
            queue_service.delete_queue(queue_name)
        queue_service.delete_queue("queue-init")

        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        logging.info(f"Deleting table: {TABLE_NAME}")
        table_service.delete_table(TABLE_NAME)
        
        return func.HttpResponse("Tasks done.", status_code = 200)
    
    except Exception as e:
        logging.error(f"Errore: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code = 500)

@app.route(route = "crawling_starter")
@app.durable_client_input(client_name = "client")
async def crawling_starter(req: func.HttpRequest, client) -> func.HttpResponse:
    table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    config_table = table_service.get_table_client(TABLE_NAME)
    queue_client = QueueClient.from_connection_string(conn_str = STORAGE_CONNECTION_STRING, queue_name = "queue-init", message_decode_policy = None)
    
    entity = config_table.get_entity(partition_key = "Config", row_key = "GlobalSettings")
    max_workers = int(entity["max_workers"])
    max_depth = int(entity["max_depth"])
    max_links = int(entity["max_links"])
    marketplace = str(entity["marketplace"])
    
    setup_url_database()
    create_storage_container("crawling-results")
    
    messages = queue_client.receive_messages()
    for msg in messages:
        message = msg.content
        queue_client.delete_message(msg)

    url_insert(message, 0, marketplace)

    # Testing function app before orchestration
        # logging.info(f"Testing function app with {max_workers} workers, {max_depth} depth, and {max_links} links on marketplace: {marketplace}.")
        # logging.info(f"Testing URL insertion for root URL: {message}")
    
    # Production code to start orchestration
    logging.info(f"Testing function app with {max_workers} workers.")
    instance_id = await client.start_new("orchestrator_function", None, {"marketplace": marketplace, "counter": 0, "max_workers": max_workers, "max_depth": max_depth, "max_links": max_links})
    logging.info(f"Launched orchestration with ID = '{instance_id}'.")
    
    return func.HttpResponse(instance_id, status_code = 200)

@app.orchestration_trigger(context_name = "context")
def orchestrator_function(context: df.DurableOrchestrationContext):
    try:
        instance_id = context.instance_id
        input_data = context.get_input()
        marketplace = input_data.get("marketplace")
        max_workers = input_data.get("max_workers")
        max_depth = input_data.get("max_depth")
        max_links = input_data.get("max_links")
        
        counter = input_data.get("counter", 0)
        
        if not context.is_replaying:
            logging.info(f"[ORCHESTRATOR {instance_id}]: Starting orchestrator {instance_id}...")
            logging.info(f"[ORCHESTRATOR {instance_id}]: Info received from config table: max_workers: {max_workers}, max_depth: {max_depth}, max_links: {max_links}")
            
        if not context.is_replaying:
            logging.info(f"[ORCHESTRATOR {instance_id}]: setup_crawling_env completed.")
        
        queue_index = 0
        while queue_index <= max_depth:   
            logging.info(f"[ORCHESTRATOR {instance_id}]: Current counter: {counter}")         
            if counter >= max_links:
                logging.warning(f"[ORCHESTRATOR {instance_id}]: Max links limit of {max_links} reached. Current count: {counter}. Terminating orchestration.")
                break
            
            if not context.is_replaying:
                logging.info(f"[ORCHESTRATOR {instance_id}]: Processing queue: {queue_index}")
            
            retrieved_messages = yield context.call_activity("queue_reader_function", {
                "index": queue_index,
                "max_workers": max_workers
            })
            
            if not context.is_replaying:
                logging.info(f"[ORCHESTRATOR {instance_id}]: got {retrieved_messages}")
                logging.info((f"[ORCHESTRATOR {instance_id}]: retrieved {len(retrieved_messages)} URLs from queue {queue_index}. Processing..."))
            
            if len(retrieved_messages) == 0:
                logging.info(f"[ORCHESTRATOR {instance_id}]: queue {queue_index} is empty. Skipping to next level.")
                queue_index += 1
                continue
                
            url_to_crawl = yield context.call_activity("filter_urls", {
                "messages": retrieved_messages
            })

            if len(url_to_crawl) == 0:
                logging.info(f"[ORCHESTRATOR {instance_id}]: no new URLs to process at level {queue_index}.")
                queue_index += 1
                continue
            
            remaining_links = max_links - counter
            if remaining_links <= 0:
                logging.info(f"[ORCHESTRATOR {instance_id}]: Max links limit reached. No more URLs to process.")
                break
            
            urls_to_process = url_to_crawl[:remaining_links]
            if not context.is_replaying:
                logging.info(f"[ORCHESTRATOR {instance_id}]: launching url_processor_function workers for {len(url_to_crawl)} URLs... (Limited by max_links: {remaining_links} remaining)")            
        
            try:
                crawl_tasks = [context.call_activity("url_processor_function", {"url": msg, "marketplace": marketplace}) for msg in urls_to_process]
                crawl_results = yield context.task_all(crawl_tasks)

                if not context.is_replaying:
                    logging.info(f"[ORCHESTRATOR {instance_id}]: crawl results received.")
            except Exception as e:
                logging.error(f"[ORCHESTRATOR {instance_id}]: error in processing URLs: {str(e)}")
                raise
            
            postprocess_result = yield context.call_activity("postprocess_results", {
                "results": crawl_results,
                "depth": queue_index,
                "max_depth": max_depth,
                "current_counter": counter,
                "marketplace": marketplace
            })
            
            has_failures = postprocess_result.get("has_failures")
            counter = postprocess_result.get("updated_counter")
            
            if not context.is_replaying:
                logging.info(f"[ORCHESTRATOR {instance_id}]: Updated counter to {counter}")

            if has_failures:
                if not context.is_replaying:
                    logging.info(f"[ORCHESTRATOR {instance_id}]: retry needed at queue {queue_index} due to failures.")
        logging.info("Orchestration complete.")
    except Exception as e:
        logging.error(f"ORCHESTRATOR {instance_id} - ERROR: {str(e)}")
        raise
    
@app.activity_trigger(input_name = "inputdata")
def filter_urls(inputdata):
    messages = inputdata.get("messages")
    filtered_urls = []
    
    for message in messages:
        data = json.loads(message)
        url = data.get("url")
        
        if url_check(url):
            filtered_urls.append(message)
            
    return filtered_urls

@app.activity_trigger(input_name = "inputdata")
def postprocess_results(inputdata):
    logging.info("[POST-PROCESS ACTIVITY]: Postprocessing results...")
    
    crawl_results = inputdata["results"]
    depth = inputdata["depth"]
    max_depth = inputdata["max_depth"]
    current_counter = inputdata.get("current_counter")
    marketplace = inputdata.get("marketplace")
    
    queue_name = f"{QUEUE_PREFIX}{depth + 1}"
    queue_client = QueueClient.from_connection_string(
        conn_str = STORAGE_CONNECTION_STRING,
        queue_name = queue_name,
        message_encode_policy = None
    )
    
    has_failures = False
    successfully_crawled = 0 
    
    for result in crawl_results:
        if type(result) == dict:
            extracted_links = result.get("extracted_links")
            successfully_crawled += 1
            
            url_id = get_object_id(result.get("url"))
            
            for new_url in extracted_links:
                if depth + 1 > max_depth:
                    logging.info("[POST-PROCESS ACTIVITY]: Max depth reached. Not inserting further URLs.")
                    continue
                
                check = url_check(new_url)
                if check == False or check == True:
                    logging.info(f"[POST-PROCESS ACTIVITY]: URL {new_url} already exists in the database. Skipping...")
                    new_url_id = get_object_id(new_url)
                    create_edge(url_id, str(new_url_id))
                    continue
                else:
                    queue_client.send_message(
                        json.dumps({"url": new_url, "depth": depth + 1}),
                        visibility_timeout = 1
                    )
                    logging.info(f"[POST-PROCESS ACTIVITY]: Inserted URL into queue {queue_name}: {new_url}")
                    inserted_url = url_insert(new_url, depth + 1, marketplace)
                    
                    create_edge(url_id, str(inserted_url))
        else:
            update_fail(result)
            has_failures = True
            
    updated_counter = current_counter + successfully_crawled
    logging.info(f"[POST-PROCESS ACTIVITY]: Successfully crawled {successfully_crawled} URLs. Updated counter: {updated_counter}")
    
    return {
        "has_failures": has_failures,
        "updated_counter": updated_counter
    }
    
@app.activity_trigger(input_name = "inputdata")
def queue_reader_function(inputdata: dict) -> list:
    index = inputdata.get('index')
    max_workers = inputdata.get('max_workers')
    
    queue_name = f"queue-{index}"
    
    queue_service = QueueServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    queue_client = queue_service.get_queue_client(queue_name)
    
    logging.info(f"[QUEUE-READER ACTIVITY]: Looking for messages from queue: {queue_name}")
    messages = list(queue_client.receive_messages(max_messages = max_workers))
    
    if messages:
        logging.info(f"[QUEUE-READER ACTIVITY]: Received {len(messages)} messages from queue {queue_name}.")
        urls = [msg.content for msg in messages]
        for msg in messages:
            queue_client.delete_message(msg)
        return urls
    else:
        logging.info(f"[QUEUE-READER ACTIVITY]: No messages found in queue {queue_name}.")
        return []
     
@app.activity_trigger(input_name = "inputdata")
def url_processor_function(inputdata: dict) -> str:
    logging.info(f"[URL-CRAWLER ACTIVITY]: Processing URL: {inputdata.get('url')}")
    
    message = inputdata.get("url")
    marketplace = inputdata.get("marketplace")
    
    data = json.loads(message)
    url = data.get("url")
    depth = data.get("depth")
    
    local_proxy = {
        'http': 'socks5h://localhost:9050',
        'https': 'socks5h://localhost:9050'
    }
    
    proxies = {
        'http': f'socks5h://{PROXY_URL}:9050',
        'https': f'socks5h://{PROXY_URL}:9050'
    }

    try:
        logging.info(f"[URL-CRAWLER ACTIVITY]: Scraping url: {url}")
        
        db = mongo_client["cookies"]
        marketplaces = db["marketplaces"]

        marketplace_db = marketplaces.find_one({"name": marketplace})
        use_cookies = marketplace_db.get("cookie")
        boolean_val = use_cookies.lower() == "true"
        
        start_time = time.time()
        start_time_db = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        final_response = None
        
        if boolean_val:
            logging.info(f"[URL-CRAWLER ACTIVITY]: Using cookies for {marketplace} marketplace.")
            cookie_list = marketplace_db.get("cookie_list")
            
            for cookie in cookie_list:
                try:
                    cookies = {cookie["name"]: cookie["value"]}
                    logging.info(f"[URL-CRAWLER ACTIVITY]: Trying with cookie: {cookies}")
                    response = requests.get(url, proxies = proxies, cookies = cookies)

                    if response.status_code == 200:
                        logging.info(f"[URL-CRAWLER ACTIVITY]: Success with cookie {cookies}")
                        final_response = response
                        break
                    else:
                        logging.warning(f"[URL-CRAWLER ACTIVITY]: Received status {response.status_code} with cookie {cookies}")
                except Exception as inner_e:
                    logging.warning(f"[URL-CRAWLER ACTIVITY]: Cookie attempt failed: {inner_e}")
        else:
            logging.info(f"[URL-CRAWLER ACTIVITY]: Marketplace {marketplace} does not require cookies.")
            response = requests.get(url, proxies = proxies)
            logging.info(f"[URL-CRAWLER ACTIVITY]: Successfully got a response from TOR server.")
            final_response = response
   
        elapsed_time = time.time() - start_time
        
        update_crawled(url, elapsed_time, start_time_db)
        logging.info(f"[URL-CRAWLER ACTIVITY]: Uploading HTML content to storage...")
        upload_html(f"{marketplace}", f"level-{depth}/{hashlib.sha256(url.encode('utf-8')).hexdigest()}.html", final_response.text, "crawling-results")
        
        extracted_links = extract_internal_links(final_response)
        logging.info(f"[URL-CRAWLER ACTIVITY]: Found {len(extracted_links)} internal links.")
        
        return {"url": url, "extracted_links": extracted_links}
    except Exception as e:
        logging.error(f"[URL-CRAWLER ACTIVITY]: Error processing URL {url}: {str(e)}")
        return url
        
def setup_url_database(db_name: str = "url_db", collection_name: str = "urls"):
    try:
        mongo_client.admin.command("ping")
        logging.info(f"[URL-DB SETUP]: Connected to CosmosDB.")

        db = mongo_client[db_name]
        collection = db[collection_name]
        
    except ConnectionFailure:
        logging.info("[URL-DB SETUP]: CosmosDB connection failed.")
    except OperationFailure as e:
        logging.info(f"[URL-DB SETUP]: Error: {e}")
        
def create_storage_container(container_name: str):
    try:
        logging.info(f"[STORAGE SETUP]: Connecting to storage container...")
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        if not container_client.exists():
            logging.info(f"[STORAGE SETUP]: Creating container: {container_name}")
            container_client.create_container()
    except Exception as e:
        logging.error(f"[STORAGE SETUP]: Error creating container: {str(e)}")
        raise
    
def upload_html(domain, page_name, content, container_name: str):
    logging.info(f"[UPLOAD HTML]: Connecting to storage container...")
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)
        
    logging.info(f"[UPLOAD HTML]: Uploading HTML content to {container_name}...")
    blob_path = f"{domain}/{page_name}"
    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(content, overwrite = True)
            
def url_check(url: str, db_name: str = "url_db", collection_name: str = "urls"):
    try:
        db = mongo_client[db_name]
        collection = db[collection_name]

        existing_url = collection.find_one({"url": url})
        if existing_url:
            logging.info(f"[URL CHECK]: URL {url} already exists in the database, checking if it is already crawled...")
            if existing_url.get("crawled"):
                logging.info(f"[URL CHECK]: URL {url} has already been crawled.")
                return False
            else:
                logging.info(f"[URL CHECK]: URL {url} exists but has not been crawled yet.")
                return True
        return None
    except ConnectionFailure:
        logging.info("[URL CHECK]: CosmosDB connection failed.")
    except OperationFailure as e:
        logging.info(f"[URL CHECK]: Error: {e}")
        
def get_object_id(url: str, db_name: str = "url_db", collection_name: str = "urls"):
    try:
        db = mongo_client[db_name]
        collection = db[collection_name]

        existing_url = collection.find_one({"url": url})
        if existing_url:
            return existing_url['identifier']
    except ConnectionFailure:
        logging.info("[OBJECT ID]: CosmosDB connection failed.")
    except OperationFailure as e:
        logging.info(f"[OBJECT ID]: Error: {e}")

def create_edge(source_id: str, target_id: str, db_name: str = "url_db", collection_name: str = "edges"):
    try:
        db = mongo_client[db_name]
        collection = db[collection_name]

        collection.insert_one({
            "source": source_id,
            "target": target_id,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
    except ConnectionFailure:
        logging.info("[CREATE EDGE]: CosmosDB connection failed.")
    except OperationFailure as e:
        logging.info(f"[CREATE EDGE]: Error: {e}")

def url_insert(url: str, depth: int, marketplace, db_name: str = "url_db", collection_name: str = "urls"):
    try:
        db = mongo_client[db_name]
        collection = db[collection_name]

        logging.info(f"[URL INSERT]: URL {url} does not exist in the database, creating entry...")
        result = collection.insert_one({"url": url, 
                               "marketplace": marketplace,
                               "depth": depth, 
                               "crawled": False, 
                               "hash_url": hashlib.sha256(url.encode('utf-8')).hexdigest(),
                               "inserted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                               "downloaded_at": None,
                               "download_time": None,
                               "identifier": None,
                               "retries": 0
                               })
        
        object_id = result.inserted_id
        collection.update_one(
            {"_id": object_id},
            {"$set": {"identifier": str(object_id)}}
        )

        return object_id

    except ConnectionFailure:
        logging.info("[URL INSERT]: CosmosDB connection failed.")
    except OperationFailure as e:
        logging.info(f"[URL INSERT]: Error: {e}")
        
def update_crawled(url: str, download_time, start_time, db_name: str = "url_db", collection_name: str = "urls"):
    try:
        db = mongo_client[db_name]
        collection = db[collection_name]

        existing_url = collection.find_one({"url": url})
        if existing_url:
            logging.info(f"[URL UPDATE]: Updating status for {url}...")
            collection.update_one({"url": url}, {"$set": {"crawled": True, "download_time": download_time, "downloaded_at": start_time}})
    
    except ConnectionFailure:
        logging.info("[URL UPDATE]: CosmosDB connection failed.")
    except OperationFailure as e:
        logging.info(f"[URL UPDATE]: Error: {e}")
        
def update_fail(url: str, db_name: str = "url_db", collection_name: str = "urls"):
    try:
        db = mongo_client[db_name]
        collection = db[collection_name]

        existing_url = collection.find_one({"url": url})
        if existing_url:
            logging.info(f"[URL FAIL]: Updating status for {url}...")
            collection.update_one({"url": url}, {"$set": {"crawled": False}, "$inc": {"retries": 1}})
    
    except ConnectionFailure:
        logging.info("[URL FAIL]: CosmosDB connection failed.")
    except OperationFailure as e:
        logging.info(f"[URL FAIL]: Error: {e}")
        
def extract_internal_links(web_page: str):
    request_url = web_page.request.url
    logging.info(f"[LINKS EXTRACTION]: Extracting internal links...")
    domain = urlparse(request_url).netloc

    soup = BeautifulSoup(web_page.content, "html.parser", from_encoding = "iso-8859-1")
    urls = set()

    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        href = urljoin(request_url, href).strip("/")

        if href == "" or href is None:
            continue

        if urlparse(href).netloc != domain:
            continue

        urls.add(href)
        
    return list(urls)
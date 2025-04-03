import azure.functions as func
import azure.durable_functions as df
from azure.data.tables import TableServiceClient, UpdateMode
from azure.storage.queue import QueueServiceClient
import logging, os, base64, json

app = df.DFApp(http_auth_level = func.AuthLevel.FUNCTION)

# Storage connection
STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
QUEUE_PREFIX = "queue-"
TABLE_NAME = "crawlerconfig"

@app.route(route = "crawling_setup")
def crawling_setup(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Setup Function triggered.")

    try:
        req_body = req.get_json()
        root_url = req_body.get("root_url")
        max_workers = int(req_body.get("max_workers", 1))
        max_depth = int(req_body.get("max_depth", 3))
        max_links = int(req_body.get("max-links"))
        max_execution_time = int(req_body.get("max-execution-time"))

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
            "max_workers": max_workers,
            "max_depth": max_depth,
            "max_links": max_links,
            "max_exec_time": max_execution_time
        }
        table_client.upsert_entity(entity, mode = UpdateMode.REPLACE)
        logging.info("Configuration saved in Table Storage.")

        # First message goes in queue-0 and init message in queue-init
        queue_client = queue_service.get_queue_client(f"{QUEUE_PREFIX}0")
        first_message = json.dumps({"url": root_url, "depth": 0})
        queue_client.send_message(first_message, visibility_timeout = 2)
        logging.info(f"Inserted root URL into queue-0: {root_url}")
        
        queue_client = queue_service.get_queue_client("queue-init")
        init_message = json.dumps({"init": "start"})
        queue_client.send_message(init_message, visibility_timeout = 5)
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


@app.queue_trigger(arg_name = "azqueue", queue_name = "queue-init", connection = "AzureWebJobsStorage", ) 
@app.durable_client_input(client_name = "client")
async def crawling_starter(azqueue: func.QueueMessage, client) -> None:    
    table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    config_table = table_service.get_table_client(TABLE_NAME)
    
    entity = config_table.get_entity(partition_key = "Config", row_key = "GlobalSettings")
    max_workers = int(entity["max_workers"])
    max_depth = int(entity["max_depth"])

    logging.info(f"Starting orchestration with {max_workers} workers.")
    instance_id = await client.start_new("orchestrator_function", None, {"max_workers": max_workers, "max_depth": max_depth})
    logging.info(f"Launched orchestration with ID = '{instance_id}'.")


@app.orchestration_trigger(context_name = "context")
def orchestrator_function(context: df.DurableOrchestrationContext):
    try:
        instance_id = context.instance_id
        logging.info(f"Starting orchestrator {instance_id}...")

        input_data = context.get_input()
        max_workers = input_data.get("max_workers")
        max_depth = input_data.get("max_depth")
        
        logging.info(f"Max workers: {max_workers}, Max depth: {max_depth}")

        queue_index = 0
        while True:
            logging.info(f"Processing queue: {queue_index}")
            urls = yield context.call_activity("queue_reader_function", {"index": queue_index, "max_workers": max_workers})
            logging.info((f"Retrieved {len(urls)} URLs from queue {queue_index}. Processing..."))

            if not urls:
                logging.info(f"Queue {queue_index} is empty. Moving to the next one.")
                queue_index += 1
                if queue_index > max_depth:
                    break
                continue

            logging.info(f"Launching url_processor...")
            tasks = [context.call_activity("url_processor_function", url) for url in urls[:max_workers]]
            results = yield context.task_all(tasks)

        logging.info("Orchestration complete.")
    except Exception as e:
        logging.error(f"Orchestrator {instance_id} - ERROR: {str(e)}")
        raise


@app.activity_trigger(input_name = "inputdata")
def queue_reader_function(inputdata: dict) -> list:
    index = inputdata.get('index')
    max_workers = inputdata.get('max_workers')
    
    queue_name = f"queue-{index}"
    
    queue_service = QueueServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    queue_client = queue_service.get_queue_client(queue_name)
    
    logging.info(f"Looking for messages from queue: {queue_name}")
    messages = list(queue_client.receive_messages(visibility_timeout = 30, max_messages = max_workers))
    
    if messages:
        logging.info(f"Received {len(messages)} messages from queue {queue_name}.")
        urls = [msg.content for msg in messages]
        for msg in messages:
            queue_client.delete_message(msg)
        return urls
    else:
        logging.info(f"No messages found in queue {queue_name}.")
        return []
    
    
@app.activity_trigger(input_name = "url")
def url_processor_function(url: str) -> str:    
    decoded_url = decode_base64(url)
    logging.info(f"Processing URL: {decoded_url.decode('utf-8')}")
    return f"Processed {decoded_url.decode('utf-8')}"

def decode_base64(data):
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    return base64.b64decode(data)
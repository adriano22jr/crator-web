import logging, os, json
import azure.functions as func
from azure.storage.queue import QueueServiceClient
from azure.data.tables import TableServiceClient, UpdateMode


# Storage connection
STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
QUEUE_PREFIX = "queue-"
TABLE_NAME = "crawlerconfig"

def main(req: func.HttpRequest) -> func.HttpResponse:
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

        for i in range(max_depth + 1):
            queue_name = f"{QUEUE_PREFIX}{i}"
            try:
                queue_service.create_queue(queue_name)
                logging.info(f"Created queue for level: {queue_name}")
            except:
                logging.info(f"Queue {QUEUE_PREFIX}{i} already exists.")
            
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

        # First message goes in queue-0
        queue_client = queue_service.get_queue_client(f"{QUEUE_PREFIX}0")
        first_message = json.dumps({"url": root_url, "depth": 0})
        queue_client.send_message(first_message)
        logging.info(f"Inserted root URL into queue-0: {root_url}")
        
        return func.HttpResponse("Setup completed successfully!", status_code = 200)

    except Exception as e:
        logging.error(f"Error in setup function: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code = 500)

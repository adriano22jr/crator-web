import os
import logging
import azure.functions as func
from azure.storage.queue import QueueServiceClient
from azure.data.tables import TableServiceClient

STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
QUEUE_PREFIX = "queue-"
TABLE_NAME = "crawlerconfig"

def main(req):
    try:
        partition_key = "Config"
        row_key = "GlobalSettings"
        
        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        table_client = table_service.get_table_client(TABLE_NAME)
        
        entity = table_client.get_entity(partition_key = partition_key, row_key = row_key)
        max_depth = int(entity["max_depth"])

        logging.info(f"max_depth found: {max_depth}")

        queue_service = QueueServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        
        for i in range(max_depth + 1):
            queue_name = f"{QUEUE_PREFIX}{i}"
            logging.info(f"Eliminazione della coda: {queue_name}")
            queue_service.delete_queue(queue_name)

        table_service = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        logging.info(f"Eliminazione della tabella: {TABLE_NAME}")
        table_service.delete_table(TABLE_NAME)
        
        return func.HttpResponse("Code e tabella eliminate con successo.", status_code = 200)
    
    except Exception as e:
        logging.error(f"Errore: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code = 500)

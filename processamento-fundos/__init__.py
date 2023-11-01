import logging

import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage, BinaryBase64DecodePolicy, BinaryBase64EncodePolicy
import base64

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Iniciando processo de adição de mensagem na fila')

    connect_str = "DefaultEndpointsProtocol=https;AccountName=functionspythonfram8ffb;AccountKey=a1vrSV+WzPqf0PLv7RlNtRsb0JpOsNOHsTCDuLKoxrh69dIDstM1WgY5ydR5ViyrVgPFPfCxe0a4+ASt+TK9XA==;EndpointSuffix=core.windows.net"

    req_body = req.get_body()

    logging.info(req_body)
    
    queue_name = "processamento-de-fundos"
    queue_client = QueueClient.from_connection_string(connect_str, queue_name)

    print("\nAdicionando mensagem...")

    queue_client.message_encode_policy = BinaryBase64EncodePolicy()
    
    message_bytes = req_body.encode('utf-8')
    # base64_message = base64.b64encode(message_bytes).decode('utf-8')

    # queue_client.send_message(base64_message)

    queue_client.send_message(queue_client.message_encode_policy.encode(content=message_bytes))
    
    return func.HttpResponse(
            "This HTTP triggered function executed successfully",
            status_code=200
    )

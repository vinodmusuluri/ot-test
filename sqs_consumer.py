import boto3
import logging
import json
import global_properties as prop

class SQSConsumer:
    def __init__(self):
        self.logger = logging.getLogger("SQSConsumer")
        self.sqs_client = boto3.client('sqs', region_name=prop.aws_region)

    def fetch_messages(self):
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=prop.queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10
            )
            return response.get("Messages", [])
        except Exception as e:
            self.logger.error(f"Failed to fetch messages: {e}")
            return []

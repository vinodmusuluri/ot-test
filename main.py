import logging
from sqs_consumer import SQSConsumer
from transformer import Transformer
from snowflake_writer import SnowflakeWriter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataPipeline")

def main():
    try:
        # Initialize components
        logger.info("Starting the application.")
        consumer = SQSConsumer()
        transformer = Transformer()
        writer = SnowflakeWriter()

        while True:
            messages = consumer.fetch_messages()
            if messages:
                transformed_data = transformer.transform(messages)
                writer.write(transformed_data)
            else:
                logger.info("No messages to process. Sleeping...")

    except Exception as e:
        logger.error(f"Application failed with error: {e}")

if __name__ == "__main__":
    main()

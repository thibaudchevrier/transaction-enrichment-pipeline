"""Kafka streaming producer for transaction data.

This module implements an async Kafka producer that continuously streams
transaction data to Kafka topics, simulating real-time transaction events
with configurable intervals and batch sizes.
"""

import asyncio
import json
import logging
import os
import random
from contextlib import asynccontextmanager
from uuid import uuid4

from confluent_kafka.aio import AIOProducer
from infrastructure.generator import load_and_validate_transactions

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_kafka_producer(bootstrap_servers: str):
    """
    Async context manager for Kafka AIOProducer.

    Parameters
    ----------
    bootstrap_servers : str
        Kafka bootstrap servers (e.g., "localhost:9092").

    Yields
    ------
    AIOProducer
        Kafka async producer instance for message publishing.

    Notes
    -----
    Automatically flushes buffered messages and closes the
    producer on context exit.
    """
    producer = AIOProducer({"bootstrap.servers": bootstrap_servers})
    try:
        yield producer
        # Flush any remaining buffered messages before shutdown
        await producer.flush()
    finally:
        await producer.close()


async def produce_messages(
    producer: AIOProducer,
    topic: str,
    interval: float,
    record_samples: list[dict],
    min_records: int = 1,
    max_records: int = 10,
):
    """
    Continuously produce messages at regular intervals.

    Parameters
    ----------
    producer : AIOProducer
        Kafka async producer instance.
    topic : str
        Kafka topic name to publish messages to.
    interval : float
        Time in seconds between message batches.
    record_samples : list[dict]
        List of transaction records to sample from.
    min_records : int, optional
        Minimum number of records to send per interval, by default 1.
    max_records : int, optional
        Maximum number of records to send per interval, by default 10.

    Notes
    -----
    Messages are produced in parallel using asyncio.gather().
    Samples are selected randomly with replacement. Each produced event gets a
    fresh id, as a source system would assign one per new transaction: the
    consumer keeps UUID ids as they are, so a redelivered message deduplicates
    while two draws of the same sample stay two distinct transactions.
    No key is used, so messages distribute round-robin across partitions.
    """
    message_count = 0

    try:
        while True:
            # Randomly decide how many records to send this interval
            num_records = random.randint(min_records, max_records)

            # Randomly sample that many records (with replacement)
            samples = random.choices(record_samples, k=num_records)

            # Create tasks to send all sampled records in parallel
            async def send_message(sample, msg_num):
                try:
                    # Produce message (no key - no transaction event ordering required)
                    # Kafka will distribute messages round-robin across all partitions
                    event = {**sample, "id": str(uuid4())}
                    delivery_future = await producer.produce(topic, value=json.dumps(event).encode("utf-8"))
                    await delivery_future
                    logger.debug(f"Produced message #{msg_num} to topic '{topic}'")
                    return True
                except Exception as e:
                    logger.error(f"Failed to produce message #{msg_num}: {e}")
                    return False

            # Send all messages in parallel
            tasks = [send_message(sample, message_count + i + 1) for i, sample in enumerate(samples)]
            results = await asyncio.gather(*tasks)

            # Update message count
            successful = sum(results)
            message_count += num_records

            logger.info(f"Batch complete: sent {successful}/{num_records} records")

            # Wait for the specified interval
            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        logger.info(f"Producer stopped. Total messages sent: {message_count}")
        raise


async def main():
    """
    Execute Kafka producer workflow.

    This async function orchestrates the producer lifecycle:
    - Loads configuration from environment variables
    - Loads and validates all transactions from S3/MinIO
    - Creates Kafka producer connection
    - Continuously streams messages at configured intervals

    Environment Variables
    ---------------------
    KEY : str
        MinIO access key (required).
    SECRET : str
        MinIO secret key (required).
    ENDPOINT_URL : str
        MinIO endpoint URL (required).
    KAFKA_BOOTSTRAP_SERVERS : str
        Kafka bootstrap servers (default: 'localhost:9092').
    KAFKA_TOPIC : str
        Target Kafka topic (default: 'transactions').
    PRODUCE_INTERVAL : float
        Seconds between message batches (default: 0.5).
    MIN_RECORDS_PER_BATCH : int
        Minimum records per batch (default: 1).
    MAX_RECORDS_PER_BATCH : int
        Maximum records per batch (default: 10).

    Notes
    -----
    Loads all transactions into memory for random sampling.
    Logs invalid transactions but doesn't produce them.
    Runs indefinitely until interrupted (KeyboardInterrupt).
    """
    # Configuration
    # Read and validate CSV from MinIO
    s3_path = "s3://transactions/transactions_fr.csv"

    storage_options = {
        "key": os.environ["KEY"],
        "secret": os.environ["SECRET"],
        "client_kwargs": {"endpoint_url": os.environ["ENDPOINT_URL"]},
    }
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "transactions")
    interval = float(os.getenv("PRODUCE_INTERVAL", "0.5"))  # Default: 0.5 second
    min_records = int(os.getenv("MIN_RECORDS_PER_BATCH", "1"))
    max_records = int(os.getenv("MAX_RECORDS_PER_BATCH", "10"))
    run_id = os.getenv("PRODUCER_RUN_ID", "producer_unknown")

    valid_transactions, invalid_transactions = next(
        load_and_validate_transactions(
            s3_path=s3_path,
            storage_options=storage_options,
            batch_size=10000,  # Load one at a time for sampling
            run_id=run_id,
            processing_type="streaming",
        )
    )

    logger.info(f"Starting producer - Server: {bootstrap_servers}, Topic: {topic}, Interval: {interval}s")
    logger.info(f"Records per batch: {min_records}-{max_records}, Total samples loaded: {len(valid_transactions)}")

    if invalid_transactions:
        logger.warning(
            f"Loaded {len(invalid_transactions)} invalid transactions during validation - "
            f"these will be ignored for production"
        )

    async with get_kafka_producer(bootstrap_servers) as producer:
        await produce_messages(producer, topic, interval, valid_transactions, min_records, max_records)


if __name__ == "__main__":
    asyncio.run(main())

from datetime import datetime
import json
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
from utils.constants import KAFKA_BROKER, TOPIC
from threading import Thread
from app.models import Stock
from sqlalchemy.orm import Session
from core.database import get_db

BATCH_SIZE = 50

# Consumer Function - Receives data from Kafka and processes it
def consumerDB(consumer_id, db: Session):
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'db_consumers',  # Unique group ID for all consumers
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)

    batch = []

    try:
        consumer.subscribe([TOPIC])

        while True:
            msg = consumer.poll(1.0)  # Poll with 1 second timeout
            if msg is None:
                continue  # No message received within timeout period
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Consumer {consumer_id} reached end of partition.")
                    continue
                else:
                    raise KafkaException(msg.error())

            # Check if message is empty before decoding
            if msg.value():
                try:
                    stock_data = json.loads(msg.value().decode('utf-8'))
                    batch.append(stock_data)
                    print(f"Consumer {consumer_id} received: {stock_data}")
                except json.JSONDecodeError:
                    print(f"Consumer {consumer_id} received invalid JSON.")

                if len(batch) >= BATCH_SIZE:
                    print(f"Consumer {consumer_id} processing batch of {len(batch)}")
                    save_to_db(db, batch)  # Save accumulated data to DB
                    batch = []  # Reset the batch after saving

            else:
                print(f"Consumer {consumer_id} received an empty message.")
    except KeyboardInterrupt:
        print(f"Stopping consumer {consumer_id}...")
    finally:
        consumer.close()  # Close consumer to commit offsets

# Save batch of data to the database
def save_to_db(db: Session, batch: list):
    retries = 3
    for attempt in range(retries):
        try:
            db_stocks = []
        
            for stock in batch:
                # Convert Unix timestamp to datetime
                created_at = datetime.fromtimestamp(stock['created_at'])
                db_stock = Stock(
                    ticker=stock['ticker'], 
                    price=stock['price'],
                    volume=stock['volume'],
                    created_at=created_at
                )
                db_stocks.append(db_stock)
            
            db.bulk_save_objects(db_stocks)  # Efficient bulk insert
            db.commit()  # Commit the transaction
            print(f"Inserted {len(batch)} records into DB.")
            break
        except Exception as e:
            db.rollback()
            if attempt < retries - 1:
                print(f"Retrying due to error: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Error saving data to DB: {e}")


# Start multiple consumers, each in its own thread
def start_consumer_db():

    threads = []
    num_consumers = 3  # Define how many consumers you want

    for i in range(num_consumers):
        db_session = next(get_db())  # Get DB session from the generator
        thread = Thread(target=consumerDB, args=(i, db_session))
        thread.daemon = True  # Daemon thread to exit when the main program ends
        thread.start()
        threads.append(thread)
        print(f"Started consumer thread {i}")

    # Wait for all threads to finish (which will not happen unless the process is stopped)
    for thread in threads:
        thread.join()



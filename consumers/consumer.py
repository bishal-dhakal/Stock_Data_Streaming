import json
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
from datetime import datetime
from threading import Thread
from app.models import Stock
from sqlalchemy.orm import Session
from core.database import get_db
import logging
from utils.constants import KAFKA_BROKER, TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 50

# This consumer saves data to PostgreSQL for persistence and preservation.
def consumerDB(consumer_id, db: Session):
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'db_consumers',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)

    batch = []

    try:
        consumer.subscribe([TOPIC])

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue 
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Consumer {consumer_id} reached end of partition.")
                    continue
                else:
                    raise KafkaException(msg.error())

            if msg.value():
                try:
                    stock_data = json.loads(msg.value().decode('utf-8'))
                    batch.append(stock_data)
                    logger.info(f"Consumer {consumer_id} received: {stock_data}")
                except json.JSONDecodeError:
                    logger.warning(f"Consumer {consumer_id} received invalid JSON.")

                if len(batch) >= BATCH_SIZE:
                    save_to_db(db, batch)
                    batch = []  

            else:
                logger.warning(f"Consumer {consumer_id} received an empty message.")
    except KeyboardInterrupt:
        logger.info(f"Stopping consumer {consumer_id}...")
    finally:
        consumer.close()

def save_to_db(db: Session, batch: list):
    retries = 3
    for attempt in range(retries):
        try:
            db_stocks = []
        
            for stock in batch:
                created_at = datetime.fromtimestamp(stock['created_at'])
                db_stock = Stock(
                    ticker=stock['ticker'], 
                    price=stock['price'],
                    volume=stock['volume'],
                    created_at=created_at
                )
                db_stocks.append(db_stock)
            
            db.bulk_save_objects(db_stocks) 
            db.commit() 
            break
        except Exception as e:
            db.rollback()
            if attempt < retries - 1:
                logger.warning(f"Retrying due to error: {e}")
                time.sleep(2 ** attempt) 
            else:
                logger.error(f"Error saving data to DB: {e}")

def main():
    while True:
        threads = []
        num_consumers = 3

        for i in range(num_consumers):
            db_session = next(get_db())
            thread = Thread(target=consumerDB, args=(i, db_session))
            thread.daemon = True
            thread.start()
            threads.append(thread)
            logger.info(f"Started consumer thread {i}")

        for thread in threads: thread.join()

if __name__ == "__main__":
    main()

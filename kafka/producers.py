import json
import random
import time
from confluent_kafka import Producer
from threading import Thread
# from utils.utils import STOCK_PARTITIONS
# from utils.constants import STOCKS ,KAFKA_BROKER, TOPIC

from utils.constants import STOCKS, KAFKA_BROKER, TOPIC
from utils.utils import STOCK_PARTITIONS

def produce_stock_data(stock):
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(conf)
    partition = STOCK_PARTITIONS[stock]

    try:
        while True:
            stock_data = {
                "ticker": stock,
                "price": round(random.uniform(100, 1500), 2),
                "volume": random.randint(100, 10000),
                "created_at": time.time()
            }
            # Produce message to the correct partition
            producer.produce(
                TOPIC,
                key=stock,
                value=json.dumps(stock_data),
                partition=partition
            )
            print(f"Produced to Partition {partition}: {stock_data}")
            producer.flush()  # Ensure the message is sent immediately
            time.sleep(random.uniform(0.5, 1.5))  # Simulate varying producer rates
    except KeyboardInterrupt:
        print(f"Stopping producer for {stock}...")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting

def main():
    while True:
        threads = []
        for stock in STOCKS:
            thread = Thread(target=produce_stock_data, args=(stock,))
            thread.start()
            threads.append(thread)
            print(f"Started producer thread for {stock}")

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        while any(thread.is_alive() for thread in threads):
            time.sleep(1)
            print("Waiting for producer threads to finish...")

if __name__ == "__main__":
    main()
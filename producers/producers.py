import json
import random
import time
from confluent_kafka import Producer
from threading import Thread
from utils.constants import STOCKS, KAFKA_BROKER, TOPIC
from utils.utils import STOCK_PARTITIONS


def json_serializer(obj):
    return json.dumps(obj).encode("utf-8")


def produce_stock_data(stock):
    conf = {"bootstrap.servers": KAFKA_BROKER}
    producer = Producer(conf)
    partition = STOCK_PARTITIONS[stock]

    try:
        while True:
            stock_data = {
                "ticker": stock,
                "price": round(random.uniform(100, 1500), 2),
                "volume": random.randint(100, 10000),
                "created_at": time.time(),
            }

            serialized_key = json_serializer(stock)
            serialized_value = json_serializer(stock_data)

            producer.produce(
                TOPIC, key=serialized_key, value=serialized_value, partition=partition
            )
            print(f"Data : Partition -> {partition}: {stock_data}")
            producer.flush()
            time.sleep(random.uniform(10, 15))
    except KeyboardInterrupt:
        print(f"Stopping producer for {stock}...")
    finally:
        producer.flush()


def main():
    while True:
        threads = []
        for stock in STOCKS:
            thread = Thread(target=produce_stock_data, args=(stock,))
            thread.start()
            threads.append(thread)
            print(f"Started producer thread for {stock}")

        # for thread in threads:
        #     thread.join()

        while any(thread.is_alive() for thread in threads):
            time.sleep(1)
            print("Waiting for producer threads to finish...")


if __name__ == "__main__":
    main()

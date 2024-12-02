from fastapi import FastAPI
from app.routes import router
from threading import Thread
from kafka.consumerdb import start_consumer_db
from core.database import Base, engine
import uvicorn
import signal
import sys
from spark.consumerspark import Consumer_Spark

app = FastAPI()

Base.metadata.create_all(bind=engine)

consumer_thread = Thread(target=start_consumer_db)
consumer_thread.start()
print("Started Kafka consumer threads.")

Consumer_Spark()

app.include_router(router)

def signal_handler(sig, frame):
    print("Caught signal. Shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    uvicorn.run("main:app", host="0.0.0.0", port=8000)
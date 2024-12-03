from fastapi import FastAPI
from app.routes import router
from core.database import Base, engine
import uvicorn
import signal
import sys
from spark.consumers import ConsumerSpark

app = FastAPI()

Base.metadata.create_all(bind=engine)

consumer = ConsumerSpark()
consumer.start_streaming()

app.include_router(router)

def signal_handler(sig, frame):
    print("Caught signal. Shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    uvicorn.run("main:app", host="0.0.0.0", port=8000)
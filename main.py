import signal
import sys
from threading import Thread

import uvicorn
from fastapi import FastAPI

from core.database import Base, engine
from spark.consumers import ConsumerSpark

app = FastAPI()

Base.metadata.create_all(bind=engine)

consumer = ConsumerSpark()


def signal_handler(sig, frame):
    """
    Gracefully handle termination signals (SIGINT, SIGTERM) to shut down the application.
    """
    print("Caught signal. Shutting down...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    uvicorn.run("main:app", host="0.0.0.0", port=8000)

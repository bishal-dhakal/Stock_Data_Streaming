from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
import asyncio
import uvicorn
from datetime import datetime
from typing import Dict, List

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RedisStreamManager:
    def __init__(self):
        self.redis_client = redis.StrictRedis(
            host='localhost', 
            port=6379, 
            db=0,
            decode_responses=True  
        )
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    def get_historical_data(self, symbol: str) -> List[Dict]:
        try:
            all_data = self.redis_client.xrange(f'stock:{symbol}', '-', '+')
            return [fields for _, fields in all_data]
        except redis.RedisError as e:
            print(f"Redis error: {e}")
            return []

    async def stream_data(self, websocket: WebSocket, symbol: str):
        last_id = '0'
        try:
            while True:
                try:
                    streams = {f'stock:{symbol}': last_id}
                    response = self.redis_client.xread(streams, block=100, count=100)
                    
                    if response:
                        for stream_name, messages in response:
                            for message_id, data in messages:
                                last_id = message_id
                                await websocket.send_json({
                                    'id': message_id,
                                    'data': data,
                                    'timestamp': datetime.now().isoformat()
                                })
                finally:
                    pass
                
                await asyncio.sleep(0.1)
                
        except Exception as e:
            print(f"Streaming error: {e}")
            self.disconnect(websocket)

manager = RedisStreamManager()

@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await manager.connect(websocket)
    try:
        # Send historical data first
        historical_data = manager.get_historical_data(symbol)
        await websocket.send_json({
            'type': 'historical',
            'data': historical_data
        })
        
        # Start streaming new data
        await manager.stream_data(websocket, symbol)
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5000)
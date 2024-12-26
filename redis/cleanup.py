from typing import Optional
import redis
from datetime import datetime, timedelta
import logging
import time
from prometheus_client import Counter, Gauge

class StreamManager:
    def __init__(self, host: str, port: int, db: int):
        self.redis_pool = redis.ConnectionPool(
            host=host, 
            port=port, 
            db=db,
            max_connections=10,
            socket_timeout=5.0
        )
        self.redis = redis.Redis(connection_pool=self.redis_pool)
        
        # Metrics
        self.messages_processed = Counter('stream_messages_processed', 'Messages processed')
        self.stream_size = Gauge('stream_size', 'Current stream size')
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def trim_stream(self, stream_key: str, max_len: int = 1000000, max_age: timedelta = timedelta(days=1)):
        try:
            # Trim by length
            self.redis.xtrim(stream_key, maxlen=max_len)
            
            # Trim by age
            cutoff = datetime.now() - max_age
            cutoff_ms = int(cutoff.timestamp() * 1000)
            self.redis.xtrim(stream_key, minid=cutoff_ms)
            
            # Update metrics
            current_len = self.redis.xlen(stream_key)
            self.stream_size.set(current_len)
            
            self.logger.info(f"Trimmed stream {stream_key} to {current_len} entries")
            
        except redis.RedisError as e:
            self.logger.error(f"Failed to trim stream {stream_key}: {e}")
            raise

    def monitor_health(self) -> bool:
        try:
            return self.redis.ping()
        except redis.RedisError:
            return False

    def cleanup_streams(self):
        """Periodic cleanup of all stock streams"""
        try:
            streams = self.redis.keys('stock:*')
            for stream in streams:
                self.trim_stream(stream.decode())
        except redis.RedisError as e:
            self.logger.error(f"Cleanup failed: {e}")
            raise
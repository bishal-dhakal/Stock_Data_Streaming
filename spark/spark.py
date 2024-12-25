import redis
import json

# Setup Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def fetch_and_print_data():
    try:
        # Fetch historical data for the whole day from Redis stream
        all_data = redis_client.xrange('stock:GOOGL', '-', '+')  # Modify to your Redis key if necessary
        if not all_data:
            print("No data in Redis for stock:GOOGL")
        else:
            print("Historical Data for the whole day:")
            for data in all_data:
                # Redis data is returned as a tuple (ID, [field-value pairs]).
                # We need to decode the data from byte strings and parse it as JSON.
                stream_id, fields = data
                message = json.loads(fields[0][1].decode('utf-8'))  # Decoding the message part
                print(message)  # Print the historical data

        # Listen for new data and print it as it arrives
        print("\nListening for new data...")
        while True:
            # Continuously check for new data in the stream
            entries = redis_client.xread({'stock:GOOGL': '0'}, block=0, count=1)
            for entry in entries:
                # Extract and print the new data
                stream_id, fields = entry
                message = json.loads(fields[0][1].decode('utf-8'))
                print("New Data: ", message)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_and_print_data()

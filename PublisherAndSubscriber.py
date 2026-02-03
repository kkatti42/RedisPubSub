import redis
import json
import sys
import threading
import time

# Command-line Arguments
if len(sys.argv) != 3:
    print("Usage: python pubsub_dual_role.py <channelName> <cacheKey>")
    sys.exit(1)

channel_name = sys.argv[1]
target_cache_key = sys.argv[2]

# ⚙️ Redis Connections
r_sub = redis.Redis(host='localhost', port=6379)
r_pub = redis.Redis(host='localhost', port=6379)

# Subscriber Thread
def subscriber():
    pubsub = r_sub.pubsub()
    pubsub.subscribe(channel_name)
    print(f"✅ Subscribed to '{channel_name}', listening for messages...")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                print(f"Received message: {data}")

                if data.get("invalidate") and data.get("cacheKey") == target_cache_key:
                    print(f"Cache invalidation triggered for '{target_cache_key}'")
                    # Publish an acknowledgment message
                    ack = {
                        "ack": True,
                        "cacheKey": target_cache_key,
                        "timestamp": time.time()
                    }
                    print("\n")
                    print("Publishing ack on successful cache invalidation")
                    r_pub.publish(channel_name, json.dumps(ack))
                elif data.get("ack") and data.get("cacheKey") == target_cache_key:
                    print(f"Received ack for cacheKey '{target_cache_key}' at {data['timestamp']}")
                else:
                    print("ℹIgnored: Not matching or no invalidate flag\n")

            except Exception as e:
                print(f"Failed to parse message: {e}")

# Publisher Function
def publisher():
    time.sleep(2)  # wait for subscriber to start
    message = {
        "cacheKey": target_cache_key,
        "invalidate": True
    }
    print(f"✅ Publishing message: {message}, to '{channel_name}'")
    r_pub.publish(channel_name, json.dumps(message))

# Run Threads
t = threading.Thread(target=subscriber)
t.start()

publisher()

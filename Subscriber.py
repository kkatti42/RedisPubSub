import redis
import json
import sys

# --- Command-line arguments ---
if len(sys.argv) != 3:
    print("Usage: python subscriber_with_args.py <channelName> <cacheKey>")
    sys.exit(1)

channel_name = sys.argv[1]
target_cache_key = sys.argv[2]

# --- Redis setup ---
r = redis.Redis(host='localhost', port=6379)
pubsub = r.pubsub()
pubsub.subscribe(channel_name)

print(f"✅ Listening for messages on '{channel_name}'...")
print(f"🔍 Will invalidate only if cacheKey == '{target_cache_key}'")

# --- Message loop ---
for message in pubsub.listen():
    if message['type'] == 'message':
        try:
            data = json.loads(message['data'])
            print(f"\n📩 Received message: {data}")

            if data.get("invalidate") and data.get("cacheKey") == target_cache_key:
                print(f"🚀 Cache invalidation triggered for key '{target_cache_key}'!")
            else:
                print(f"ℹ️ Message ignored (different cacheKey or invalidate=false).")

        except Exception as e:
            print(f"❌ Failed to parse message: {e}")

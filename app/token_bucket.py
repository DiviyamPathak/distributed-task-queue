import time
import redis
from pathlib import Path

# Redis connection
r = redis.Redis(host="redis", port=6379, db=0)

# Load Lua script once at startup
LUA_SCRIPT_PATH = Path(__file__).with_name("token_bucket.lua")

with open(LUA_SCRIPT_PATH, "r") as f:
    TOKEN_BUCKET_LUA = f.read()

# Optional: cache script in Redis and use SHA for faster execution
TOKEN_BUCKET_SHA = r.script_load(TOKEN_BUCKET_LUA)


def try_consume(tenant_id: str, requested: int = 1) -> bool:
    max_tokens = 50
    refill_rate = 10
    now = int(time.time())

    result = r.evalsha(
        TOKEN_BUCKET_SHA,
        2,
        f"tenant:{tenant_id}:tokens",
        f"tenant:{tenant_id}:last_refill",
        max_tokens,
        refill_rate,
        now,
        requested
    )

    return result == 1

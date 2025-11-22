import time
import redis

r = redis.Redis(host="redis", port=6379, db=0)

TOKEN_BUCKET_LUA = """
local key_tokens = KEYS[1]
local key_time = KEYS[2]

local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- read state or initialize
local tokens = tonumber(redis.call("GET", key_tokens) or max_tokens)
local last = tonumber(redis.call("GET", key_time) or now)

-- refill
local delta = math.max(0, now - last)
local refill = math.floor(delta * refill_rate)
tokens = math.min(max_tokens, tokens + refill)

-- check quota
if tokens < requested then
  redis.call("SET", key_tokens, tokens)
  redis.call("SET", key_time, now)
  return 0
else
  tokens = tokens - requested
  redis.call("SET", key_tokens, tokens)
  redis.call("SET", key_time, now)
  return 1
end
"""

def try_consume(tenant_id: str, requested: int = 1):
    max_tokens = 50          
    refill_rate = 10         
    now = int(time.time())

    return r.eval(
        TOKEN_BUCKET_LUA,
        2,
        f"tenant:{tenant_id}:tokens",
        f"tenant:{tenant_id}:last_refill",
        max_tokens, refill_rate, now, requested
    ) == 1

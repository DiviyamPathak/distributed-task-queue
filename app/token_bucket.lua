-- token_bucket.lua

local key_tokens = KEYS[1]
local key_time   = KEYS[2]

local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now        = tonumber(ARGV[3])
local requested  = tonumber(ARGV[4])

local tokens = tonumber(redis.call("GET", key_tokens) or max_tokens)
local last   = tonumber(redis.call("GET", key_time) or now)

local delta = math.max(0, now - last)
local refill = math.floor(delta * refill_rate)
tokens = math.min(max_tokens, tokens + refill)

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

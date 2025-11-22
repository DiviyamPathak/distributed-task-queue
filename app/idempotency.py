import redis
import os
from typing import Optional

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

def claim_once(client_request_id: str, ttl_seconds: int = 3600) -> bool:
    """
    Try to claim a client_request_id. Return True if claim successful (first time),
    False if duplicate.
    """
    key = f"idempotency:{client_request_id}"
    # SETNX: set if not exists
    was_set = r.setnx(key, "1")
    if was_set:
        r.expire(key, ttl_seconds)
        return True
    return False

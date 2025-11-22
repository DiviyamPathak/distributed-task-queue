from fastapi import HTTPException
from .token_bucket import try_consume

def enforce_quota():
    def decorator(func):
        async def wrapper(payload, *args, **kwargs):
            tenant = payload.tenant_id

            allowed = try_consume(tenant, requested=1)
            if not allowed:
                raise HTTPException(
                    status_code=429,
                    detail=f"Tenant {tenant} is over quota"
                )
            return await func(payload, *args, **kwargs)
        return wrapper
    return decorator

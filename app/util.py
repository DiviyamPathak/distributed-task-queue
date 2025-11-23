import uuid
from typing import Dict

def gen_client_request_id(tenant_id: str, tag: str = "") -> str:
    return f"{tenant_id}:{tag}:{uuid.uuid4().hex}"

def tenants():
    return [
        {"tenant_id": "tenantA", "name": "Tenant A"},
        {"tenant_id": "tenantB", "name": "Tenant B"},
        {"tenant_id": "tenantC", "name": "Tenant C"},
    ]

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from .tasks import ingest_csv, generate_report, send_email, deliver_webhook
from .util import gen_client_request_id, tenants
from .rate_limiter import enforce_quota
from typing import Optional

app = FastAPI(title="Multi-tenant Task API")

class EnqueueIngest(BaseModel):
    tenant_id: str
    s3_path: str
    client_request_id: Optional[str] = None

class EnqueueReport(BaseModel):
    tenant_id: str
    report_type: str
    client_request_id: Optional[str] = None

class EnqueueEmail(BaseModel):
    tenant_id: str
    to: str
    subject: str
    body: str
    client_request_id: Optional[str] = None

class EnqueueWebhook(BaseModel):
    tenant_id: str
    url: str
    payload: dict
    client_request_id: Optional[str] = None

@enforce_quota()
@app.get("/tenants")
def list_tenants():
    return tenants()

@enforce_quota()
@app.post("/enqueue/ingest")
def api_ingest(payload: EnqueueIngest):
    client_id = payload.client_request_id or gen_client_request_id(payload.tenant_id, "ingest")
    task = ingest_csv.apply_async(kwargs={
        "tenant_id": payload.tenant_id,
        "s3_path": payload.s3_path,
        "client_request_id": client_id
    })
    return {"task_id": task.id, "client_request_id": client_id}

@enforce_quota()
@app.post("/enqueue/report")
def api_report(payload: EnqueueReport):
    client_id = payload.client_request_id or gen_client_request_id(payload.tenant_id, "report")
    task = generate_report.apply_async(kwargs={
        "tenant_id": payload.tenant_id,
        "report_type": payload.report_type,
        "client_request_id": client_id
    })
    return {"task_id": task.id, "client_request_id": client_id}

@enforce_quota()
@app.post("/enqueue/email")
def api_email(payload: EnqueueEmail):
    client_id = payload.client_request_id or gen_client_request_id(payload.tenant_id, "email")
    task = send_email.apply_async(kwargs={
        "tenant_id": payload.tenant_id,
        "to": payload.to,
        "subject": payload.subject,
        "body": payload.body,
        "client_request_id": client_id
    })
    return {"task_id": task.id, "client_request_id": client_id}

@enforce_quota()
@app.post("/enqueue/webhook")
def api_webhook(payload: EnqueueWebhook):
    client_id = payload.client_request_id or gen_client_request_id(payload.tenant_id, "webhook")
    task = deliver_webhook.apply_async(kwargs={
        "tenant_id": payload.tenant_id,
        "url": payload.url,
        "payload": payload.payload,
        "client_request_id": client_id
    })
    return {"task_id": task.id, "client_request_id": client_id}

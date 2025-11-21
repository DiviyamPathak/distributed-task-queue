from .celery_app import app
from .idempotency import claim_once
import time
import random
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

@app.task(bind=True)
def ingest_csv(self, tenant_id: str, s3_path: str, client_request_id: str = None):
    # idempotency
    if client_request_id and not claim_once(client_request_id):
        logger.info("Duplicate ingest_csv: %s", client_request_id)
        return {"status": "duplicate", "task": "ingest_csv"}

    # simulate ingest work
    logger.info("[%s] ingest_csv started for %s", tenant_id, s3_path)
    time.sleep(2 + random.random() * 2)
    rows = random.randint(100, 2000)
    logger.info("[%s] ingest_csv finished rows=%s", tenant_id, rows)
    return {"status": "ok", "rows": rows}

@app.task(bind=True)
def generate_report(self, tenant_id: str, report_type: str, client_request_id: str = None):
    if client_request_id and not claim_once(client_request_id):
        logger.info("Duplicate generate_report: %s", client_request_id)
        return {"status": "duplicate", "task": "generate_report"}

    logger.info("[%s] generate_report type=%s", tenant_id, report_type)
    time.sleep(1 + random.random() * 3)
    # pretend we generated a report url
    report_url = f"https://example.com/reports/{tenant_id}/{self.request.id}.pdf"
    logger.info("[%s] generated %s", tenant_id, report_url)
    return {"status": "ok", "url": report_url}

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def send_email(self, tenant_id: str, to: str, subject: str, body: str, client_request_id: str = None):
    if client_request_id and not claim_once(client_request_id):
        logger.info("Duplicate send_email: %s", client_request_id)
        return {"status": "duplicate", "task": "send_email"}

    logger.info("[%s] send_email to=%s subj=%s", tenant_id, to, subject)
    # simulate flaky delivery
    if random.random() < 0.2:
        logger.warning("Simulated SMTP error, retrying...")
        raise self.retry(exc=Exception("SMTP temporary error"))
    time.sleep(0.5)
    logger.info("[%s] email sent", tenant_id)
    return {"status": "sent"}

@app.task(bind=True)
def deliver_webhook(self, tenant_id: str, url: str, payload: dict, client_request_id: str = None):
    if client_request_id and not claim_once(client_request_id):
        logger.info("Duplicate deliver_webhook: %s", client_request_id)
        return {"status": "duplicate", "task": "deliver_webhook"}

    logger.info("[%s] deliver_webhook -> %s", tenant_id, url)
    # simulate network call and backoff only here (no real HTTP to keep baseline)
    time.sleep(0.5 + random.random())
    # random fail to show retries in Flower if desired
    if random.random() < 0.1:
        raise Exception("Webhook delivery failed")
    return {"status": "delivered"}

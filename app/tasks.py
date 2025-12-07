from app.celery_app import app
from .idempotency import claim_once
from .db import get_conn

import csv
import os
import httpx
import smtplib
from email.message import EmailMessage
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

FAKE_S3_DIR = "/app/data"  

@app.task(bind=True)
def ingest_csv(self, tenant_id: str, s3_path: str, source: str, client_request_id: str = None):
    if client_request_id and not claim_once(client_request_id):
        return {"status": "duplicate"}

    file_path = os.path.join(FAKE_S3_DIR, s3_path)

    if not os.path.exists(file_path):
        return {"status": "error", "reason": "file_not_found"}

    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    failed = 0

    with open(file_path, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                txn_id = row.get("upi_txn_id") or row.get("imps_txn_id") or row.get("neft_txn_id") or row.get("transaction_reference")
                amount = float(row.get("amount", 0))

                cur.execute("""
                    INSERT INTO fintech_transactions
                    (tenant_id, source, txn_id, reference_id, account_id,
                     counterparty_account, vpa_or_ifsc, amount, currency,
                     status, txn_timestamp, raw_row)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    tenant_id,
                    source,
                    txn_id,
                    row.get("rrn") or row.get("utr") or row.get("stan"),
                    row.get("payer_vpa") or row.get("account_number") or row.get("sender_account"),
                    row.get("payee_vpa") or row.get("beneficiary_account") or row.get("receiver_account"),
                    row.get("ifsc"),
                    amount,
                    row.get("currency", "INR"),
                    row.get("txn_status") or row.get("status"),
                    row.get("txn_timestamp") or row.get("txn_date"),
                    row
                ))
                inserted += 1
            except Exception:
                failed += 1

    conn.commit()
    conn.close()

    logger.info("[%s] Fintech CSV ingested | inserted=%s failed=%s", tenant_id, inserted, failed)

    return {"status": "ok", "inserted": inserted, "failed": failed}

@app.task(bind=True)
def reconcile_transactions(self, tenant_id: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT txn_id, amount, status
        FROM fintech_transactions
        WHERE tenant_id = %s
    """, (tenant_id,))

    rows = cur.fetchall()

    mismatches = 0

    for txn_id, amount, status in rows:
        if status != "SUCCESS":
            cur.execute("""
                INSERT INTO reconciliations
                (tenant_id, internal_txn_id, external_txn_id, mismatch_type, notes)
                VALUES (%s,%s,%s,%s,%s)
            """, (
                tenant_id,
                txn_id,
                txn_id,
                "STATUS_MISMATCH",
                f"Status = {status}"
            ))
            mismatches += 1

    conn.commit()
    conn.close()

    return {"status": "done", "mismatches": mismatches}



@app.task(bind=True)
def generate_report(self, tenant_id: str, report_type: str, client_request_id: str = None):

    if client_request_id and not claim_once(client_request_id):
        return {"status": "duplicate"}

    report_path = f"/app/reports/{tenant_id}-{self.request.id}.txt"

    os.makedirs("/app/reports", exist_ok=True)

    with open(report_path, "w") as f:
        f.write(f"Tenant: {tenant_id}\n")
        f.write(f"Report type: {report_type}\n")

    logger.info("[%s] report generated: %s", tenant_id, report_path)

    return {"status": "ok", "path": report_path}


@app.task(bind=True, max_retries=3, default_retry_delay=5)
def send_email(self, tenant_id: str, to: str, subject: str, body: str, client_request_id: str = None):

    if client_request_id and not claim_once(client_request_id):
        return {"status": "duplicate"}

    try:
        msg = EmailMessage()
        msg["From"] = "noreply@example.com"
        msg["To"] = to
        msg["Subject"] = subject
        msg.set_content(body)

        smtp = smtplib.SMTP("mailhog", 1025)  # use MailHog in docker
        smtp.send_message(msg)
        smtp.quit()

        logger.info("[%s] email sent to %s", tenant_id, to)

        return {"status": "sent"}

    except Exception as e:
        raise self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=5)
def deliver_webhook(self, tenant_id: str, url: str, payload: dict, client_request_id: str = None):

    if client_request_id and not claim_once(client_request_id):
        return {"status": "duplicate"}

    try:
        with httpx.Client(timeout=5) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()

        logger.info("[%s] webhook delivered %s", tenant_id, url)
        return {"status": "delivered"}

    except Exception as e:
        raise self.retry(exc=e)

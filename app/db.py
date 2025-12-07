import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def init_db():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fintech_transactions (
            id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            source TEXT NOT NULL,
            txn_id TEXT NOT NULL,
            reference_id TEXT,
            account_id TEXT,
            counterparty_account TEXT,
            vpa_or_ifsc TEXT,
            amount NUMERIC(18,2) NOT NULL,
            currency CHAR(3) DEFAULT 'INR',
            status TEXT,
            txn_timestamp TIMESTAMP,
            raw_row JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (tenant_id, txn_id)
        );

        CREATE INDEX IF NOT EXISTS idx_txn_tenant 
        ON fintech_transactions (tenant_id);

        CREATE INDEX IF NOT EXISTS idx_txn_status 
        ON fintech_transactions (status);

        CREATE TABLE IF NOT EXISTS reconciliations (
            id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            internal_txn_id TEXT NOT NULL,
            external_txn_id TEXT NOT NULL,
            mismatch_type TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

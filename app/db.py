import psycopg2
import os
import time
from dotenv import load_dotenv
from psycopg2 import OperationalError, pool

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

MAX_RETRIES = 10
INITIAL_DELAY = 1  # seconds

# Initialize a connection pool
try:
    connection_pool = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,  # Adjust maxconn based on your expected load
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
except OperationalError as e:
    print(f"Failed to create connection pool: {e}")
    connection_pool = None


def get_conn():
    if not connection_pool:
        raise RuntimeError("Database connection pool is not available.")
    return connection_pool.getconn()

def put_conn(conn):
    if connection_pool:
        connection_pool.putconn(conn)

def init_db():
    """
    Initialize DB schema with retry.
    Safe for Docker, Kubernetes, restarts.
    """
    conn = None
    delay = INITIAL_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = get_conn()
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

            print(" Database initialized successfully")
            return

        except OperationalError as e:
            print(
                f" DB not ready (attempt {attempt}/{MAX_RETRIES}), retrying in {delay}s... Error: {e}"
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)
        finally:
            if conn:
                put_conn(conn)

    raise RuntimeError("Failed to connect to DB")

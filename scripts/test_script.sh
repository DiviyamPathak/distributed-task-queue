#!/usr/bin/env bash
set -euo pipefail

API_URL=http://localhost:8000

echo "test starting"
docker compose down -v
docker compose up --build -d

./scripts/wait_helper.sh localhost 5432 postgres
./scripts/wait_helper.sh localhost 6379 redis
./scripts/wait_helper.sh localhost 5672 rabbitmq
./scripts/wait_helper.sh localhost 8000 fastapi

echo "Checking tenants..."
curl -fsS $API_URL/tenants | grep t1

echo "Creating CSV data..."
docker exec distributed-task-queue-api-1 mkdir -p /app/data

docker exec distributed-task-queue-api-1 bash -c 'cat > /app/data/upi_sample.csv <<EOF
upi_txn_id,rrn,payer_vpa,payee_vpa,ifsc,amount,currency,txn_status,txn_timestamp
UPI1,111,a@upi,b@upi,HDFC,100,INR,SUCCESS,2024-12-01 10:00:00
UPI2,222,c@upi,d@upi,ICICI,200,INR,FAILED,2024-12-01 10:05:00
UPI3,333,e@upi,f@upi,SBI,300,INR,PENDING,2024-12-01 10:10:00
UPI4,444,g@upi,h@upi,AXIS,400,INR,SUCCESS,2024-12-01 10:15:00
UPI5,555,i@upi,j@upi,KOTAK,500,INR,SUCCESS,2024-12-01 10:20:00
EOF'

echo "Testing ingestion..."
RESP=$(curl -fsS -X POST $API_URL/enqueue/ingest \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"t1","s3_path":"upi_sample.csv","source":"UPI"}')

echo "$RESP" | grep task_id

echo "Verifying database..."
COUNT=$(docker exec distributed-task-queue-postgres-1 \
  psql -U postgres -d mtasks -t -c "SELECT count(*) FROM fintech_transactions;" | xargs)

if [ "$COUNT" -ne 5 ]; then
  echo "Expected 5 rows, got $COUNT"
  exit 1
fi

echo "Testing reconciliation..."
curl -fsS -X POST $API_URL/enqueue/reconcile \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"t1"}' | grep mismatches

echo "Testing report generation..."
curl -fsS -X POST $API_URL/enqueue/report \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"t1","report_type":"daily_summary"}'

sleep 2
docker exec distributed-task-queue-worker-1 ls /app/reports | grep t1

echo "Testing idempotency..."
curl -fsS -X POST $API_URL/enqueue/ingest \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"t1","s3_path":"upi_sample.csv","source":"UPI","client_request_id":"fixed-id"}'

DUP=$(curl -fsS -X POST $API_URL/enqueue/ingest \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"t1","s3_path":"upi_sample.csv","source":"UPI","client_request_id":"fixed-id"}')

echo "$DUP" | grep duplicate

echo "Testing rate limiting..."
FAILS=0
for i in {1..50}; do
  curl -s -o /dev/null -w "%{http_code}\n" \
    -X POST $API_URL/enqueue/ingest \
    -H "Content-Type: application/json" \
    -d '{"tenant_id":"t1","s3_path":"upi_sample.csv","source":"UPI"}' |
    grep -q 429 && FAILS=$((FAILS+1))
done

if [ "$FAILS" -eq 0 ]; then
  echo "Rate limiting not triggered"
  exit 1
fi

echo "Rate limiting works"

echo "Shutting down..."
docker compose down -v

echo "TESTS PASSED"

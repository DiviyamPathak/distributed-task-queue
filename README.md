# distributed-task-queue
multi-tenant task queue with fair share allocation and priority


```mermaid
flowchart LR
  subgraph Producers
    P[API / Producer Services]
  end
  P -->|enqueue| Rabbit[RabbitMQ Broker]
  Rabbit -->|queue per tenant / shared queues| Workers[Celery Workers]
  Workers -->|results| RedisBackend[Redis / Postgres]
  Workers -->|store artifacts| S3[S3/Object Store]
  Rabbit -->|DLX| DLQ[Dead Letter Queue]
  RedisRate[Redis: token buckets & counters] ---|read/write| Workers
  Monitoring[Prometheus + Grafana + Flower] ---|metrics| Workers
  KEDA ---|scale| Workers
  Billing[Billing service] <---|usage data| RedisRate
```

### Completed
1. API server
2. Celery Tasks and app setup
3. Rate limiting and quaota

### Inprogress
1. Logic for CSV and all task processing logic

### Running locally
run first time
```
docker compose up --build
```

remove

```
docker compose down

```

delete 

```
sudo docker rmi -f xxx_id


sudo docker volume rm xxx_id


sudo docker network rm xxx_id

```

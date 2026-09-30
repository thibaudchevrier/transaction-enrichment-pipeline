# Streaming Consumer

Consumes transactions from Kafka, gets their category from the ML API, and writes transactions and
predictions to Postgres. Records it can't process go to a dead letter queue. Delivery is
at-least-once with no loss: offsets are committed only after the results are stored.

## Processing a window

The consumer works in windows of up to `MESSAGE_BATCH_SIZE` messages. A window closes when it is
full, after `BUFFER_TIMEOUT` seconds, or after 3 empty polls, whichever comes first.
`process_window` (in `main.py`) then runs these steps in order:

1. **Validate**: JSON decoding and the shared Pydantic model (`core.model.Transaction`). Ids are
   kept when they are UUIDs, otherwise mapped to a deterministic UUID5.
2. **Predict**: parallel calls to the ML API (`API_BATCH_SIZE` per request, `API_MAX_WORKERS`
   threads), with exponential backoff retries.
3. **Write**: transactions (`ON CONFLICT DO NOTHING`) and predictions (upsert) in **one** database
   transaction.
4. **Dead-letter**: publish invalid records (`error_type=invalid`) and transactions whose prediction
   still failed after retries (`error_type=prediction_failed`) to `KAFKA_DLQ_TOPIC`, then flush.
5. **Commit offsets** synchronously. Kafka auto-commit is disabled.

| Failure | Effect |
|---------|--------|
| Crash or error before step 5 | Offsets aren't committed; after the restart (`restart: unless-stopped`) the window is redelivered. The writes are idempotent, so no rows are duplicated. |
| Database error | The transaction rolls back and the consumer stops before committing, so the window is redelivered. |
| DLQ not delivered within 30 s | `RuntimeError` before the commit, so the window is redelivered. DLQ messages may be duplicated, which is fine because they are for inspection, not counting. |

`tests/test_main.py` checks this ordering with fakes: offsets are committed last, a database failure
commits nothing, and undelivered DLQ messages block the commit.

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers |
| `KAFKA_CONSUMER_GROUP` | `transaction-consumer-group` | Consumer group (scale by adding consumers, up to the partition count) |
| `KAFKA_TOPIC` | `transactions` | Source topic |
| `KAFKA_DLQ_TOPIC` | `failed-transactions` | Dead letter queue |
| `ML_API_URL` | `http://localhost:8000` | ML API |
| `DATABASE_URL` | required | Postgres URL |
| `MESSAGE_BATCH_SIZE` | `50` | Messages per window |
| `API_BATCH_SIZE` | `10` | Transactions per ML API request |
| `API_MAX_WORKERS` | `5` | Parallel ML API requests |
| `DB_ROW_BATCH_SIZE` | `50` | Rows per bulk write |
| `BUFFER_TIMEOUT` | `5.0` | Seconds before a partial window is processed |

In Docker Compose these come from `.env` (`STREAMING_*` variables).

## Running

```bash
make streaming                 # from the repository root: Kafka, producers and this consumer
make logs-consumer             # follow its logs
uv run pytest                  # tests, from this directory
```

Kafka UI (http://localhost:8081) shows the consumer group's lag and the DLQ messages with their
`error_type` and `failed_at` headers.

# GBFS collector

Collector đọc discovery document chính thức của Citi Bike, tìm URL `station_information` và `station_status`, sau đó biến mỗi station trong một snapshot thành một event canonical và gửi vào Kafka.

## Chạy

Infrastructure phải được bootstrap trước. Chạy liên tục theo TTL của source:

```powershell
docker compose --env-file config/environments/local.env --profile ingestion up -d gbfs-collector
```

Chạy đúng một poll để kiểm tra:

```powershell
docker compose --env-file config/environments/local.env --profile ingestion run --rm gbfs-collector python3 -m producers.gbfs_collector --once
```

Theo dõi log:

```powershell
docker compose --env-file config/environments/local.env --profile ingestion logs -f --tail 100 gbfs-collector
```

## Output

- Valid event: `citibike.station-status.v1`, Kafka key là `station_id` để mọi trạng thái của cùng trạm vào cùng partition; `event_id` dùng cho deduplication.
- Invalid normalized event: `citibike.station-status.invalid.v1`.
- Raw response: `data/raw/gbfs/<feed>/date=YYYY-MM-DD/`.
- Persistent duplicate state: `data/state/gbfs-collector.json`.
- Metrics snapshot: `artifacts/step6/metrics.json`.

## Delivery semantics

Kafka producer bật idempotence và `acks=all`. Collector chỉ ghi snapshot hash vào state sau khi Kafka acknowledge toàn bộ batch. Nếu process chết giữa lúc publish, snapshot có thể được gửi lại khi restart; deterministic `event_id` cho phép downstream deduplicate. Đây là at-least-once, không tuyên bố exactly-once từ HTTP tới Kafka.

Poll interval là giá trị lớn hơn giữa TTL source và `GBFS_MIN_POLL_SECONDS`. Request lỗi được retry hữu hạn với exponential backoff. Sau khi hết retry, continuous mode chờ bounded backoff rồi rediscover endpoint; `--once` trả exit code lỗi để automation phát hiện.

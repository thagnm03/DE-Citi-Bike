# Vận hành và kiểm thử local

## Infrastructure

```powershell
pwsh -NoProfile -File scripts/bootstrap.ps1
pwsh -NoProfile -File scripts/verify_infrastructure.ps1
docker compose --env-file config/environments/local.env ps
docker compose --env-file config/environments/local.env logs -f --tail 100
```

Mẫu cấu hình ở `config/environments/local.env` và `ci.env`. Container dùng `kafka:29092`, `postgres:5432`, `spark://spark-master:7077`; host dùng published ports.

## Luồng dữ liệu thật

Khởi động collector theo [hướng dẫn collector](../producers/gbfs_collector/README.md). Full historical batch tạo `data/gold/station_demand_baseline`; sample batch ghi dưới `artifacts/`, chưa cung cấp baseline mặc định cho live job.

Station-state job chạy trong terminal riêng:

```powershell
docker compose --env-file config/environments/local.env run --rm --no-deps toolbox /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --conf spark.jars.ivy=/tmp --master local[2] /workspace/spark/streaming/station_state_job.py --source kafka --sink kafka --bootstrap-servers kafka:29092 --checkpoint-root /workspace/data/state/spark-step8 --baseline-path /workspace/data/gold/station_demand_baseline
```

Đối với alert job/serving, dùng CLI `--help` và definitions trong `docker-compose.yml` để chọn topics, checkpoints và environment. Integration scripts dưới đây dựng từng phần bằng fixture, không tự thay thế orchestration cho live deployment.

## Integration scenarios

Từ repository root, chạy `pwsh -NoProfile -File scripts/<script>`:

| Script | Kiểm tra |
|---|---|
| `run_step4_docker.ps1` | Minimal vertical prototype |
| `run_step7_batch.ps1` | Historical pipeline; hỗ trợ `-Sample` |
| `run_step8_streaming.ps1` | Validation, dedup, late data, checkpoint |
| `run_step9_alerts.ps1` | Persistence, score, lifecycle, replay |
| `run_step10_serving.ps1` | Kafka → DB → API và ACK |
| `run_step11_dashboard.ps1` | Dashboard; hỗ trợ `-KeepRunning` |
| `run_step12_observability.ps1` | Metrics, Prometheus, SLO benchmark |
| `run_step13_recovery.ps1` | Failure, backlog, backup/restore |
| `run_step14_demo.ps1` | Deterministic end-to-end scenario |

Artifacts được tạo local và không nằm trong repo. End-to-end verifier có thể cần evidence từ các scenarios trước. Recovery script reset/drop/restore schema `serving`: chỉ chạy trên local/CI test database.

## API

Dashboard: <http://localhost:18000/dashboard>. OpenAPI: <http://localhost:18000/docs>. Prometheus khi khởi động: <http://localhost:19090>.

Endpoints: `/health/live`, `/health/ready`, `/api/v1/alerts`, alert detail/history, `/api/v1/stations`, `POST /api/v1/alerts/{alert_id}/acknowledge`. ACK có `idempotency_key`, `requested_by`, `acknowledged_at_utc` với timezone. Retry cùng command giữ cùng effect; ACK resolved alert trả conflict.

## Dừng services

```powershell
docker compose --env-file config/environments/local.env stop
docker compose --env-file config/environments/local.env down
```

`down` giữ named volumes; `down --volumes` xóa persisted data. Giữ checkpoint cho cùng topology. Cấu hình local với password mẫu/API chưa có authentication không dùng cho public deployment.

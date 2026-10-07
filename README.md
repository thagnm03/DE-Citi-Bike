# Citi Bike Station Operations Platform

Dự án Data Engineering / Big Data cho môn IE212. Hệ thống kết hợp trạng thái trạm Citi Bike gần thời gian thực và lịch sử chuyến đi để phát hiện trạm hết xe, hết chỗ trả xe, offline hoặc có dữ liệu cũ; từ đó tạo hàng đợi cảnh báo có ưu tiên và lý do giải thích được.

## Chức năng

- Thu thập GBFS, lưu raw snapshot và chuẩn hóa observation theo JSON Schema.
- Spark batch tạo Silver/Gold Parquet và demand baseline theo trạm, thứ và giờ.
- Structured Streaming xử lý deduplication, event-time windows, watermark và checkpoint.
- Theo dõi risk episode, tính priority và đề xuất `DELIVER_BIKES`, `REMOVE_BIKES`, `INSPECT_STATION`.
- PostgreSQL lưu current state và lịch sử cảnh báo; FastAPI/dashboard hỗ trợ operator ACK.
- Prometheus theo dõi metrics; fixtures kiểm tra replay, lỗi và recovery.

## Kiến trúc

```text
GBFS → Collector → Kafka → Spark Structured Streaming → Kafka
                              ↑                          ↓
Historical CSV → Spark Batch → Parquet baseline     PostgreSQL
                                                        ↓
                                                  FastAPI → Dashboard

                      Metrics → Prometheus
```

Stack: Python, Apache Kafka, Apache Spark, Parquet, PostgreSQL, FastAPI, HTML/CSS/JavaScript, Prometheus và Docker Compose.

Chi tiết: [Kiến trúc](docs/architecture.md), [Dữ liệu và pipelines](docs/data-pipelines.md), [Vận hành và kiểm thử](docs/operations.md).

## Yêu cầu

- Docker Desktop và Docker Compose.
- PowerShell 7 (`pwsh`) trên Windows cho các script đi kèm.
- Kết nối mạng để lấy images, Spark connector và dữ liệu nguồn.
- Khuyến nghị tối thiểu khoảng 4 GB RAM cho Docker; full batch có thể cần thêm tài nguyên.
- Các cổng mặc định: `15432`, `7077`, `8080`, `8081`, `9092`, `18000`, `19090`.

## Khởi động nhanh

```powershell
git clone https://github.com/thagnm03/DE-Citi-Bike.git
cd DE-Citi-Bike
pwsh -NoProfile -File scripts/bootstrap.ps1
```

Bootstrap build runtime image, khởi động Kafka/PostgreSQL/Spark, tạo topics và chạy smoke checks. Dùng `-SkipBuild` khi image đã được build.

Chạy collector với nguồn GBFS thật:

```powershell
docker compose --env-file config/environments/local.env --profile ingestion up -d gbfs-collector
```

Chạy historical batch với sample 500 trips có sẵn:

```powershell
pwsh -NoProfile -File scripts/run_step7_batch.ps1 -Sample
```

Full batch cần archive theo [hướng dẫn dữ liệu](docs/data-pipelines.md), sau đó chạy script không có `-Sample`.

Mở dashboard với dữ liệu fixture được dựng bởi integration scenario:

```powershell
pwsh -NoProfile -File scripts/run_step11_dashboard.ps1 -KeepRunning
```

Truy cập [Dashboard](http://localhost:18000/dashboard) và [API docs](http://localhost:18000/docs). Khởi động collector riêng chưa tự khởi động toàn bộ downstream jobs; xem hướng dẫn vận hành cho luồng dữ liệu thật.

## Kiểm thử

Chạy pytest trong runtime container sau khi bootstrap (pytest được cài vào container dùng một lần):

```powershell
docker compose --env-file config/environments/local.env run --rm --no-deps --user root toolbox sh -c 'python3 -m pip install pytest && python3 -m pytest -q -p no:cacheprovider'
```

Integration checks nằm trong `scripts/run_step*.ps1`; xem [hướng dẫn kiểm thử](docs/operations.md). Tên `step` được giữ để tương thích scripts và output paths.

## Cấu trúc thư mục

```text
config/          Environments, business rules, SLO/recovery targets
contracts/       JSON Schemas và examples
data/samples/    Dữ liệu nhỏ và schema fixtures
docs/            Tài liệu kỹ thuật và vận hành
producers/       GBFS collector
spark/           Batch, streaming, shared rules/schemas
serving/         PostgreSQL, materializer, API và dashboard
observability/   Metrics, SLO evaluator, benchmark
monitoring/      Prometheus configuration và alert rules
reliability/     Recovery verification helpers
prototype/       Minimal integration prototype
demo/            Deterministic end-to-end scenario
scripts/         Bootstrap, validation, integration runs
tests/           Correctness tests và fixtures
requirements/    Runtime dependencies
```

Logs, virtual environments, raw/generated data, checkpoints, run artifacts và monthly ZIP không được commit. `config/environments/*.env` là mẫu local/CI với password chỉ dành cho phát triển.

## Giới hạn

Môi trường học tập trên một máy: một Kafka broker, một Spark worker, API local chưa có authentication/TLS. Không expose trực tiếp ra mạng công cộng. Baseline là ngữ cảnh lịch sử, không phải forecast; priority queue không tối ưu tuyến điều phối. Public GBFS là snapshot, không thể hiện mọi inventory change hoặc unmet demand. Local checks không chứng minh production capacity hay high availability.

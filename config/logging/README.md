# Logging convention

## Mục tiêu

Log phải giúp trả lời bốn câu hỏi: component nào gặp vấn đề, run nào bị ảnh hưởng, dữ liệu nào liên quan và lỗi xảy ra lúc nào.

Application log mới từ Bước 6 trở đi phải ghi một JSON object trên mỗi dòng với các field chung:

| Field | Bắt buộc | Ý nghĩa |
|---|---|---|
| `timestamp_utc` | Có | ISO-8601 UTC, ví dụ `2026-09-23T08:00:00Z` |
| `level` | Có | `DEBUG`, `INFO`, `WARN` hoặc `ERROR` |
| `service` | Có | Tên component ổn định, ví dụ `gbfs-collector` |
| `event` | Có | Tên machine-readable, ví dụ `snapshot_published` |
| `message` | Có | Mô tả ngắn cho người đọc |
| `run_id` | Khi có run | Liên kết các log của cùng lần chạy |
| `station_id` | Khi liên quan trạm | Khóa business, không dùng tên trạm làm khóa |
| `event_id` | Khi liên quan record | Truy vết record xuyên pipeline |
| `error_type` | Khi lỗi | Loại exception/error ổn định |
| `duration_ms` | Khi đo thao tác | Thời gian xử lý, không trộn với event time |

Không ghi password, token, connection string chứa secret hoặc toàn bộ payload người dùng vào log.

Ví dụ:

```json
{"timestamp_utc":"2026-09-23T08:00:00Z","level":"INFO","service":"gbfs-collector","event":"snapshot_published","message":"Published normalized station snapshot","run_id":"collector-20260923T080000Z","station_count":2520,"duration_ms":841}
```

## Log của infrastructure

Kafka, Spark và PostgreSQL hiện dùng log gốc của image. Docker lưu log bằng driver `json-file`, xoay file ở 10 MB và giữ tối đa 3 file cho mỗi container. Đây là giới hạn local-development, không phải centralized logging production.

Đọc log:

```powershell
docker compose --env-file config/environments/local.env logs -f --tail 100
docker compose --env-file config/environments/local.env logs -f kafka
```

## Quy tắc level

- `INFO`: lifecycle bình thường, batch/snapshot hoàn tất, không log từng record.
- `WARN`: dữ liệu xấu có thể cô lập, retry tạm thời, source stale.
- `ERROR`: thao tác thất bại cần can thiệp hoặc run không thể hoàn tất.
- `DEBUG`: chỉ bật cục bộ; không dùng làm bằng chứng vận hành mặc định.


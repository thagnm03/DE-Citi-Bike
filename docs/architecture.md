# Kiến trúc hệ thống

## Bài toán

Đội vận hành cần xác định trạm nào cần thêm xe, lấy bớt xe hoặc kiểm tra. Hệ thống duy trì trạng thái theo thời gian để phân biệt biến động thoáng qua với rủi ro kéo dài. Output là priority queue có action, score components, reason codes và observation lineage.

## Thành phần

1. Collector lưu raw GBFS, enrich metadata và publish normalized observation vào Kafka.
2. Spark batch chuẩn hóa historical trips và tạo demand baseline trong Parquet.
3. Station-state streaming validate, dedup, cập nhật latest state, tính windows và join baseline.
4. Alert job theo dõi risk episodes và phát lifecycle events.
5. Materializer ghi events và projections vào PostgreSQL.
6. FastAPI phục vụ dashboard và ACK commands; Prometheus theo dõi metrics.

Kafka tách producer/consumer, buffer dữ liệu và cho phép replay. Parquet phục vụ scan/aggregation lịch sử; PostgreSQL phục vụ indexed queries và command transactions.

## Correctness

Event time là `time.snapshot_updated_at_utc`; ingestion time là lúc collector nhận response. Station `last_reported` được giữ riêng cho freshness; sentinel timestamp không được coi là hợp lệ.

Kafka key là `station_id`. Partition ordering không bảo đảm event time tăng: current state chỉ nhận observation mới hơn, còn out-of-order hợp lệ có thể đóng góp window. Watermark mặc định 10 phút, window 5 phút; checkpoints riêng bảo toàn state từng query.

Deterministic identities cho phép dedup observation và lifecycle events. Materializer commit DB trước Kafka offset; đọc lại sau crash phải có idempotent DB effect. Đây là at-least-once delivery với idempotency, không tuyên bố distributed exactly-once.

## Business state

Availability: `EMPTY`, `LOW_BIKES`, `BALANCED`, `LOW_DOCKS`, `FULL`. Health: `STALE`, `OFFLINE`. `config/business-rules-v1.json` cấu hình persistence, freshness và priority weights; đây là policy dự án.

Spark sở hữu risk lifecycle; operator ACK là overlay riêng. Update evidence/score không đổi alert đã ACK về OPEN. Resolve kết thúc episode. Event history giữ append-only để audit.

## Giới hạn

Baseline join dùng `station.short_name` làm bridge tới historical station ID cùng weekday/hour ở `America/New_York`, chưa có effective-dated mapping hoàn chỉnh. Đổi state schema/query topology cần migration hoặc replay và checkpoint phù hợp. Một broker/worker chưa chứng minh khả năng chịu lỗi hạ tầng.

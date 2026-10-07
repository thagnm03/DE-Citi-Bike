# Dữ liệu và pipelines

## Nguồn

- GBFS discovery: <https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json>.
- Historical trips: <https://citibikenyc.com/system-data>.
- Sample manifest: [data/samples/README.md](../data/samples/README.md).
- JSON Schema nội bộ: `contracts/`; source schema fixtures: `data/samples/gbfs/schema-v1.1/`.

Raw GBFS giữ nguyên response bytes, hash và archive path. Observation gồm station identity, availability, service flags, timestamps và quality warnings. Structural-invalid records vào dead letter với reason codes và lineage.

## Historical archive

Full batch hiện chọn tháng 04/2026. Từ repository root:

```powershell
curl.exe -fL --retry 3 -o data/samples/trips/202604-citibike-tripdata.zip https://s3.amazonaws.com/tripdata/202604-citibike-tripdata.zip
Get-FileHash data/samples/trips/202604-citibike-tripdata.zip -Algorithm SHA256
```

Checksum archive dùng xây sample hiện có: `FEFC1FCA369818BAEB9206792E02A098B71B59F263064B6627B42317965E2FFB`. Nếu nguồn thay đổi, xác minh nội dung trước khi dùng expected outputs. Monthly ZIP không nằm trong Git.

## Batch

Raw manifest lưu checksum, schema variant, row count và source path. Pipeline resolve schema từng CSV, chuẩn hóa timestamps, dedup và tạo:

| Output | Grain |
|---|---|
| Silver trips | Một row/ride ID |
| Quality quarantine | Trip có quality issues |
| Station hourly demand | Station/date/hour |
| Demand baseline | Station/weekday/hour |

Timestamp không có offset được hiểu theo `America/New_York`; elapsed duration dùng UTC. Endpoint hợp lệ vẫn có thể đóng góp demand dù endpoint kia thiếu. Warnings giữ lineage để audit.

## Streaming topics

| Topic | Vai trò |
|---|---|
| `citibike.station-status.v1` | Normalized observations |
| `citibike.station-status.invalid.v1` | Invalid observations |
| `citibike.station-current.v1` | Latest state và baseline context |
| `citibike.station-windows.v1` | Closed window aggregates |
| `citibike.station-alerts.invalid.v1` | Invalid decision input |
| `citibike.station-alerts.v1` | Alert lifecycle events |
| `citibike.station-alerts.replay.v1` | Isolated replay outputs |

Current-state topic dùng compaction; windows và events dùng retention theo environment. Contract examples ở `contracts/examples/`.

## Output paths

Raw: `data/raw/gbfs/`. Historical: `data/silver/`, `data/gold/`. Collector state/checkpoints: `data/state/`. Integration outputs/metrics: `artifacts/`. Generated paths được loại khỏi Git.

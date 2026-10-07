# Business Requirements — Bike-Sharing Station Availability & Rebalancing Platform

## 1. Mục đích

Tài liệu này chuyển project charter thành các yêu cầu nghiệp vụ có thể truy vết và kiểm thử. Nó mô tả hệ thống phải hỗ trợ ai, quyết định gì và kết quả nào; chưa khóa implementation technology hoặc data schema chi tiết.

## 2. Problem statement

Sự phân bố xe không đồng đều có thể khiến station không còn xe để thuê hoặc không còn dock để trả. Operations team cần một nguồn thông tin đáng tin cậy để phân biệt biến động ngắn hạn với tình trạng kéo dài, đánh giá mức ảnh hưởng và ưu tiên rebalancing hoặc inspection.

Hệ thống phải chuyển dữ liệu trạng thái và lịch sử thành operational actions. Việc chỉ hiển thị số xe/dock hiện tại không đủ để đáp ứng yêu cầu này.

## 3. AS-IS và TO-BE process

### AS-IS được giả định từ public data

```text
Current station snapshot
        ↓
Operator manually observes availability
        ↓
Operator independently determines whether action is needed
```

Hạn chế:

- Không có persistence context.
- Không có data-freshness classification rõ ràng.
- Không kết hợp historical demand.
- Không có priority queue thống nhất.
- Không có replay/audit trail trong phạm vi project.

### TO-BE trong project

```text
Station status + historical demand
        ↓
Validated time-aware station state
        ↓
Persistent risk episode
        ↓
Explainable priority score
        ↓
Recommended operational action
        ↓
Acknowledge / resolve / audit
```

TO-BE là mô hình hệ thống đề xuất cho đồ án, không phải mô tả internal architecture hiện tại của Citi Bike.

## 4. Actors

| Actor | Vai trò |
|---|---|
| Rebalancing dispatcher | Theo dõi queue và quyết định thứ tự can thiệp |
| Field operator | Thực hiện hoặc mô phỏng hành động giao/lấy xe, kiểm tra station |
| Service analyst | Phân tích lịch sử mất cân bằng và nhu cầu |
| Data/platform operator | Theo dõi freshness, data quality và pipeline health |
| System clock/source collector | Cung cấp observations định kỳ để duy trì station state |

## 5. Business goals

| ID | Business goal | Kết quả mong muốn |
|---|---|---|
| BG-01 | Giảm thời gian station ở trạng thái không phục vụ được | Phát hiện và ưu tiên empty/full episodes |
| BG-02 | Tập trung nguồn lực vào station có tác động cao | Priority có historical demand context |
| BG-03 | Tránh hành động từ dữ liệu cũ hoặc lỗi | Stale/offline/data-quality states tách biệt |
| BG-04 | Tăng độ tin cậy của operational alerts | Idempotent alert lifecycle và audit trail |
| BG-05 | Cho phép planning dựa trên lịch sử | Demand/imbalance aggregates theo station và thời gian |
| BG-06 | Chứng minh hệ thống có thể phục hồi | Replay và recovery không làm sai business state |

## 6. Business requirements

### BR-01 — Current station visibility

Hệ thống phải cung cấp trạng thái gần nhất của từng station, gồm tối thiểu số xe khả dụng, số dock khả dụng, trạng thái phục vụ, source event time và data freshness.

Liên kết: BG-01, BG-03.

### BR-02 — Service-risk classification

Hệ thống phải phân biệt tối thiểu các trạng thái:

- `EMPTY`
- `LOW_BIKES`
- `BALANCED`
- `LOW_DOCKS`
- `FULL`
- `STALE`
- `OFFLINE`

Threshold phải cấu hình được và không được trình bày như rule chính thức của Citi Bike.

Liên kết: BG-01, BG-03.

### BR-03 — Persistent episode detection

Hệ thống phải theo dõi thời gian station duy trì một trạng thái rủi ro và chỉ tạo operational alert khi condition tồn tại đủ lâu theo policy cấu hình.

Liên kết: BG-01, BG-04.

### BR-04 — Recommended action

Mỗi actionable alert phải đề xuất một trong các hành động:

- `DELIVER_BIKES`
- `REMOVE_BIKES`
- `INSPECT_STATION`

Liên kết: BG-01.

### BR-05 — Explainable prioritization

Hệ thống phải xếp hạng alert bằng priority score và cung cấp các reason codes có thể đọc được, ví dụ:

- `EMPTY_FOR_12_MINUTES`
- `HIGH_HISTORICAL_PICKUP_DEMAND`
- `RAPID_RECENT_OUTFLOW`
- `SOURCE_DATA_STALE`

Liên kết: BG-02, BG-04.

### BR-06 — Historical demand context

Hệ thống phải có khả năng xây baseline nhu cầu tương đối theo station, ngày trong tuần và khung giờ từ historical trip data.

Baseline hỗ trợ prioritization và planning; không được gọi là forecast nếu chưa có predictive model và evaluation tương ứng.

Liên kết: BG-02, BG-05.

### BR-07 — Alert lifecycle

Alert phải có lifecycle tối thiểu:

```text
OPEN → ACKNOWLEDGED → RESOLVED
```

Hệ thống phải tránh tạo nhiều open alerts cho cùng một station, condition và episode chỉ vì duplicate input hoặc processor restart.

Liên kết: BG-04, BG-06.

### BR-08 — Freshness and station-health handling

Hệ thống phải phân biệt:

- Station thực sự empty/full.
- Station offline hoặc không cho thuê/trả.
- Source snapshot quá cũ để ra quyết định.
- Pipeline không nhận được dữ liệu mới.

Liên kết: BG-03.

### BR-09 — Data-quality visibility

Hệ thống phải đo và công khai tối thiểu:

- Invalid records.
- Duplicate observations.
- Late/out-of-order observations.
- Unmatched station IDs.
- Stale source records.
- Inconsistent capacity/availability values.

Record lỗi không được biến mất mà không có count hoặc audit output.

Liên kết: BG-03, BG-04.

### BR-10 — Historical and operational audit

Reviewer/operator phải có khả năng truy từ một alert về các station observations và business reasons đã tạo ra alert đó.

Liên kết: BG-04, BG-06.

### BR-11 — Replayability

Hệ thống phải có khả năng tái xử lý một khoảng dữ liệu đã lưu để:

- Khôi phục state.
- Kiểm thử business rule mới.
- Reproduce một demo hoặc incident.

Replay cùng input và cùng rule version phải tạo business output tương đương theo idempotency policy.

Liên kết: BG-04, BG-06.

### BR-12 — Historical service analysis

Hệ thống phải cung cấp dữ liệu để trả lời:

- Station nào thường xuyên empty/full?
- Episode thường xảy ra vào giờ/ngày nào?
- Episode kéo dài bao lâu?
- Station nào có demand cao nhưng service availability thấp?

Liên kết: BG-05.

## 7. Decision requirements

| Decision ID | Câu hỏi | Thông tin cần có | Output |
|---|---|---|---|
| D-01 | Station nào cần thêm xe trước? | Empty/low duration, pickup baseline, recent trend | Ranked `DELIVER_BIKES` tasks |
| D-02 | Station nào cần lấy bớt xe trước? | Full/low-dock duration, drop-off baseline, recent trend | Ranked `REMOVE_BIKES` tasks |
| D-03 | Station nào cần kiểm tra? | Freshness, service flags, metadata consistency | `INSPECT_STATION` tasks |
| D-04 | Có thể tin output hiện tại không? | Source age, pipeline health, invalid/late rates | Data health status |
| D-05 | Khu vực/thời gian nào cần planning? | Historical alerts, demand and duration aggregates | Batch analytical output |

## 8. Time and latency requirements

Các giá trị dưới đây là target của project, không phải cam kết chính thức của Citi Bike.

| ID | Requirement | Target ban đầu |
|---|---|---|
| TR-01 | Current state freshness phải được hiển thị | Mỗi output có source age |
| TR-02 | Sau khi persistence condition được thỏa, alert phải xuất hiện sớm | Trong vòng 2 phút ở steady-state prototype |
| TR-03 | Stale source phải được đánh dấu | Theo configurable freshness threshold |
| TR-04 | Historical baseline | Recompute theo batch, không yêu cầu real-time |
| TR-05 | Recovery | Sau restart, processor tiếp tục từ điểm đã lưu và bắt kịp backlog |

Persistence duration, watermark và freshness threshold được cấu hình theo data profiling; target phải có scenario test tương ứng.

## 9. Business rules cấp cao

### 9.1 Availability ratios

```text
bike_availability_ratio = available_bikes / capacity
dock_availability_ratio = available_docks / capacity
```

Rule phải xử lý riêng trường hợp capacity thiếu, bằng 0 hoặc không nhất quán.

### 9.2 Risk episode

Một observation rủi ro chưa tự động tạo alert. Alert được mở khi condition liên tục vượt persistence threshold. Episode kết thúc khi station trở lại vùng an toàn đủ lâu theo recovery threshold.

### 9.3 Priority

Priority score ban đầu phải rule-based và giải thích được:

```text
priority = severity
         + persistence
         + historical_demand
         + recent_inventory_trend
         + service_impact
```

Trọng số cụ thể chưa được chốt trong thiết kế ban đầu. Mỗi thay đổi sau này phải có rule version.

## 10. Data requirements cấp cao

### Current station data cần có

- Stable station identifier.
- Source timestamp hoặc last-reported timestamp.
- Available bikes.
- Available docks.
- Service flags.
- Station metadata/capacity hoặc cách liên kết tới metadata.

### Historical trip data cần có

- Start/end timestamps.
- Start/end station identifiers hoặc tên/tọa độ có thể chuẩn hóa.
- Ride identifier hoặc deduplication strategy.
- Phạm vi thời gian đủ để tạo hourly/weekday baseline.

### Metadata cần có

- Station identifier.
- Name.
- Coordinates.
- Capacity nếu source cung cấp.
- Effective time hoặc strategy xử lý metadata changes.

Tính đầy đủ và chất lượng thực tế của các trường trên sẽ được kiểm tra trong data profiling.

## 11. Non-functional business requirements

### NFR-01 — Correctness

Các business scenario quan trọng phải có golden input và expected output tính được bằng tay.

### NFR-02 — Idempotency

Duplicate input, replay và processor retry không được tạo duplicated open alerts hoặc duplicated operational tasks ngoài policy.

### NFR-03 — Recoverability

Hệ thống phải chứng minh phục hồi sau processor failure và có thể bắt kịp buffered input.

### NFR-04 — Explainability

Priority và recommended action phải đi kèm reason codes; không sử dụng black-box score trong MVP.

### NFR-05 — Traceability

Business requirement, rule version, input observation và output alert phải có đường truy vết.

### NFR-06 — Observability

Phải phân biệt được source failure, data-quality failure, processing failure và serving failure.

### NFR-07 — Scalability evidence

Mọi tuyên bố scale phải đi kèm dataset size, input rate, processing rate, latency, partition configuration và môi trường test.

### NFR-08 — Reproducibility

Một người khác phải có thể dựng môi trường, lấy sample hợp lệ và chạy demo từ tài liệu repository.

### NFR-09 — Privacy minimization

Project không cần và không nên thu thập thông tin định danh cá nhân. Historical fields không cần cho operational use case phải được loại khỏi curated layers.

## 12. Prioritization theo MoSCoW

### Must have

- Current station state.
- Empty/full/stale classification.
- Persistent episode detection.
- Explainable recommended action.
- Alert lifecycle và idempotency.
- Historical baseline tối thiểu.
- Data-quality visibility.
- Replay/recovery demonstration.

### Should have

- Low-bike/low-dock states.
- Recent inventory trend.
- Acknowledge/resolve simulation.
- Historical alert analysis.
- Partition/skew experiment.

### Could have

- Multi-city GBFS support.
- Weather enrichment.
- Simple demand forecast.
- Rebalancing route optimization.

### Won't have trong MVP

- Production dispatch integration.
- Automated physical actions.
- User-level tracking.
- Dynamic pricing.
- Kubernetes/multi-region architecture.

## 13. Acceptance scenarios cấp business

### Scenario A — Empty station cần bổ sung xe

```text
Given: station có demand pickup cao trong khung giờ hiện tại
And: số xe giảm về 0
When: trạng thái empty kéo dài quá persistence threshold
Then: đúng một OPEN alert được tạo
And: recommended action là DELIVER_BIKES
And: reason bao gồm duration và historical demand context
```

### Scenario B — Full station cần lấy bớt xe

```text
Given: station không còn dock trống
When: trạng thái full kéo dài quá persistence threshold
Then: alert REMOVE_BIKES được tạo và xếp hạng
```

### Scenario C — Snapshot bị lặp

```text
Given: cùng station observation được gửi nhiều lần
When: processor nhận duplicate
Then: current state và alert episode không bị nhân đôi
And: duplicate count tăng
```

### Scenario D — Dữ liệu cũ

```text
Given: station không có source update mới trong freshness threshold
Then: station được đánh dấu STALE
And: hệ thống không trình bày availability cũ như trạng thái hiện tại đáng tin cậy
```

### Scenario E — Recovery

```text
Given: processor dừng trong khi input tiếp tục được buffer
When: processor khởi động lại
Then: backlog được xử lý
And: không tạo duplicated open alerts
And: final business state phù hợp với input history
```

### Scenario F — Historical planning

```text
Given: historical trips và alert history đã được xử lý
When: analyst truy vấn station theo weekday/hour
Then: có thể xác định station thường xuyên mất cân bằng và mức demand tương đối
```

## 14. Limitations phải công khai

- Station status là snapshot, không phải complete stream của mọi inventory change.
- Public feed không đại diện toàn bộ internal operational data.
- Historical trip demand không chứa đầy đủ unmet demand: người dùng không thể thuê xe khi station đã empty sẽ không tạo trip.
- Không có truck location, crew capacity hoặc dispatch outcome.
- Priority queue không phải tối ưu toàn cục.
- Project đánh giá data-system correctness và decision support, không đánh giá tác động kinh doanh thật ngoài đời.

## 15. Requirement traceability summary

| Goal | Requirements chính |
|---|---|
| BG-01 | BR-01, BR-02, BR-03, BR-04 |
| BG-02 | BR-05, BR-06 |
| BG-03 | BR-01, BR-02, BR-08, BR-09 |
| BG-04 | BR-05, BR-07, BR-09, BR-10, BR-11 |
| BG-05 | BR-06, BR-12 |
| BG-06 | BR-07, BR-10, BR-11 |


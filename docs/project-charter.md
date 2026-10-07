# Project Charter — Citi Bike Station Operations Platform

## Bài toán

Người dùng thuê và trả xe làm số xe và dock trống thay đổi giữa các trạm. Một trạm hết xe không thể phục vụ lượt thuê; một trạm hết dock không thể phục vụ lượt trả. Trạm offline hoặc dữ liệu cũ cũng làm thông tin vận hành thiếu tin cậy.

Nếu chỉ nhìn snapshot, operator khó biết tình trạng vừa xuất hiện hay kéo dài, trạm nào cần xử lý trước và cảnh báo nào đã được nhận hoặc giải quyết. Dự án xây nền tảng dữ liệu hỗ trợ các quyết định đó bằng public Citi Bike data.

## Người dùng và quyết định

| Người dùng | Quyết định |
|---|---|
| Rebalancing dispatcher | Trạm nào cần can thiệp trước? |
| Field operations | Giao thêm xe, lấy bớt xe hay kiểm tra trạm? |
| Service analyst | Trạm nào thường mất cân bằng vào giờ/ngày nào? |
| Platform operator | Có thể tin dữ liệu và trạng thái hệ thống hiện tại không? |

Mô hình này phục vụ đồ án; không khẳng định là quy trình nội bộ thực tế của Citi Bike.

## Input và output

Input gồm GBFS station information/status và historical trip files. Station status cho biết inventory và service flags tại một thời điểm; lịch sử trips cung cấp ngữ cảnh nhu cầu theo trạm, thứ và giờ.

Output cốt lõi là hàng đợi cảnh báo có ưu tiên, đề xuất `DELIVER_BIKES`, `REMOVE_BIKES` hoặc `INSPECT_STATION`, kèm duration, score components, reason codes và evidence truy về observation. Dashboard là giao diện sử dụng hàng đợi này.

## Quy trình đề xuất

```text
Station observations + historical demand
    → validation và freshness checks
    → station state và persistent risk episode
    → explainable priority và recommended action
    → operator ACK
    → resolve khi condition được phục hồi theo policy
    → audit history
```

ACK nghĩa là operator đã nhận cảnh báo; không chứng minh trạm đã phục hồi. Một observation empty chưa đủ mở alert nếu chưa đạt persistence policy.

## Mục tiêu và tiêu chí thành công

- Current state có nguồn thời gian và freshness rõ ràng.
- Phân biệt empty/full với offline/stale, không đề xuất hành động từ availability cũ như thể đó là dữ liệu đáng tin cậy.
- Cảnh báo dựa trên condition kéo dài, có action và lý do giải thích được.
- Duplicate, retry và replay không tạo thêm open alerts cho cùng episode.
- Historical baseline kết hợp được với current state; missing match được hiển thị rõ.
- Có audit lineage, checkpoint recovery, API/dashboard và metrics.
- Người khác có thể dựng môi trường và chạy controlled demo từ repository.

## Phạm vi

Bao gồm ingestion, raw archive, data quality, batch baseline, streaming state, rule-based decision engine, alert lifecycle, PostgreSQL/API/dashboard, monitoring và recovery tests.

Ngoài phạm vi: dispatch thật, hành động vật lý tự động, ML forecasting, tối ưu tuyến xe tải, dynamic pricing, user-level tracking, Kubernetes và multi-region deployment.

## Giả định và giới hạn

GBFS là snapshot, không ghi mọi inventory transition. Trip history không chứa unmet demand của khách không thuê được xe. Không có truck location, crew capacity hoặc dispatch outcomes. Baseline lịch sử không phải forecast; queue không phải global optimization. Threshold là policy của dự án, không phải quy tắc chính thức Citi Bike.

Triển khai local một broker/worker không chứng minh production scale hoặc high availability. Mục tiêu đánh giá là data correctness, explainability và khả năng phục hồi trong các scenarios được đo.

## Tài liệu liên quan

[Yêu cầu nghiệp vụ](business-requirements.md), [Kiến trúc](architecture.md), [Dữ liệu](data-pipelines.md), [Vận hành](operations.md).

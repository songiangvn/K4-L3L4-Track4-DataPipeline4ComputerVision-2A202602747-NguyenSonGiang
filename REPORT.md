# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Sơn Giang |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602747` |
| GitHub | songiangvn |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt giữa A và B mà mình thấy trong bronze (2 dòng đầu mỗi bảng, A có 3021 dòng, B có 3234 dòng):
1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`. B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A tính bằng m/s (`1.213`), B tính bằng km/h (`55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili-giây (`1759933801509`).
4. **Bàn đạp**: A là tỷ lệ 0..1 (`gas=0.028`), B là phần trăm (`throttle_pct=2.0`).
5. **Đồng hồ**: `bronze.video_files` ghi `telemetry_clock_offset_ms` = 0 cho drive_a và **1500** cho drive_b. Mẫu đầu tiên của B là `1759933801509` = 14:30:01.509 UTC, trong khi video B bắt đầu lúc 14:30:00, tức đồng hồ thiết bị B chạy nhanh khoảng 1,5 s.

Mỗi nguồn có bảng bronze riêng vì hai schema khác nhau. Nếu gộp ngay thì phải chọn tên cột và đơn vị chung ở bước ingest, mà bước đó không biết hết luật chuyển đổi. Mọi cột được giữ ở dạng chữ để bronze là **bản sao trung thực** của file nguồn: ô trống, giá trị vô lý hay sai định dạng đều được giữ nguyên chứ không vỡ lúc nạp. Khi luật ép kiểu ở silver sai, ta chỉ cần sửa dbt rồi chạy lại từ bronze, không phải ingest lại. Bronze cũng chỉ thêm và idempotent: nạp mỗi drive hai lần thì lần hai báo `already present` (nhận diện bằng sha256), và `history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, `rows_added=3021`).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Các model `stg_telemetry_a.sql` và `stg_telemetry_b.sql` đưa hai nguồn về cùng một schema (`ts, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`). Với nguồn B:
- **Đơn vị**: `spd_kmh / 3.6` thành m/s; `throttle_pct/100`, `brake_pct/100` thành tỷ lệ 0..1.
- **Thời gian**: `to_timestamp((timestamp_ms - offset_ms)/1000)`, đổi epoch ms sang `timestamptz` UTC. A thì chỉ cần `cast(ts_utc as timestamptz)`.
- **Độ lệch đồng hồ**: `offset_ms` lấy từ `bronze.video_files` (1500 ms cho drive_b, khai báo trong `sources.json`), join theo `drive_id` rồi trừ đi.

Bằng chứng cho thấy chuẩn hoá đúng: sau bước 2, `bat_dau` của drive_b là `2025-10-08 14:30:00.009`, khớp giờ bắt đầu video `14:30:00` (bản thô là 14:30:01.509). Tốc độ tối đa sau chuẩn hoá hợp lý: 71 km/h (A) và 100 km/h (B). Kiểm tra `drive B clock offset removed` PASS.

Bảng quarantine của mình:

| drive | duplicate | speed_missing | speed_out_of_range | tổng cách ly | dòng sạch | nhận về |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

Tổng cộng `silver.telemetry` có 6149 dòng và `telemetry_rejects` có 106 dòng; sạch + cách ly = nhận về ở cả hai drive. Dòng lỗi vào `rejects` kèm `reject_reason` thay vì bị xoá vì: (1) có thể **đối soát** được, nhìn vào là biết dòng nào mất và vì sao, không có dữ liệu "biến mất im lặng"; (2) nếu luật làm sạch sai (ví dụ ngưỡng 70 m/s quá chặt) thì có thể khôi phục dòng mà không ingest lại; (3) tỷ lệ cách ly theo lý do là tín hiệu giám sát chất lượng nguồn (ví dụ thiết bị B bắt đầu gửi nhiều ô trống).

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống giữa hai mẫu liên tiếp > 1000 ms) đo **tính liên tục** của cảm biến. Mỗi drive có đúng 1 khoảng hở (3073 ms ở A, 3124 ms ở B, trong khi trung vị là 100 ms): cảm biến mất tín hiệu khoảng 3 giây. Dữ liệu còn lại vẫn đúng, chỉ là thiếu một đoạn, nên test này là `severity: warn`. Pipeline báo để người vận hành biết nhưng không dừng, vì dừng thì mất cả dataset chỉ vì 3 giây trống. Ngược lại, `in_range` (0..70 m/s) kiểm tra **tính đúng** của từng giá trị. Một tốc độ vô lý lọt vào silver nghĩa là luật làm sạch đã hỏng và dữ liệu sai sẽ chảy xuống gold và model, nên nó là `error` và chặn mọi task phía sau.

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000, các giá trị vô lý không còn bị cách ly mà lọt vào silver. `dbt test` cho `PASS=5 WARN=1 ERROR=1`, test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** (đúng bằng 3 + 3 dòng `speed_out_of_range` ở bảng quarantine), `gate_failed=true`. Khôi phục luật thì trở lại `PASS=6 WARN=1 ERROR=0`. Bài học: luật làm sạch (transform) và cổng chất lượng (test) là **hai lớp độc lập**. Khi một người sửa nhầm luật làm sạch, test vẫn giữ định nghĩa "đúng" và chặn dữ liệu xấu trước khi nó đến gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)

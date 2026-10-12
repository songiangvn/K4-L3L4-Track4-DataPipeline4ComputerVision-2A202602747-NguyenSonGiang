# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Sơn Giang |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602747` |
| GitHub | songiangvn |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-06570` |

> Ghi chú về trip: MSSV `2A202602747` có chữ nên `int(MSSV)` trong GUIDE báo lỗi. Mình lấy 4 chữ số cuối: `2747 % 26 = 17`, và `list-trips[17]` = `trip-06570` (Airflow map_index 17).

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

Kết quả `idempotency.json` (trip `trip-00003`, 60 khung):
- Trước khi chạy lại: `silver.frames` có **1246 dòng**, dấu vân tay `6c7480dfde7e`.
- Sau **2 lần chạy lại** partition đó: vẫn **1246 dòng**, dấu vân tay vẫn `6c7480dfde7e` (`unchanged=true`).
- Khi ghi kiểu `append` sai cách, chạy 2 lần cho cùng trip: **120 dòng** cho một trip chỉ có 60 khung, tức dữ liệu bị nhân đôi.

`frames.py` ghi bằng `mode="overwrite", predicate="trip_id = '<trip>'"`. Delta làm việc này trong **một commit nguyên tử**: đánh dấu các file cũ của trip là đã xoá và thêm file mới vào cùng một mục transaction log. Người đọc chỉ thấy phiên bản cũ hoặc phiên bản mới, không bao giờ thấy trạng thái nửa vời. Nếu "xoá rồi ghi lại" thành hai thao tác và job sập giữa chừng (sau khi xoá, trước khi ghi), trip đó sẽ **mất dữ liệu** cho tới lần chạy sau, và downstream (gold) có thể đọc đúng lúc đó. Với Delta, sập giữa chừng chỉ để lại vài file Parquet mồ côi chưa được commit. Bảng vẫn ở phiên bản cũ, chạy lại thì cho đúng kết quả, và nếu cần vẫn có time travel để quay về.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Pipeline phân biệt bằng độ sáng trung bình (luma): **đen** là `brightness < 12` (`dark_luma`), **tối** là `12 ≤ brightness < 40` (`dark_soft`). Khung đen là lỗi cảm biến hoặc lỗi truyền, không có thông tin, nên bị loại khỏi gold (drive A có 6, B có 4, verify bắt 6/6 và 4/4). Khung tối là **điều kiện lái thật** (hầm, bóng râm, chạng vạng). Nó chỉ được gắn cờ `is_dark` và **giữ lại**: drive A giữ 4, drive B giữ **61** khung tối. Mình đồng ý: nếu bỏ khung tối thì model sẽ không bao giờ học cảnh hầm và đúng ở chỗ nguy hiểm nhất thì model mù. Cờ vẫn có ích để cân bằng hoặc phân tích theo điều kiện ánh sáng. Một điểm cần lưu ý: luật mờ chỉ áp khi `brightness ≥ 12`, nên khung đen không bị tính trùng là mờ.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

(Tổng: A 602 khung, B 644 khung. Ở ngưỡng 8 thì A bị coi là trùng 438 khung, B 348.)

Mình chọn **ngưỡng 2** như pipeline. Ở ngưỡng 0 chỉ bắt được khung giống hệt bit (61 ở A), bỏ sót đoạn camera đứng hình có nhiễu nén nhẹ. Từ ngưỡng 4 trở lên số khung "trùng" tăng vọt (A: 269, rồi 363 ở ngưỡng 6, tức 60% drive), nghĩa là đã loại cả khung khác nhau thật khi xe di chuyển chậm. Ngưỡng phụ thuộc tốc độ: drive A trung bình 38 km/h và 15% thời gian dưới 7 km/h, nên hai khung cách 0,5 s gần như giống nhau và tỷ lệ trùng cao (149/602). Drive B trung bình 71 km/h, cảnh đổi nhanh, nên ít trùng (53/644). Khi triển khai rộng, nên chọn ngưỡng **theo tốc độ** (hoặc lấy mẫu theo quãng đường thay vì theo thời gian) thay vì một hằng số.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Theo `leakage.json`: có 1019 khung hình dùng được, thuộc 26 trip. Nếu chia **theo khung hình** (ngẫu nhiên 70/15/15) thì **26/26 trip** bị chia cắt qua nhiều split, và cả **26 trip** đều có khung ở cả train lẫn test. Nếu chia **theo trip** (gold của pipeline) thì **0 trip** bị chia cắt. Test dbt chống rò rỉ và verify `no trip is in two splits` đều PASS.

Điểm kiểm thử đẹp giả tạo vì pipeline lấy mẫu 2 khung/giây: hai khung liền nhau chỉ cách 0,5 s, cùng đường, cùng xe phía trước, cùng ánh sáng (bước 3 cho thấy ở ngưỡng dHash 6, 363/602 khung của A gần như giống khung trước nó). Chia theo khung thì với mỗi ảnh test, gần như chắc chắn có "anh em sinh đôi" trong train. Model chỉ cần **nhớ** cảnh chứ không cần **tổng quát hoá**, nên điểm test gần với điểm train. Khi gặp một con đường mới thật sự thì điểm tụt. Chia theo trip (theo cảnh) mô phỏng đúng tình huống triển khai: test là những chuyến model chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` = 12 ký tự đầu của `sha256` trên danh sách `frame_id|split` đã sắp xếp (`publish.py`). Nó là **băm nội dung**, nên cùng nội dung thì luôn cùng version, khác dù chỉ một khung hay một split thì version khác. Ngày giờ thì đổi mỗi lần chạy dù dữ liệu y hệt. Số đếm thì phụ thuộc thứ tự chạy và máy chạy (máy giảng viên chạy lại sẽ ra số khác). Cả hai đều không trả lời được câu "hai dataset này có giống nhau không?".

Hai lần chạy liên tiếp: `step 4` cho `7e8e0335cc37`, chạy lại `cli export-dataset` vẫn ra **`7e8e0335cc37`**, và lần chạy đầu `step all --reset` cũng ra đúng số đó. Version không phụ thuộc giờ chạy.

MSSV được trộn vào cách xếp trip: `row_number() over (partition by drive_id order by md5(trip_id || LAB_STUDENT_ID))` trong `gold_dataset_frames.sql`. Thứ tự trip quyết định trip nào vào train/val/test, nên đổi MSSV thì đổi split, đổi nội dung, và đổi version. Mình thử đổi tạm sang `2A202602748`: ra version **`fedabb0286ef`** với train/val/test = 692/160/167 khung. Đặt lại `2A202602747` thì quay về **`7e8e0335cc37`** với 733/130/156. Tổng khung (1019) và số trip mỗi split (18/4/4) không đổi, chỉ đổi *trip nào* ở đâu. Vì vậy bạn bè cùng dữ liệu vẫn có version khác mình, và giảng viên dùng version để xác nhận bài chạy đúng với MSSV.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`manifest.json` của dataset `7e8e0335cc37` (1019 khung: train 733 / val 130 / test 156; drive_a 436, drive_b 583) ghi phần `lineage` như sau:
- Từ hai video gốc, nhận diện bằng sha256: drive_a `636ba1a98dfc5ba6…e7e9aa`, drive_b `bfa62703f645ff77…bbee9fdc`.
- Từ các bảng Delta ở đúng các phiên bản: `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.telemetry` v0, `silver.frames` **v27**, `silver.frames_flagged` v0, `silver.frame_telemetry` v0.
- Theo luật: loại `is_black, is_blurry, is_near_dup, not sync_ok`; split "per trip, stratified by drive, md5 order".
- Kèm `manifest_sha256 = ce27070a…a0163` của `manifest.parquet` (danh sách từng ảnh, split, đường dẫn).

`silver.frames` ở v27 là vì 26 trip ghi 26 commit, cộng các lần chạy lại của thí nghiệm idempotent. Số phiên bản ghi lại *lịch sử*, còn nội dung thì không đổi. Ba tháng sau, muốn tái tạo đúng dataset này thì cần: (1) hai video có đúng sha256 trên và hai file CSV telemetry; (2) dùng Delta time travel đọc các bảng ở đúng phiên bản trong `lineage`; (3) cùng commit mã của `dbt/` và `pipeline/` (các ngưỡng `dark_luma=12`, `blur_var=40`, `dup_hamming=2`, `sync_tol_ms`); (4) cùng `LAB_STUDENT_ID=2A202602747`. Chạy lại xong, so `dataset_version` và `manifest_sha256` để chứng minh tái tạo đúng. Một điểm còn thiếu: manifest chưa ghi git commit của mã và các biến dbt, nên phần (3) hiện phải tự ghi chép bên ngoài.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Đặt `LAB_FAIL_ONCE=trip-06570`, trigger `day09_reset`, rồi trigger `day09_video_pipeline` (run `manual__2026-10-12T18:56:43+00:00`):
- **1 task đỏ**: `silver_frames` map_index **17**. Log có dòng `RuntimeError: simulated crash while extracting trip-06570; run it again` (ảnh `crash_log.png`).
- **25 task `silver_frames`** của các trip khác vẫn **success**: mỗi trip là một mapped task độc lập nên lỗi không lan sang ngang.
- **7 task `upstream_failed`**: `dbt_run_silver, dbt_test_silver, dbt_run_gold, dbt_test_gold, publish_lake, export_dataset, publish_warehouse` (ảnh `crash_failed_graph.png`). Cổng chặn đúng chỗ: gold không được dựng khi silver thiếu một trip.
- Trên giao diện, mình bấm **Clear task** ở đúng `silver_frames [17]`, giữ mặc định *Downstream*. Hộp thoại báo **Affected Tasks: 8** (ảnh `crash_clear_dialog.png`). Đúng **8 task** chạy lại (1 trip + 7 task phía sau), 25 trip kia **không** chạy lại. Run chuyển xanh: 37/37 task success (ảnh `crash_rerun_green.png`).
- **Không nhân đôi**: `silver.frames` vẫn **1246 dòng**, 1246 `frame_id` khác nhau, `trip-06570` có 45 khung. Lần ghi lại là commit Delta v25, `WRITE` với `predicate = trip_id = 'trip-06570'`, `rows_added=45`. Lần thử đầu sập *trước khi ghi* nên không để lại gì; lần thử hai ghi đè đúng partition đó.
- `scripts/lab.sh verify` sau đó vẫn **27/27**, và `dataset_version` **không đổi**: `7e8e0335cc37`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **Giải mã video + trích khung trên một máy Airflow** (`silver_frames` chạy FFmpeg tuần tự trong container, slide 24). Riêng 2 drive × 5 phút đã mất khoảng 2 phút; 500 xe × hàng giờ video × 7 camera sẽ cần hàng nghìn giờ CPU mỗi ngày. Thay bằng: tách phần decode ra một cụm worker co giãn (Kubernetes/KubernetesExecutor hoặc Ray/Spark trên GPU với NVDEC), partition theo `vehicle/date/trip`, và **chỉ lấy mẫu khung cần thiết** (theo sự kiện, theo quãng đường) thay vì 2 khung/giây đều đặn.
2. **DuckDB một tiến trình + dbt chạy toàn bộ mỗi lần** (slide 22). DuckDB đọc Delta trên MinIO bằng một máy, lại rebuild hết silver/gold mỗi lần chạy. Khi dữ liệu lên hàng TB/ngày thì không còn vừa RAM và thời gian. Thay bằng: engine phân tán (Spark/Databricks, Trino) với **dbt incremental** theo partition ngày/xe, và `MERGE` trên Delta/Iceberg thay vì rebuild.
3. **Airflow dynamic mapping một task cho mỗi trip, cùng ingest theo batch file CSV** (slide 12, 23). 500 xe × khoảng 50 trip/ngày = 25 000 mapped task mỗi run: scheduler và metadata DB quá tải, mỗi trip lỗi lại phải clear tay. Thay bằng: ingest **streaming** (Kafka/Kinesis) để xe đẩy telemetry liên tục, Airflow chỉ điều phối ở mức lô (theo xe/giờ, hoặc theo sự kiện qua Datasets), có retry tự động, và theo dõi tỷ lệ quarantine theo xe thay vì nhìn từng task. Kèm theo đó, MinIO một node cũng cần thay bằng S3/object storage có lifecycle (video thô chuyển sang lớp lạnh).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Câu 1. "[2A202602747] Ty le khung bi loai theo trip"** (biểu đồ cột):
```sql
select trip_id, drive_id, split, frames, frames_kept, frames - frames_kept as dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped, round(avg_speed_kmh::numeric, 1) as avg_kmh
from marts.mart_trip_summary order by pct_dropped desc
```
Trung bình 18,2% khung bị loại (227/1246), nhưng phân bố rất lệch. Năm trip bị loại nhiều nhất đều thuộc drive_a: `trip-04622` 56,7% (34/60), `trip-03904` 50,0%, `trip-02086` 46,7% (17,6 km/h), `trip-08888` 40,0% (6,5 km/h), `trip-07774` 38,2%. Đối chiếu với silver, gần như toàn bộ là **khung gần trùng**: `trip-04622` có 34 khung trùng, `trip-08888` có 24. Riêng trip sập của mình, `trip-06570` (28,9%), lại chủ yếu bị loại vì **mờ** (9 khung mờ, 4 trùng). Có ba trip bị loại 0%.

**Câu 2 (vận hành). "[2A202602747] Ty le loai theo drive va dai toc do"** (bảng): nhóm trip theo `drive_id` và dải tốc độ trung bình (`< 20`, `20-50`, `>= 50 km/h`). Kết quả: drive_a loại 32,4% ở dải < 20 km/h, 22,6% ở 20-50, 25,8% ở ≥ 50. Drive_b chỉ 10,5% và 9,2%. Kết luận: xe chạy chậm làm tăng khung trùng (đúng như dự đoán ở Câu 5), nhưng drive_a vẫn loại nhiều ngay cả ở dải nhanh. Nguyên nhân là `trip-04622` (67 km/h mà vẫn 34 khung trùng): đó là dấu hiệu **camera đứng hình**, không phải xe đứng yên. Về vận hành: một trip chạy nhanh mà tỷ lệ trùng cao là cảnh báo lỗi camera, đáng gửi alert cho đội phần cứng thay vì coi là chuyện bình thường.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)

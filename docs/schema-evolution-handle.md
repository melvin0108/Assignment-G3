# Schema Evolution — Handling Rules (Bronze → Silver, CSV Input)

## Định nghĩa
Schema evolution: cấu trúc dữ liệu nguồn thay đổi theo thời gian. Pipeline phải xử lý mà không mất dữ liệu, không crash toàn hệ thống, không âm thầm sai lệch dữ liệu.

## Kiến trúc
```
Landing (CSV) → Bronze (permissive, không bao giờ chặn) → Silver (enforce contract, output cuối)
```

## Auto Loader config (CSV)
```python
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", "/schema/orders_bronze")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("cloudFiles.inferColumnTypes", "false")   # toàn bộ cột đọc dạng string
.option("header", "true")                          # bắt buộc có header
```

**Lưu ý riêng cho CSV:**
- Không có nested structure (không struct/array) → không có case "structural mismatch" như JSON.
- Phụ thuộc header để xác định tên cột, luôn bắt buộc `header=true`.
- Số lượng cột lệch so với header (thừa/thiếu delimiter) là lỗi phổ biến riêng của CSV.

## Quy tắc xử lý theo scenario

**Add column** → `addNewColumns` tự xử lý: fail 1 lần, cập nhật schema log, tự restart, cột mới ghi thẳng vào bronze dạng string.
→ Log warning: "Schema evolved: cột mới [tên cột] được thêm, batch [batch_id], timestamp [ts]."
→ Silver: review, chỉ đưa vào contract khi cần dùng.

**Missing column** → Delta tự fill NULL.
→ Log warning: "Schema evolved: cột [tên cột] không xuất hiện trong batch [batch_id], giá trị NULL."
→ Silver: theo dõi null rate, alert nếu spike bất thường so với baseline.

**Column reordering** → Không ảnh hưởng, Delta đọc theo tên cột (header), không theo vị trí.
→ Không cần log.

**Row CSV không parse được** (malformed CSV: quote/escape không hợp lệ) → raw row
được giữ trong `_corrupt_record`.
→ Log warning: "CSV malformed row: [file_path], đã giữ raw payload."

**Type change (scalar)** → Không xảy ra ở bronze (đã all-string). Convert ở silver bằng `try_cast()`, không dùng `cast()` cứng.
→ Parse fail → NULL, đẩy record sang bảng quarantine (nằm ở silver, không đẩy tiếp xuống gold).
→ Log warning: "Type cast failed: cột [tên cột], giá trị gốc [value], batch [batch_id]."

## Vai trò `_rescued_data` và `_corrupt_record`
`_rescued_data` giữ field không khớp schema/type/case. `_corrupt_record` giữ
toàn bộ raw CSV row khi parser không thể parse. Add/missing column vẫn do Auto
Loader/Delta xử lý riêng.

## Nguyên tắc log/warning
Mọi sự kiện schema evolution đều phải log warning, gồm: loại sự kiện, tên bảng/cột, timestamp/batch_id/file_path, và giá trị gốc nếu là type cast fail.

```python
def check_schema_drift(current_schema, previous_schema, batch_id):
    added = set(current_schema) - set(previous_schema)
    missing = set(previous_schema) - set(current_schema)
    if added:
        log.warning(f"[SCHEMA_EVOLUTION] batch={batch_id} added_columns={added}")
    if missing:
        log.warning(f"[SCHEMA_EVOLUTION] batch={batch_id} missing_columns={missing}")
```

```python
corrupt_count = bronze_df.filter(F.col("_corrupt_record").isNotNull()).count()
if corrupt_count > 0:
    log.warning(f"[SCHEMA_EVOLUTION] corrupt_rows={corrupt_count} — kiểm tra _corrupt_record")
```

```python
# Silver type cast + quarantine, log warning theo record
silver_df = bronze_df.withColumn("amount_clean", F.expr("try_cast(amount as decimal(18,2))"))

fail_df = silver_df.filter(F.col("amount").isNotNull() & F.col("amount_clean").isNull())
if fail_df.count() > 0:
    for row in fail_df.select("order_id", "amount").collect():
        log.warning(f"[SCHEMA_EVOLUTION] type_cast_failed column=amount order_id={row['order_id']} raw_value={row['amount']}")

fail_df.write.mode("append").saveAsTable("silver.orders_quarantine")
valid_df = silver_df.filter(~(F.col("amount").isNotNull() & F.col("amount_clean").isNull()))
valid_df.write.mode("append").saveAsTable("silver.orders")
```

## Nguyên tắc chung
1. Bronze: ưu tiên tuyệt đối không mất dữ liệu, không chặn pipeline, cột scalar lưu dạng string.
2. Silver: nơi duy nhất enforce type, validate, business rule — là **output cuối cùng của pipeline**.
3. Record lỗi → quarantine table nằm trong silver, không raise exception làm chết batch, không âm thầm drop.
4. Mọi sự kiện schema evolution đều phải log warning, kèm đủ context để trace lại.
5. Không có gold layer — silver chính là điểm dừng, downstream (nếu có) tự đọc trực tiếp từ silver + silver_quarantine.

## Áp dụng cho dự án G3

Dự án G3 vẫn giữ Gold AI-ready vì đây là yêu cầu bắt buộc của assignment.
Schema evolution chỉ tự động mở rộng Bronze; Silver và Gold dùng explicit
allow-list, vì vậy cột mới chỉ được đưa xuống từng layer sau khi contract, DQ,
PII và access policy đã được review.

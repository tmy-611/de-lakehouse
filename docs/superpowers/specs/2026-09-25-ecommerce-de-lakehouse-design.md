# Spec thiết kế — E-commerce Data Engineering Lakehouse (end-to-end)

- **Ngày:** 2026-09-25
- **Trạng thái:** Chờ người dùng review
- **Tác giả:** phiên brainstorming (user + agent)
- **Mục đích:** Dự án tự học Data Engineering end-to-end, áp dụng các core skill DE vào một hệ thống chạy thật bằng Docker, miễn phí, có thể dùng làm portfolio.

---

## 1. Mục tiêu & tiêu chí thành công

### 1.1 Mục tiêu
Xây dựng một **data platform end-to-end** theo kiến trúc lakehouse (medallion Bronze → Silver → Gold) cho domain e-commerce, chạy hoàn toàn local bằng Docker Compose, để học và thực hành các core skill DE.

### 1.2 Đối tượng
Một người học đã **vững Python + SQL nhưng mới với DE tooling**.

### 1.3 Tiêu chí thành công
- Tự tay dựng được toàn bộ vòng đời dữ liệu: ingest → lake → transform → model → orchestrate → quality → CI.
- Mỗi thành phần đều chạy thật (không mock), `docker compose up` là lên hệ thống.
- Tái sử dụng được kiến thức cho công việc/phỏng vấn DE.
- Không phải copy-paste tutorial: hiểu được **why** đằng sau từng bước.

---

## 2. Phạm vi

### 2.1 Trong phạm vi
- Nguồn batch: Olist Brazilian E-Commerce (dataset thật, tải 1 lần, nạp vào Postgres OLTP).
- Nguồn streaming (Phase 2): event producer tự sinh.
- Kiến trúc medallion Bronze/Silver/Gold, star schema, SCD Type 2.
- Orchestration nhiều DAG bằng Airflow + Datasets.
- Data quality bằng dbt tests + Great Expectations.
- Đóng gói Docker Compose, CI bằng GitHub Actions.

### 2.2 Ngoài phạm vi (YAGNI)
- Cloud managed services (giữ local; chỉ thiết kế để sẵn sàng migrate sau).
- Multi-tenant, phân quyền người dùng, high availability.
- BI dashboard phức tạp (Metabase chỉ là tùy chọn demo).
- Machine learning / feature store.

---

## 3. Kiến trúc tổng thể

```
Nguồn batch:  Postgres OLTP (olist_source: customers/products/orders/...)
                    │  (Python extract incremental)
                    ▼
                 [ BRONZE ]  Parquet thô + metadata, trên Garage (S3-compatible)
                    │
Streaming (P2):   Kafka  ──▼
Producer events →         [ BRONZE Kafka sink ]
                    │
                    ▼
                 [ SILVER ]  Làm sạch, dedup, chuẩn hóa, type-cast — PySpark
                    │            (ghi Parquet lên Garage + load vào Postgres schema silver)
                    ▼
                 [ GOLD ]  Star schema + marts — dbt (trên Postgres warehouse)
                    │
                    ▼
              Warehouse/BI mart  → (tùy chọn) Metabase dashboard

Xuyên suốt:  Airflow (nhiều DAG + Datasets) │ dbt tests + Great Expectations │ Docker Compose │ GitHub Actions
```

---

## 4. Stack & lý do chọn

| Lớp | Công nghệ | Vì sao (benefit) |
|---|---|---|
| Orchestration | Apache Airflow (LocalExecutor, Docker) | Xuất hiện nhiều nhất trong JD DE; học DAG/scheduling/backfill |
| Lake storage | Garage (S3 API) + Parquet partitioned | S3 API giống cloud → tư duy lake transferable |
| Batch processing | PySpark | Skill bắt buộc; xử lý khối lượng lớn, dùng chung cho streaming |
| Modeling/ELT | dbt trên Postgres | Star schema, test, lineage, snapshot, docs — rất hay hỏi |
| Streaming (P2) | Kafka (KRaft) + Spark Structured Streaming | Real-time pipeline, Kafka là keyword phổ biến |
| CDC (P2) | Debezium | Kỹ năng CDC đang "hot" trong JD hiện đại |
| Data quality | Great Expectations + dbt tests | Thể hiện tư duy data reliability |
| Packaging/CI | Docker Compose + GitHub Actions | DevOps cho data platform |
| Query/BI (tùy chọn) | DuckDB hoặc Metabase | Demo kết quả trực quan |

> **Ruling 2026-09-25 — object storage MinIO → Garage v2.3.0.** MinIO đã xoá `minio/minio` và `minio/mc` khỏi Docker Hub (11/09/2026) và bản trên Quay không pull ẩn danh được. Ta dùng **Garage v2.3.0** (`dxflrs/garage:v2.3.0`) — vẫn là S3 API nên mọi đường dẫn `s3://lake/...`, s3fs và Spark s3a không đổi; chỉ đổi endpoint/credentials. Bucket `lake` và access key được tạo tự động bằng `garage server --single-node --default-bucket`.

---

## 5. Phân phase

Mỗi phase là một sản phẩm hoàn chỉnh, có thể dừng ở bất kỳ phase nào.

- **Phase 1 — Batch lakehouse cốt lõi:** Postgres → Bronze Parquet (Garage) → Spark Silver → dbt Gold (star schema, SCD2) → Airflow (nhiều DAG + Datasets) → quality → Docker Compose + CI.
- **Phase 2 — Real-time + CDC:** event producer → Kafka → Spark Structured Streaming ghi Silver; Debezium CDC từ Postgres.
- **Phase 3 — Hardening:** Apache Iceberg + query engine (DuckDB/Trino), observability (Prometheus/Grafana hoặc OpenLineage/Marquez), IaC & CI/CD nâng cao.

**Phạm vi chốt của Phase 1:** 1 nguồn batch (Olist với 9 bảng), 3 bảng fact + 4 dim, DAG có backfill, data quality checks, chạy bằng `docker compose up`.

---

## 6. Nguồn dữ liệu & ingestion

### 6.1 Nguồn batch — Olist Brazilian E-Commerce
- Dataset công khai trên Kaggle, miễn phí, ~100k đơn hàng (2016–2018), 9 bảng quan hệ:
  - Fact: `orders`, `order_items`, `order_payments`, `order_reviews`
  - Dim: `customers`, `products`, `sellers`, `product_category_translation`, `geolocation`
- Dữ liệu thật có lỗi tự nhiên (null, timestamp lệch, duplicate, category thiếu) → buộc làm data cleaning/quality thật.
- **Cách dùng:** `scripts/download_olist.py` tải CSV + checksum → `scripts/seed_postgres.py` nạp vào Postgres DB `olist_source` đóng vai OLTP.
- **Raw data KHÔNG commit** vào git (chỉ commit script + checksum).
- **License:** CC BY-NC-SA 4.0 → dùng học/portfolio được, không thương mại.

### 6.2 Nguồn streaming (Phase 2)
- `producer/` (Python + Faker) sinh event `view / add_to_cart / purchase`, dùng `customer_id`/`product_id` thật từ Olist để đảm bảo nhất quán tham chiếu.

### 6.3 Cơ chế ingestion (Phase 1)
- Extract **incremental** dựa trên cột watermark (vd `order_purchase_timestamp`, `updated_at`), ghi Bronze Parquet partitioned theo `ingest_date`.
- Idempotent, replay được, không full-load lại.
- Cấu hình hóa nguồn trong `config/sources.yaml` (bảng, cột watermark, khóa chính, kiểu dữ liệu) để dễ thêm nguồn.

---

## 7. Mô hình dữ liệu

### 7.1 Bronze — raw, append-only, 1:1 nguồn
- Đường dẫn: `s3://lake/bronze/olist/<table>/ingest_date=YYYY-MM-DD/*.parquet`
- Giữ nguyên cột gốc dạng string + metadata: `_ingested_at`, `_batch_id`, `_source_file`, `_row_hash`.
- Không biến đổi → replay/audit được.

### 7.2 Silver — sạch, typed
- Mỗi entity một bảng: `customers`, `orders`, `order_items`, `products`, `sellers`, `payments`, `reviews`, `geolocation`.
- Quy tắc: cast type + trim; dedup theo PK lấy bản mới nhất theo watermark; chuẩn hóa null/giá trị lạ; join `product_category_translation` để có tên category tiếng Anh; geolocation gộp theo `zip_code_prefix`.
- Grain: `order_items` giữ 1 dòng/item; `orders` 1 dòng/order.
- Lưu: Parquet trên Garage **và** load vào Postgres schema `silver`.

### 7.3 Gold — star schema (dbt trên Postgres)

| Bảng | Grain | Nội dung chính |
|---|---|---|
| `dim_customer` | 1 dòng / phiên bản khách | SCD Type 2 (dbt snapshot) theo `customer_unique_id`, tracking city/state |
| `dim_product` | 1 dòng / sản phẩm | category EN/PT, thuộc tính |
| `dim_seller` | 1 dòng / seller | vị trí |
| `dim_date` | 1 dòng / ngày | year/quarter/month/weekday |
| `fact_order_items` | 1 dòng / order item | price, freight_value, quantity |
| `fact_orders` | 1 dòng / order | tổng tiền, payment, delivery_days, review_score |
| `fact_payments` | 1 dòng / payment | method, installments, value |

### 7.4 Marts (Gold)
- `mart_sales_daily`
- `mart_customer_rfm` (Recency/Frequency/Monetary)
- `mart_product_performance`
- `mart_delivery_sla`

### 7.5 Phân chia trách nhiệm
- **Spark** = Bronze→Silver: khối lượng lớn, typing, dedup, sẵn sàng streaming.
- **dbt** = Silver→Gold/Marts: modeling SQL, macro, test, lineage, snapshot (SCD2), docs.
- Warehouse Postgres chứa `silver` (Spark load) + `gold`/`marts` (dbt build). Lake Garage giữ Bronze + Silver Parquet.

---

## 8. Orchestration, idempotency & backfill

### 8.1 DAGs (nhiều DAG + Datasets)
```
dag_ingest_olist      @daily  → ghi Bronze, phát Dataset bronze://olist
dag_silver_transform  triggered by bronze://olist → Spark Bronze→Silver, phát silver://olist
dag_gold_dbt          triggered by silver://olist → dbt snapshot+run+test, phát gold://olist
dag_quality_ge        triggered by gold://olist → Great Expectations checkpoints
dag_backfill          manual/param (start,end) → kích chuỗi trên theo từng logical date
```
- **Chốt phiên bản:** dùng **Airflow 2.10.x** (ổn định provider, hỗ trợ **Datasets**). Airflow 3 đổi tên Datasets thành **Assets** — ghi chú làm lộ trình nâng cấp Phase 3.
- Spark standalone (`spark-master` + `spark-worker`), Airflow gọi qua `SparkSubmitOperator`.
- dbt chạy bằng `BashOperator` trong môi trường có dbt trỏ vào warehouse.

### 8.2 Idempotency
- Bronze: ghi partition `ingest_date=<ds>` + `batch_id`; chạy lại overwrite đúng partition đó.
- Silver: merge/overwrite partition theo PK + watermark.
- dbt: fact lớn `incremental` với `unique_key` (merge/delete+insert); dim nhỏ dùng `table`.
- Mọi thứ suy ra từ **logical date** (`{{ ds }}`, `data_interval_start`), không dùng `now()` → backfill chính xác.

### 8.3 Backfill
- Backfill chạy trên `dag_ingest_olist` (DAG nguồn), sau đó chuỗi DAG downstream tự kích qua Datasets:
  `airflow dags backfill -s 2023-01-01 -e 2023-01-07 dag_ingest_olist`
- Mục tiêu: reprocess đúng khoảng ngày, chứng minh tính idempotent.

### 8.4 Retry & xử lý lỗi
- `retries=3`, `retry_exponential_backoff`, `execution_timeout`, SLA miss → cảnh báo.
- **Quarantine:** bản ghi lỗi ở Silver (null PK, ngày không hợp lệ, FK mồ côi) vào bảng `silver_rejects` kèm lý do, không làm chết pipeline.
- `on_failure_callback` bắn cảnh báo (Email/Telegram).

---

## 9. Data quality, testing & observability

### 9.1 Tầng 1 — Schema/Data contract (lúc ingest)
- `config/sources.yaml` khai báo cột, type, PK, watermark.
- Ingest validate cột/type; record sai schema vào `bronze/_rejects`.

### 9.2 Tầng 2 — dbt tests (Silver→Gold)
- `not_null`, `unique` trên PK/surrogate key.
- `relationships` cho FK fact→dim.
- `accepted_values` cho status, payment_type, review_score.
- `dbt source freshness` trên Silver.
- Custom singular tests: `payment_value >= 0`, `delivery_days >= 0`.

### 9.3 Tầng 3 — Great Expectations (trên Gold)
- Suite: row count trong khoảng kỳ vọng, không null bất thường, doanh thu ≥ 0, phân phối review_score hợp lý.
- Render **Data Docs** (HTML).

### 9.4 Tầng 4 — Unit tests Python (pytest)
- Hàm extract/watermark, dedup, type-cast (pure functions).
- Producer (P2): test schema event.
- Transform PySpark: test bằng local SparkSession với fixture nhỏ.

### 9.5 Observability
- Airflow UI + task logs, duration trend.
- `on_failure_callback` → cảnh báo.
- (P3) Prometheus + Grafana hoặc OpenLineage/Marquez cho lineage.

### 9.6 CI (GitHub Actions, mỗi PR)
`ruff` lint → `pytest` → `sqlfluff` → `dbt parse` + `dbt build --empty` → `docker compose config`.

---

## 10. Cấu trúc repo & vận hành

```
project2/
├─ docker-compose.yml
├─ Makefile
├─ .env.example
├─ pyproject.toml
├─ airflow/dags/ + airflow/requirements.txt
├─ ingestion/ (sources.yaml, extract.py, load_bronze.py)
├─ spark/jobs/ + spark/transforms/
├─ dbt/ (models/staging, models/marts, snapshots, tests)
├─ quality/great_expectations/
├─ producer/            # Phase 2
├─ scripts/ (download_olist.py, seed_postgres.py)
├─ tests/
├─ docs/ (architecture.md + notes/ + superpowers/specs/)
└─ .github/workflows/ci.yml
```

### 10.1 Docker Compose services (Phase 1)
- `postgres` với 3 DB tách biệt: `olist_source`, `airflow`, `warehouse`.
- `garage` (S3-compatible, tự tạo bucket + access key) — Phase 1.
- `airflow-webserver` + `airflow-scheduler` (LocalExecutor).
- `spark-master` + `spark-worker`.
- Phase 2 thêm `kafka`/`debezium`; Phase 3 thêm `trino`/`grafana`; tùy chọn `metabase`.

### 10.2 Config & bảo mật
- Secret qua `.env` (gitignore); `.env.example` commit.
- Airflow Connections/Variables cho Postgres/Garage.
- Kaggle token qua env. Không hardcode secret.

### 10.3 Lệnh vận hành
```
make up          # dựng toàn stack
make download    # tải Olist + checksum
make seed        # nạp OLTP vào Postgres
make backfill START=2023-01-01 END=2023-01-07
make test        # pytest + dbt test + GE
make lint        # ruff + sqlfluff
make docs        # dbt docs + GE data docs
```

---

## 11. Yêu cầu về tài liệu hướng dẫn (BẮT BUỘC)

Mọi tài liệu hướng dẫn/triển khai trong dự án (`README.md`, `docs/architecture.md`, `docs/notes/**`, comment trong Makefile/script hướng dẫn) **KHÔNG được chỉ nêu ngắn gọn đây là bước gì**. Mỗi bước/khái niệm phải trình bày đầy đủ theo template 4 mục:

1. **What — Đây là gì:** mô tả bước/khái niệm một cách dễ hiểu.
2. **Why — Vì sao làm bước này:** động cơ, vấn đề nó giải quyết.
3. **Impact — Ảnh hưởng đến hệ thống:** nó tác động tới các thành phần khác, tới dữ liệu, tới vận hành như thế nào.
4. **Benefit — Lợi ích:** giá trị mang lại (về kỹ thuật, về reliability, về kỹ năng/nghề nghiệp).

Kèm theo, khi phù hợp: ví dụ minh họa, lệnh chạy, cách kiểm chứng (verify) kết quả.

Yêu cầu này áp dụng cho cả spec, plan, và mọi tài liệu sinh ra trong quá trình triển khai.

---

## 12. Definition of Done — Phase 1

1. `docker compose up` một lệnh là cả hệ thống lên.
2. Backfill một khoảng ngày ra đúng star schema; chạy lại **không nhân đôi** (idempotent).
3. Toàn bộ dbt tests + GE checks pass.
4. CI xanh trên PR.
5. `README.md` có sơ đồ kiến trúc, cách chạy, ghi chú quyết định thiết kế, và tài liệu hướng dẫn theo template mục 11.

---

## 13. Rủi ro & giảm thiểu

| Rủi ro | Giảm thiểu |
|---|---|
| Stack nhiều service, dễ sa lầy | Chia phase; Phase 1 giới hạn 1 nguồn batch |
| Spark + Airflow trong Docker cồng kềnh | Dùng `SparkSubmitOperator` + standalone master; giới hạn tài nguyên |
| Kaggle download cần token | `make download` đọc token từ env; có hướng dẫn; checksum để verify |
| Kim thời gian lớn, dễ bỏ dở | Mỗi phase là sản phẩm hoàn chỉnh, dừng được |

---

## 14. Lộ trình

- **Phase 1:** Batch lakehouse cốt lõi (portfolio đã đủ tốt).
- **Phase 2:** Kafka + Spark Structured Streaming + Debezium CDC.
- **Phase 3:** Iceberg/Trino + observability + IaC/CI-CD nâng cao.
- Mỗi phase kèm `docs/notes/` giải thích khái niệm theo template mục 11.

---

## 15. Bước tiếp theo

Sau khi spec này được duyệt: chuyển sang **writing-plans** để tạo kế hoạch triển khai chi tiết cho Phase 1.

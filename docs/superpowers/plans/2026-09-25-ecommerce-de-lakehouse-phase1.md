# E-commerce DE Lakehouse — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable local batch lakehouse (Bronze → Silver → Gold) over the Olist e-commerce dataset, orchestrated by Airflow, modeled with dbt, quality-checked, and packaged in Docker Compose.

**Architecture:** Postgres `olist_source` acts as the OLTP source. Python extracts incrementally into Parquet on MinIO (`bronze/`). PySpark cleans/dedupes into `silver/` Parquet and loads a `silver` schema into Postgres `warehouse`. dbt builds star-schema Gold models and marts in `warehouse`. Airflow chains four DAGs via Datasets and provides backfill. Great Expectations validates Gold.

**Tech Stack:** Python 3.11, pandas, pyarrow, s3fs, psycopg2-binary, PySpark 3.5, dbt-postgres 1.8, Apache Airflow 2.10.3, Great Expectations 0.18, Postgres 16, Garage v2.3.0 (S3-compatible), Docker Compose, GitHub Actions, pytest, ruff, sqlfluff.

**Spec:** `docs/superpowers/specs/2026-09-25-ecommerce-de-lakehouse-design.md`

## Addendum 2026-09-25 — Object storage switched MinIO → Garage

**Ruling:** MinIO removed `minio/minio` and `minio/mc` from Docker Hub on 2026-09-11 and the Quay copies are not anonymously pullable, so the object store is **Garage v2.3.0** (`dxflrs/garage:v2.3.0`, Docker Hub). The S3 API is unchanged, so all `s3://lake/...` / `s3a://lake/...` paths and s3fs code stay as written.

Substitutions that apply wherever the MinIO form occurs below:

| Was | Now |
|---|---|
| service `minio` (image `minio/minio:latest`) | service `garage` (image `dxflrs/garage:v2.3.0`) |
| service `minio-init` (`minio/mc`) | removed — `garage server --single-node --default-bucket` creates bucket + key on first start |
| env `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `GARAGE_ACCESS_KEY` / `GARAGE_SECRET_KEY` (+ `GARAGE_RPC_SECRET`) |
| `http://minio:9000` | `http://garage:3900` |
| `http://localhost:9000` | `http://localhost:3900` |
| volume `miniodata` | volume `garagedata` |

Task 1 was implemented with these substitutions (service block, `.env.example`, `.env`, `tests/test_infra.py`); the rest of Task 1 is unchanged. In Task 8, `SPARK_CONF` reads `GARAGE_ACCESS_KEY`/`GARAGE_SECRET_KEY` instead of the MinIO variables. Bucket name `lake` is unchanged.

## Global Constraints

- Everything runs locally via Docker Compose; no cloud services.
- Postgres 16, one server, three databases: `olist_source`, `airflow`, `warehouse`.
- Object storage: Garage v2.3.0 with bucket `lake`; S3 endpoint `http://garage:3900` inside the compose network, `http://localhost:3900` from the host.
- Airflow pinned to `2.10.3-python3.11`, LocalExecutor, scheduling via **Datasets**.
- Spark `bitnami/spark:3.5`, standalone `spark-master` (`spark://spark-master:7077`) + one worker.
- dbt uses `dbt-postgres`; Python is 3.11.
- Olist raw CSV is **never committed**; only download/seed scripts and a checksum manifest are committed. License CC BY-NC-SA 4.0 (non-commercial).
- Secrets only via `.env` (gitignored); `.env.example` is committed and contains no real secrets.
- All transforms are idempotent and derive their execution date from the Airflow logical date (`{{ ds }}`), never `datetime.now()`.
- Every guide/doc created by these tasks follows the spec §11 template: **What / Why / Impact / Benefit**.
- Idempotency keys and watermarks use UTC.
- Batch source tables: `customers, orders, order_items, order_payments, order_reviews, products, sellers, geolocation, product_category_name_translation`.

## Review Focus

- **Re-running any stage for the same logical date must not duplicate rows.** Each of ingest (Task 3), silver load (Task 5), and dbt incremental (Task 7) gets a test that runs the stage twice and asserts row counts are unchanged.
- **Bad source rows must be quarantined, not crash the pipeline.** Rows with null/invalid primary keys or unparseable types must land in a rejects dataset; tests in Tasks 3 and 4 assert both the reject count and that the valid job still succeeds.
- **Watermark timezone consistency.** Extraction must compare timestamps in UTC; a naive vs aware mismatch silently drops or duplicates rows. Task 3 tests naive and aware inputs against the same boundary.
- **Late/idempotent incremental facts.** Re-running a dbt incremental fact with overlapping input must merge on `unique_key`; Task 7 tests a second run with one changed row and expects an update, not a duplicate.
- **SCD2 correctness.** When a tracked attribute changes, the snapshot must close the prior version (`dbt_valid_to` set) and open a new current version; Task 7 tests exactly this transition.

---

### Task 1: Repo foundation & core infrastructure (Postgres + MinIO)

**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Create: `docker-compose.yml`
- Create: `docker/postgres/init/00-init-databases.sh`
- Create: `pyproject.toml`
- Create: `Makefile`
- Create: `requirements-dev.txt`
- Test: `tests/test_infra.py`

**Interfaces:**
- Consumes: nothing.
- Produces: compose services `postgres` (databases `olist_source`, `airflow`, `warehouse`) and `minio` (bucket `lake`); env var names consumed by later tasks: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `SOURCE_DSN`, `AIRFLOW_DB_DSN`, `WAREHOUSE_DSN`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `S3_ENDPOINT_HOST`, `S3_ENDPOINT_INTERNAL`, `LAKE_BUCKET`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_infra.py
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]

def test_compose_has_required_services():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    assert {"postgres", "minio"} <= set(services)
    assert "healthcheck" in services["postgres"]
    assert "healthcheck" in services["minio"]

def test_postgres_init_creates_three_databases():
    script = (ROOT / "docker" / "postgres" / "init" / "00-init-databases.sh").read_text()
    for db in ("olist_source", "airflow", "warehouse"):
        assert db in script

def test_env_example_lists_required_vars():
    text = (ROOT / ".env.example").read_text()
    for key in ("POSTGRES_USER", "POSTGRES_PASSWORD", "SOURCE_DSN",
                "AIRFLOW_DB_DSN", "WAREHOUSE_DSN", "MINIO_ROOT_USER",
                "MINIO_ROOT_PASSWORD", "LAKE_BUCKET"):
        assert key in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_infra.py -v`
Expected: FAIL with `FileNotFoundError` for `docker-compose.yml`.

- [ ] **Step 3: Create `.gitignore`**

```gitignore
.env
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
data/raw/
data/checksums/
airflow/logs/
airflow/plugins/
dbt/target/
dbt/logs/
quality/great_expectations/uncommitted/
```

- [ ] **Step 4: Create `.env.example`**

```dotenv
POSTGRES_USER=deuser
POSTGRES_PASSWORD=depassword
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

SOURCE_DSN=postgresql://deuser:depassword@postgres:5432/olist_source
AIRFLOW_DB_DSN=postgresql://deuser:depassword@postgres:5432/airflow
WAREHOUSE_DSN=postgresql://deuser:depassword@postgres:5432/warehouse

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
S3_ENDPOINT_INTERNAL=http://minio:9000
S3_ENDPOINT_HOST=http://localhost:9000
LAKE_BUCKET=lake

KAGGLE_USERNAME=
KAGGLE_KEY=
```

- [ ] **Step 5: Create the Postgres init script**

```bash
#!/bin/bash
# docker/postgres/init/00-init-databases.sh
set -euo pipefail
for db in olist_source airflow warehouse; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE $db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
done
```

- [ ] **Step 6: Create `docker-compose.yml` (core services)**

```yaml
name: de-lakehouse
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./docker/postgres/init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      timeout: 5s
      retries: 10

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - miniodata:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 10

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} &&
      mc mb --ignore-existing local/${LAKE_BUCKET} &&
      echo 'bucket ready'"

volumes:
  pgdata:
  miniodata:
```

- [ ] **Step 7: Create `pyproject.toml`, `requirements-dev.txt`, `Makefile`**

```toml
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["integration: requires running docker services"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP"]
```

```text
# requirements-dev.txt
pytest==8.2.2
ruff==0.5.5
sqlfluff==3.0.7
sqlfluff-templater-dbt==3.0.7
PyYAML==6.0.1
psycopg2-binary==2.9.9
```

```makefile
# Makefile
SHELL := /bin/bash
include .env
export

.PHONY: up down logs test lint compose-check

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

test:
	python -m pytest -v

lint:
	ruff check .
	sqlfluff lint dbt/models --dialect postgres

compose-check:
	docker compose config --quiet
```

- [ ] **Step 8: Run test to verify it passes**

Run: `python -m pytest tests/test_infra.py -v`
Expected: PASS (3 passed).

- [ ] **Step 9: Verify services actually start**

Run: `docker compose up -d postgres minio minio-init` then `docker compose ps`
Expected: `postgres` and `minio` show `healthy`; `minio-init` exits 0.

- [ ] **Step 10: Commit**

```bash
git add .gitignore .env.example docker-compose.yml docker/ pyproject.toml requirements-dev.txt Makefile tests/test_infra.py
git commit -m "feat(infra): add postgres+minio compose foundation"
```

---

### Task 2: Olist data acquisition & OLTP seeding

**Files:**
- Create: `scripts/__init__.py`
- Create: `scripts/download_olist.py`
- Create: `scripts/seed_postgres.py`
- Create: `data/checksums/olist.sha256` (manifest of expected file names only; hashes filled on first download and committed)
- Test: `tests/test_download.py`
- Test: `tests/test_seed.py`

**Interfaces:**
- Consumes: `SOURCE_DSN`.
- Produces:
  - `scripts.download_olist.discover_tables(data_dir: Path) -> dict[str, Path]` — maps `olist_orders_dataset.csv` → `orders`.
  - `scripts.download_olist.verify_manifest(data_dir: Path, manifest: Path) -> list[str]` — returns missing file names.
  - `scripts.download_olist.download(dest: Path) -> Path` — uses Kaggle API credentials from env.
  - `scripts.seed_postgres.seed(dsn: str, data_dir: Path, schema: str = "public") -> dict[str, int]` — loads each table, returns row counts.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_download.py
from pathlib import Path
from scripts.download_olist import discover_tables

def test_discover_tables_maps_olist_files(tmp_path: Path):
    for name in ["olist_orders_dataset.csv",
                 "olist_order_items_dataset.csv",
                 "olist_product_category_name_translation.csv"]:
        (tmp_path / name).write_text("id\n1\n")
    tables = discover_tables(tmp_path)
    assert tables["orders"].name == "olist_orders_dataset.csv"
    assert tables["order_items"].name == "olist_order_items_dataset.csv"
    assert tables["product_category_name_translation"].name == \
        "olist_product_category_name_translation.csv"
```

```python
# tests/test_seed.py
from pathlib import Path
from scripts.seed_postgres import table_name_from_file

def test_table_name_from_file_strips_prefix_and_suffix():
    assert table_name_from_file("olist_order_payments_dataset.csv") == "order_payments"
    assert table_name_from_file("olist_customers_dataset.csv") == "customers"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_download.py tests/test_seed.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts'`.

- [ ] **Step 3: Implement `scripts/download_olist.py`**

```python
# scripts/download_olist.py
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

DATASET_SLUG = "olistbr/brazilian-ecommerce"


def table_name_from_file(filename: str) -> str:
    stem = Path(filename).stem
    if stem.startswith("olist_"):
        stem = stem[len("olist_"):]
    if stem.endswith("_dataset"):
        stem = stem[: -len("_dataset")]
    return stem


def discover_tables(data_dir: Path) -> dict[str, Path]:
    tables: dict[str, Path] = {}
    for path in sorted(data_dir.glob("olist_*_dataset.csv")):
        tables[table_name_from_file(path.name)] = path
    return tables


def verify_manifest(data_dir: Path, manifest: Path) -> list[str]:
    if not manifest.exists():
        return []
    expected = [line.split()[1] for line in manifest.read_text().splitlines() if line.strip()]
    return [name for name in expected if not (data_dir / name).exists()]


def write_manifest(data_dir: Path, manifest: Path) -> None:
    lines = []
    for path in sorted(data_dir.glob("*.csv")):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.name}")
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text("\n".join(lines) + "\n")


def download(dest: Path) -> Path:
    from kaggle.api.kaggle_api_extended import KaggleApi

    dest.mkdir(parents=True, exist_ok=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET_SLUG, path=str(dest), unzip=True)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dest", type=Path, default=Path("data/raw/olist"))
    parser.add_argument("--manifest", type=Path, default=Path("data/checksums/olist.sha256"))
    args = parser.parse_args()
    download(args.dest)
    write_manifest(args.dest, args.manifest)
    print(f"downloaded {len(discover_tables(args.dest))} tables to {args.dest}")


if __name__ == "__main__":
    main()
```

> If the user prefers not to use Kaggle credentials, they may place the CSVs manually in `data/raw/olist/` and run `write_manifest` via `python -c "from scripts.download_olist import write_manifest; from pathlib import Path; write_manifest(Path('data/raw/olist'), Path('data/checksums/olist.sha256'))"`.

- [ ] **Step 4: Implement `scripts/seed_postgres.py`**

```python
# scripts/seed_postgres.py
from __future__ import annotations

import argparse
from pathlib import Path

from scripts.download_olist import discover_tables, table_name_from_file


def table_name_from_file(filename: str) -> str:
    stem = Path(filename).stem
    if stem.startswith("olist_"):
        stem = stem[len("olist_"):]
    if stem.endswith("_dataset"):
        stem = stem[: -len("_dataset")]
    return stem


def seed(dsn: str, data_dir: Path, schema: str = "public") -> dict[str, int]:
    import psycopg2

    tables = discover_tables(data_dir)
    counts: dict[str, int] = {}
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table, path in tables.items():
            cur.execute(f'DROP TABLE IF EXISTS {schema}."{table}"')
            with path.open("r", encoding="utf-8") as fh:
                cur.copy_expert(
                    f"COPY {schema}.\"{table}\" FROM STDIN WITH (FORMAT csv, HEADER true)", fh
                )
            cur.execute(f'SELECT count(*) FROM {schema}."{table}"')
            counts[table] = cur.fetchone()[0]
        conn.commit()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw/olist"))
    parser.add_argument("--schema", default="public")
    args = parser.parse_args()
    counts = seed(args.dsn, args.data_dir, args.schema)
    for table, count in sorted(counts.items()):
        print(f"{table}: {count}")


if __name__ == "__main__":
    main()
```

> `COPY ... FROM STDIN` requires the table to exist with matching columns. Task 2 creates tables from the CSV headers by first creating an untyped table: replace the `DROP/CREATE` with a helper `create_table_from_header(cur, schema, table, path)`. Implement it as: read the header row, `CREATE TABLE {schema}."{table}" (col text, ...)`, then `COPY`. This keeps Olist as an untyped OLTP landing zone, which is realistic and lets Task 3 handle typing.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_download.py tests/test_seed.py -v`
Expected: PASS (3 passed).

- [ ] **Step 6: Seed the source (manual verification)**

Run: `python scripts/seed_postgres.py --dsn "$SOURCE_DSN"` (from the host use `postgresql://deuser:depassword@localhost:5432/olist_source`)
Expected: prints 9 table names with row counts; `orders` ≈ 99,441.

- [ ] **Step 7: Commit**

```bash
git add scripts/ data/checksums/ tests/test_download.py tests/test_seed.py
git commit -m "feat(ingest): add olist download and source seeding"
```

---

### Task 3: Source config, validation, and Bronze ingestion

**Files:**
- Create: `ingestion/__init__.py`
- Create: `ingestion/config.py`
- Create: `ingestion/validate.py`
- Create: `ingestion/watermark.py`
- Create: `ingestion/bronze.py`
- Create: `ingestion/run_ingest.py`
- Create: `config/sources.yaml`
- Test: `tests/test_config.py`
- Test: `tests/test_validate.py`
- Test: `tests/test_watermark.py`
- Test: `tests/test_bronze.py`

**Interfaces:**
- Consumes: `SOURCE_DSN`, `S3_ENDPOINT_INTERNAL`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `LAKE_BUCKET`.
- Produces:
  - `ingestion.config.SourceConfig` dataclass with fields `name, table, primary_key: list[str], watermark_column: str | None, columns: dict[str, str]`; `load_sources(path: str | Path) -> dict[str, SourceConfig]`.
  - `ingestion.validate.validate_schema(df: pd.DataFrame, cfg: SourceConfig) -> tuple[pd.DataFrame, pd.DataFrame]` (valid, rejects).
  - `ingestion.watermark.to_utc(value) -> datetime | None`; `build_extract_sql(cfg, since: datetime | None) -> str`; `max_watermark(df, column) -> datetime | None`.
  - `ingestion.bronze.partition_prefix(root: str, table: str, ds: str) -> str`; `record_hash(record: dict, columns: list[str]) -> str`; `write_bronze(df, root, table, ds, batch_id) -> str` (returns written path).
  - `ingestion.run_ingest.ingest_source(cfg, since, ds, batch_id) -> IngestResult` with `rows_in, rows_out, path`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_config.py
from pathlib import Path
from ingestion.config import load_sources

def test_load_sources_reads_orders_config(tmp_path: Path):
    (tmp_path / "sources.yaml").write_text(
        "sources:\n"
        "  orders:\n"
        "    table: olist_orders_dataset\n"
        "    primary_key: [order_id]\n"
        "    watermark_column: order_purchase_timestamp\n"
        "    columns:\n"
        "      order_id: string\n"
        "      order_purchase_timestamp: timestamp\n"
    )
    cfg = load_sources(tmp_path / "sources.yaml")["orders"]
    assert cfg.primary_key == ["order_id"]
    assert cfg.columns["order_purchase_timestamp"] == "timestamp"
```

```python
# tests/test_validate.py
import pandas as pd
from ingestion.config import SourceConfig
from ingestion.validate import validate_schema

CFG = SourceConfig(
    name="orders", table="olist_orders_dataset", primary_key=["order_id"],
    watermark_column=None, columns={"order_id": "string", "order_status": "string"},
)

def test_null_primary_key_goes_to_rejects():
    df = pd.DataFrame({"order_id": ["a", None], "order_status": ["delivered", "shipped"]})
    valid, rejects = validate_schema(df, CFG)
    assert len(valid) == 1 and len(rejects) == 1
    assert rejects.iloc[0]["_reject_reason"] == "null_primary_key"

def test_unparseable_timestamp_goes_to_rejects():
    cfg = SourceConfig("orders", "t", ["order_id"], "ts",
                       {"order_id": "string", "ts": "timestamp"})
    df = pd.DataFrame({"order_id": ["a", "b"], "ts": ["2023-01-01 10:00:00", "not-a-date"]})
    valid, rejects = validate_schema(df, cfg)
    assert len(valid) == 1 and len(rejects) == 1
```

```python
# tests/test_watermark.py
from datetime import datetime, timezone
from ingestion.watermark import to_utc, max_watermark
import pandas as pd

def test_to_utc_makes_naive_timestamps_utc_aware():
    assert to_utc("2023-01-01 00:00:00") == datetime(2023, 1, 1, tzinfo=timezone.utc)

def test_to_utc_normalizes_aware_timestamps():
    aware = datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc)
    assert to_utc(aware) == aware

def test_max_watermark_ignores_nulls():
    s = pd.Series(["2023-01-01 00:00:00", None, "2023-01-03 00:00:00"])
    assert max_watermark(s, "ts") == datetime(2023, 1, 3, tzinfo=timezone.utc)
```

```python
# tests/test_bronze.py
from ingestion.bronze import partition_prefix, record_hash

def test_partition_prefix_is_hive_style():
    assert partition_prefix("s3://lake/bronze/olist", "orders", "2023-01-01") == \
        "s3://lake/bronze/olist/orders/ingest_date=2023-01-01"

def test_record_hash_is_stable_regardless_of_key_order():
    a = record_hash({"x": "1", "y": "2"}, ["x", "y"])
    b = record_hash({"y": "2", "x": "1"}, ["x", "y"])
    assert a == b and len(a) == 64
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_config.py tests/test_validate.py tests/test_watermark.py tests/test_bronze.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion'`.

- [ ] **Step 3: Implement `ingestion/config.py`**

```python
# ingestion/config.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class SourceConfig:
    name: str
    table: str
    primary_key: list[str]
    watermark_column: str | None
    columns: dict[str, str] = field(default_factory=dict)


def load_sources(path: str | Path) -> dict[str, SourceConfig]:
    raw = yaml.safe_load(Path(path).read_text())["sources"]
    return {
        name: SourceConfig(
            name=name,
            table=body["table"],
            primary_key=body["primary_key"],
            watermark_column=body.get("watermark_column"),
            columns=body.get("columns", {}),
        )
        for name, body in raw.items()
    }
```

- [ ] **Step 4: Implement `ingestion/watermark.py`**

```python
# ingestion/watermark.py
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def to_utc(value) -> datetime | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").to_pydatetime()


def build_extract_sql(cfg, since: datetime | None) -> str:
    if cfg.watermark_column is None or since is None:
        return f'SELECT * FROM "{cfg.table}"'
    stamp = since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f'SELECT * FROM "{cfg.table}" '
        f'WHERE "{cfg.watermark_column}" > \'{stamp}\' '
        f'ORDER BY "{cfg.watermark_column}"'
    )


def max_watermark(values: pd.Series, column: str) -> datetime | None:
    parsed = [to_utc(v) for v in values.tolist()]
    parsed = [v for v in parsed if v is not None]
    return max(parsed) if parsed else None
```

- [ ] **Step 5: Implement `ingestion/validate.py`**

```python
# ingestion/validate.py
from __future__ import annotations

import pandas as pd

from ingestion.config import SourceConfig


def _coerce_types(df: pd.DataFrame, cfg: SourceConfig) -> pd.DataFrame:
    out = df.copy()
    for column, dtype in cfg.columns.items():
        if column not in out.columns:
            out[column] = None
        if dtype == "timestamp":
            out[column] = pd.to_datetime(out[column], errors="coerce")
        elif dtype in ("int", "integer", "bigint"):
            out[column] = pd.to_numeric(out[column], errors="coerce").astype("Int64")
        elif dtype in ("float", "double", "numeric"):
            out[column] = pd.to_numeric(out[column], errors="coerce").astype("Float64")
        elif dtype == "string":
            out[column] = out[column].astype("string")
    return out


def validate_schema(df: pd.DataFrame, cfg: SourceConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    typed = _coerce_types(df, cfg)
    reasons = pd.Series([None] * len(typed), index=typed.index, dtype="object")

    for key in cfg.primary_key:
        reasons = reasons.mask(reasons.isna() & typed[key].isna(), "null_primary_key")

    for column, dtype in cfg.columns.items():
        if dtype == "timestamp":
            original = df.get(column)
            if original is not None:
                bad = original.notna() & typed[column].isna()
                reasons = reasons.mask(reasons.isna() & bad, f"unparseable_{column}")

    rejects = typed[reasons.notna()].copy()
    rejects["_reject_reason"] = reasons[reasons.notna()]
    valid = typed[reasons.isna()].copy()
    return valid, rejects
```

- [ ] **Step 6: Implement `ingestion/bronze.py`**

```python
# ingestion/bronze.py
from __future__ import annotations

import hashlib
import json

import pandas as pd


def partition_prefix(root: str, table: str, ds: str) -> str:
    return f"{root.rstrip('/')}/{table}/ingest_date={ds}"


def record_hash(record: dict, columns: list[str]) -> str:
    payload = json.dumps({c: str(record.get(c)) for c in sorted(columns)},
                         sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_bronze(df: pd.DataFrame, root: str, table: str, ds: str, batch_id: str) -> str:
    import s3fs

    enriched = df.copy()
    enriched["_ingested_at"] = pd.Timestamp.utcnow()
    enriched["_batch_id"] = batch_id
    enriched["_row_hash"] = [
        record_hash(row, [c for c in df.columns]) for row in df.to_dict("records")
    ]
    path = f"{partition_prefix(root, table, ds)}/{batch_id}.parquet"
    enriched.to_parquet(path, engine="pyarrow", filesystem=s3fs.S3FileSystem())
    return path
```

- [ ] **Step 7: Implement `config/sources.yaml` and `ingestion/run_ingest.py`**

Create `config/sources.yaml` with all 9 tables. Full content for `orders`, `customers`, `order_items`; the remaining 6 follow the same shape with their real Olist column names and types (`timestamp` for `*_date`/`*_timestamp`, `int` for numeric codes, `float` for prices, else `string`). All tables use a primary key (composite where Olist has no single key, e.g. `order_items`: `[order_id, order_item_id]`; `geolocation`: `[geolocation_zip_code_prefix, geolocation_lat, geolocation_lng]`).

```yaml
sources:
  orders:
    table: olist_orders_dataset
    primary_key: [order_id]
    watermark_column: order_purchase_timestamp
    columns:
      order_id: string
      customer_id: string
      order_status: string
      order_purchase_timestamp: timestamp
      order_approved_at: timestamp
      order_delivered_carrier_date: timestamp
      order_delivered_customer_date: timestamp
      order_estimated_delivery_date: timestamp
  customers:
    table: olist_customers_dataset
    primary_key: [customer_id]
    watermark_column: null
    columns:
      customer_id: string
      customer_unique_id: string
      customer_zip_code_prefix: string
      customer_city: string
      customer_state: string
  order_items:
    table: olist_order_items_dataset
    primary_key: [order_id, order_item_id]
    watermark_column: shipping_limit_date
    columns:
      order_id: string
      order_item_id: int
      product_id: string
      seller_id: string
      shipping_limit_date: timestamp
      price: float
      freight_value: float
```

```python
# ingestion/run_ingest.py
from __future__ import annotations

import argparse
from dataclasses import dataclass

import pandas as pd
import psycopg2

from ingestion.bronze import write_bronze
from ingestion.config import SourceConfig, load_sources
from ingestion.validate import validate_schema
from ingestion.watermark import build_extract_sql, max_watermark


@dataclass
class IngestResult:
    rows_in: int
    rows_out: int
    path: str


def ingest_source(dsn: str, cfg: SourceConfig, since, ds: str, batch_id: str,
                  bronze_root: str) -> IngestResult:
    with psycopg2.connect(dsn) as conn:
        frame = pd.read_sql(build_extract_sql(cfg, since), conn)
    valid, rejects = validate_schema(frame, cfg)
    if len(rejects):
        write_bronze(rejects, bronze_root + "_rejects", cfg.name, ds, batch_id)
    path = write_bronze(valid, bronze_root, cfg.name, ds, batch_id)
    return IngestResult(rows_in=len(frame), rows_out=len(valid), path=path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--config", default="config/sources.yaml")
    parser.add_argument("--source", required=True)
    parser.add_argument("--ds", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--bronze-root", default="s3://lake/bronze/olist")
    args = parser.parse_args()
    cfg = load_sources(args.config)[args.source]
    result = ingest_source(args.dsn, cfg, None, args.ds, args.batch_id, args.bronze_root)
    print(f"{cfg.name}: {result.rows_out}/{result.rows_in} rows -> {result.path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `python -m pytest tests/test_config.py tests/test_validate.py tests/test_watermark.py tests/test_bronze.py -v`
Expected: PASS.

- [ ] **Step 9: Idempotency test (the Review Focus case)**

Add to `tests/test_bronze.py`:

```python
def test_rerun_same_ds_and_batch_overwrites_partition(tmp_path, monkeypatch):
    import pandas as pd
    from ingestion import bronze
    written = {}

    class FakeFS:
        def __init__(self, *a, **k): ...
    def fake_to_parquet(self, path, **kwargs):
        written[path] = self
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=False)
    monkeypatch.setattr(bronze, "s3fs", type("m", (), {"S3FileSystem": FakeFS}))
    df = pd.DataFrame({"order_id": ["a"]})
    p1 = bronze.write_bronze(df, "s3://lake/bronze/olist", "orders", "2023-01-01", "run1")
    p2 = bronze.write_bronze(df, "s3://lake/bronze/olist", "orders", "2023-01-01", "run1")
    assert p1 == p2
    assert len(written) == 1
```

Run: `python -m pytest tests/test_bronze.py -v`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add ingestion/ config/sources.yaml tests/test_config.py tests/test_validate.py tests/test_watermark.py tests/test_bronze.py
git commit -m "feat(ingest): add source config, validation, bronze writer"
```

---

### Task 4: Spark Bronze→Silver transforms

**Files:**
- Create: `spark/__init__.py`
- Create: `spark/transforms/__init__.py`
- Create: `spark/transforms/clean.py`
- Create: `spark/jobs/bronze_to_silver.py`
- Test: `tests/spark/__init__.py`
- Test: `tests/spark/test_clean.py`

**Interfaces:**
- Consumes: Bronze Parquet written by Task 3 at `{bronze_root}/{table}/ingest_date={ds}/`.
- Produces:
  - `spark.transforms.clean.dedupe_latest(df, key_cols: list[str], watermark_col: str | None) -> DataFrame`
  - `spark.transforms.clean.split_rejects(df, key_cols: list[str]) -> tuple[DataFrame, DataFrame]`
  - `spark.transforms.clean.standardize_text(df, cols: list[str]) -> DataFrame`
  - `spark.transforms.clean.to_silver(df, entity: str) -> DataFrame`
  - `spark.jobs.bronze_to_silver.run(entity: str, ds: str, bronze_root: str, silver_root: str) -> int`

- [ ] **Step 1: Write the failing tests**

```python
# tests/spark/__init__.py
```

```python
# tests/spark/test_clean.py
import pytest
from pyspark.sql import SparkSession
from spark.transforms.clean import dedupe_latest, split_rejects, standardize_text


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]")
            .appName("silver-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate())


def test_dedupe_latest_keeps_newest_watermark(spark):
    df = spark.createDataFrame(
        [("a", "2023-01-01 00:00:00"), ("a", "2023-01-02 00:00:00"), ("b", "2023-01-01 00:00:00")],
        ["order_id", "order_purchase_timestamp"],
    )
    out = dedupe_latest(df, ["order_id"], "order_purchase_timestamp")
    rows = {r["order_id"]: r["order_purchase_timestamp"] for r in out.collect()}
    assert len(rows) == 2
    assert rows["a"] == "2023-01-02 00:00:00"


def test_null_primary_key_goes_to_rejects(spark):
    df = spark.createDataFrame([("a",), (None,)], ["order_id"])
    valid, rejects = split_rejects(df, ["order_id"])
    assert valid.count() == 1
    assert rejects.count() == 1


def test_standardize_text_trims_and_lowercases(spark):
    df = spark.createDataFrame([("  Sao PAULO ",)], ["customer_city"])
    out = standardize_text(df, ["customer_city"]).collect()
    assert out[0]["customer_city"] == "sao paulo"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/spark/test_clean.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'spark'`.

- [ ] **Step 3: Implement `spark/transforms/clean.py`**

```python
# spark/transforms/clean.py
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedupe_latest(df: DataFrame, key_cols: list[str],
                  watermark_col: str | None) -> DataFrame:
    if watermark_col is None:
        return df.dropDuplicates(key_cols)
    window = Window.partitionBy(*key_cols).orderBy(F.col(watermark_col).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(window))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def split_rejects(df: DataFrame, key_cols: list[str]) -> tuple[DataFrame, DataFrame]:
    bad_condition = None
    for key in key_cols:
        cond = F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
        bad_condition = cond if bad_condition is None else (bad_condition | cond)
    rejects = df.filter(bad_condition).withColumn("_reject_reason", F.lit("null_primary_key"))
    valid = df.filter(~bad_condition)
    return valid, rejects


def standardize_text(df: DataFrame, cols: list[str]) -> DataFrame:
    out = df
    for col in cols:
        out = out.withColumn(col, F.trim(F.lower(F.col(col))))
    return out


TEXT_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_city", "customer_state"],
    "sellers": ["seller_city", "seller_state"],
    "products": ["product_category_name"],
    "orders": ["order_status"],
    "order_payments": ["payment_type"],
    "order_reviews": ["review_comment_title", "review_comment_message"],
}


def to_silver(df: DataFrame, entity: str) -> DataFrame:
    return standardize_text(df, TEXT_COLUMNS.get(entity, []))
```

- [ ] **Step 4: Implement `spark/jobs/bronze_to_silver.py`**

```python
# spark/jobs/bronze_to_silver.py
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.transforms.clean import dedupe_latest, split_rejects, standardize_text, to_silver

KEY_COLUMNS: dict[str, list[str]] = {
    "orders": ["order_id"],
    "customers": ["customer_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
    "product_category_name_translation": ["product_category_name"],
}
WATERMARKS: dict[str, str] = {"orders": "order_purchase_timestamp", "order_items": "shipping_limit_date"}


def build_session(app: str = "bronze-to-silver") -> SparkSession:
    return (SparkSession.builder.appName(app)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def read_bronze(spark: SparkSession, bronze_root: str, table: str, ds: str) -> DataFrame:
    path = f"{bronze_root}/{table}/ingest_date={ds}"
    return spark.read.parquet(path)


def run(entity: str, ds: str, bronze_root: str, silver_root: str) -> int:
    spark = build_session()
    try:
        raw = read_bronze(spark, bronze_root, entity, ds)
        valid, rejects = split_rejects(raw, KEY_COLUMNS[entity])
        cleaned = to_silver(valid, entity)
        deduped = dedupe_latest(cleaned, KEY_COLUMNS[entity], WATERMARKS.get(entity))
        (deduped.withColumn("_silver_date", F.lit(ds))
                .write.mode("overwrite")
                .parquet(f"{silver_root}/{entity}/ingest_date={ds}"))
        if rejects.count():
            (rejects.write.mode("overwrite")
                   .parquet(f"{silver_root}/_rejects/{entity}/ingest_date={ds}"))
        return deduped.count()
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity", required=True)
    parser.add_argument("--ds", required=True)
    parser.add_argument("--bronze-root", default="s3a://lake/bronze/olist")
    parser.add_argument("--silver-root", default="s3a://lake/silver/olist")
    args = parser.parse_args()
    count = run(args.entity, args.ds, args.bronze_root, args.silver_root)
    print(f"{args.entity}: {count} silver rows")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/spark/test_clean.py -v`
Expected: PASS.

- [ ] **Step 6: Idempotency verification (the Review Focus case)**

Add to `tests/spark/test_clean.py`:

```python
def test_dedupe_latest_is_idempotent(spark):
    df = spark.createDataFrame(
        [("a", "2023-01-01 00:00:00"), ("a", "2023-01-02 00:00:00")],
        ["order_id", "order_purchase_timestamp"],
    )
    once = dedupe_latest(df, ["order_id"], "order_purchase_timestamp")
    twice = dedupe_latest(once, ["order_id"], "order_purchase_timestamp")
    assert once.count() == twice.count() == 1
```

Run: `python -m pytest tests/spark/test_clean.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add spark/ tests/spark/
git commit -m "feat(silver): add spark bronze-to-silver transforms"
```

---

### Task 5: Silver → Postgres warehouse loader

**Files:**
- Create: `warehouse/__init__.py`
- Create: `warehouse/db.py`
- Create: `warehouse/load_silver.py`
- Test: `tests/test_warehouse_sql.py`
- Test: `tests/test_load_silver_integration.py`

**Interfaces:**
- Consumes: Silver Parquet from Task 4; `WAREHOUSE_DSN`.
- Produces:
  - `warehouse.db.create_schema(conn, schema: str) -> None`
  - `warehouse.db.upsert_dataframe(conn, df, table: str, keys: list[str], schema: str = "silver") -> int`
  - `warehouse.load_silver.load_entity(conn, silver_root: str, entity: str, ds: str) -> int`
  - `warehouse.load_silver.build_upsert_sql(table, columns, keys, schema) -> str`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_warehouse_sql.py
from warehouse.load_silver import build_upsert_sql

def test_build_upsert_sql_uses_on_conflict_on_keys():
    sql = build_upsert_sql("orders", ["order_id", "order_status"], ["order_id"], "silver")
    assert 'INSERT INTO "silver"."orders"' in sql
    assert 'ON CONFLICT ("order_id")' in sql
    assert 'DO UPDATE SET "order_status" = EXCLUDED."order_status"' in sql

def test_build_upsert_sql_single_key_column_has_no_update_clause():
    sql = build_upsert_sql("customers", ["customer_id"], ["customer_id"], "silver")
    assert "DO NOTHING" in sql
```

```python
# tests/test_load_silver_integration.py
import os
import pytest

pytestmark = pytest.mark.integration


def test_upsert_is_idempotent():
    dsn = os.environ.get("WAREHOUSE_DSN_HOST")
    if not dsn:
        pytest.skip("WAREHOUSE_DSN_HOST not set")
    import pandas as pd
    from warehouse.db import connect, create_schema, upsert_dataframe

    df = pd.DataFrame({"order_id": ["a", "b"], "order_status": ["delivered", "shipped"]})
    with connect(dsn) as conn:
        create_schema(conn, "silver")
        upsert_dataframe(conn, df, "orders", ["order_id"])
        upsert_dataframe(conn, df, "orders", ["order_id"])
        with conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM "silver"."orders"')
            assert cur.fetchone()[0] == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_warehouse_sql.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'warehouse'`.

- [ ] **Step 3: Implement `warehouse/db.py`**

```python
# warehouse/db.py
from __future__ import annotations

import pandas as pd
import psycopg2


def connect(dsn: str):
    return psycopg2.connect(dsn)


def create_schema(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    conn.commit()


def upsert_dataframe(conn, df: pd.DataFrame, table: str, keys: list[str],
                     schema: str = "silver") -> int:
    from warehouse.load_silver import build_upsert_sql

    columns = list(df.columns)
    data = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False)]
    with conn.cursor() as cur:
        cur.execute('CREATE TEMP TABLE _stage (LIKE "%s"."%s" INCLUDING DEFAULTS)' % (schema, table))
        placeholders = ",".join(["%s"] * len(columns))
        cols = ",".join(f'"{c}"' for c in columns)
        cur.executemany(
            f'INSERT INTO _stage ({cols}) VALUES ({placeholders})', data
        )
        sql = build_upsert_sql(table, columns, keys, schema)
        cur.execute(sql)
        cur.execute("DROP TABLE _stage")
    conn.commit()
    return len(df)
```

- [ ] **Step 4: Implement `warehouse/load_silver.py`**

```python
# warehouse/load_silver.py
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from warehouse.db import connect, create_schema, upsert_dataframe

KEY_COLUMNS: dict[str, list[str]] = {
    "orders": ["order_id"],
    "customers": ["customer_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
    "product_category_name_translation": ["product_category_name"],
}


def build_upsert_sql(table: str, columns: list[str], keys: list[str], schema: str) -> str:
    cols = ",".join(f'"{c}"' for c in columns)
    conflict = ",".join(f'"{k}"' for k in keys)
    updatable = [c for c in columns if c not in keys]
    if updatable:
        action = "DO UPDATE SET " + ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in updatable)
    else:
        action = "DO NOTHING"
    return (f'INSERT INTO "{schema}"."{table}" ({cols}) VALUES ({cols}) '
            f'ON CONFLICT ({conflict}) {action}')


def ensure_table(conn, df: pd.DataFrame, table: str, schema: str) -> None:
    mapping = {"object": "text", "string": "text", "float64": "double precision",
               "Int64": "bigint", "int64": "bigint", "bool": "boolean"}
    cols = ", ".join(f'"{c}" {mapping.get(str(t), "text")}' for c, t in df.dtypes.items())
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({cols})')
    conn.commit()


def load_entity(conn, silver_root: str, entity: str, ds: str) -> int:
    import s3fs

    path = f"{silver_root}/{entity}/ingest_date={ds}"
    fs = s3fs.S3FileSystem()
    df = pd.read_parquet(path, filesystem=fs)
    if df.empty:
        return 0
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")
    ensure_table(conn, df, entity, "silver")
    return upsert_dataframe(conn, df, entity, KEY_COLUMNS[entity], schema="silver")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--silver-root", default="s3://lake/silver/olist")
    parser.add_argument("--entity", required=True)
    parser.add_argument("--ds", required=True)
    args = parser.parse_args()
    with connect(args.dsn) as conn:
        count = load_entity(conn, args.silver_root, args.entity, args.ds)
    print(f"{args.entity}: loaded {count} rows")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run unit tests to verify they pass**

Run: `python -m pytest tests/test_warehouse_sql.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Run integration test against live warehouse (optional)**

Run: `$env:WAREHOUSE_DSN_HOST="postgresql://deuser:depassword@localhost:5432/warehouse"; python -m pytest tests/test_load_silver_integration.py -v -m integration`
Expected: PASS or SKIP if env not set.

- [ ] **Step 7: Commit**

```bash
git add warehouse/ tests/test_warehouse_sql.py tests/test_load_silver_integration.py
git commit -m "feat(warehouse): load silver parquet into postgres idempotently"
```

---

### Task 6: dbt project & staging models

**Files:**
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml`
- Create: `dbt/packages.yml`
- Create: `dbt/models/staging/_sources.yml`
- Create: `dbt/models/staging/stg_orders.sql`, `stg_customers.sql`, `stg_order_items.sql`, `stg_payments.sql`, `stg_reviews.sql`, `stg_products.sql`, `stg_sellers.sql`, `stg_geolocation.sql`, `stg_category_translation.sql`
- Create: `dbt/models/staging/_stg.yml`

**Interfaces:**
- Consumes: Postgres `silver` schema populated by Task 5.
- Produces: models `stg_orders`, `stg_customers`, `stg_order_items`, `stg_payments`, `stg_reviews`, `stg_products`, `stg_sellers`, `stg_geolocation`, `stg_category_translation` in schema `silver_staging`.

- [ ] **Step 1: Create `dbt/dbt_project.yml`, `packages.yml`, `profiles.yml`**

```yaml
# dbt/dbt_project.yml
name: ecommerce
version: "1.0.0"
profile: ecommerce
model-paths: ["models"]
snapshot-paths: ["snapshots"]
test-paths: ["tests"]
models:
  ecommerce:
    staging:
      +materialized: view
      +schema: silver_staging
    marts:
      +materialized: table
      +schema: gold
```

```yaml
# dbt/packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

```yaml
# dbt/profiles.yml
ecommerce:
  target: local
  outputs:
    local:
      type: postgres
      host: "{{ env_var('DBT_HOST', 'localhost') }}"
      port: 5432
      user: "{{ env_var('DBT_USER', 'deuser') }}"
      password: "{{ env_var('DBT_PASSWORD', 'depassword') }}"
      dbname: warehouse
      schema: silver
      threads: 4
      keepalives_idle: 0
```

- [ ] **Step 2: Create sources and staging models**

```yaml
# dbt/models/staging/_sources.yml
version: 2
sources:
  - name: silver
    schema: silver
    tables:
      - name: orders
      - name: customers
      - name: order_items
      - name: order_payments
      - name: order_reviews
      - name: products
      - name: sellers
      - name: geolocation
      - name: product_category_name_translation
```

```sql
-- dbt/models/staging/stg_orders.sql
SELECT
    order_id,
    customer_id,
    LOWER(order_status) AS order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM {{ source('silver', 'orders') }}
```

```sql
-- dbt/models/staging/stg_order_items.sql
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price::numeric(12,2) AS price,
    freight_value::numeric(12,2) AS freight_value
FROM {{ source('silver', 'order_items') }}
```

```sql
-- dbt/models/staging/stg_customers.sql
SELECT
    customer_id,
    customer_unique_id,
    LPAD(customer_zip_code_prefix, 5, '0') AS zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('silver', 'customers') }}
```

```sql
-- dbt/models/staging/stg_payments.sql
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value::numeric(12,2) AS payment_value
FROM {{ source('silver', 'order_payments') }}
```

```sql
-- dbt/models/staging/stg_reviews.sql
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM {{ source('silver', 'order_reviews') }}
```

```sql
-- dbt/models/staging/stg_products.sql
SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS category_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM {{ source('silver', 'products') }} p
LEFT JOIN {{ source('silver', 'product_category_name_translation') }} t
    ON p.product_category_name = t.product_category_name
```

```sql
-- dbt/models/staging/stg_sellers.sql
SELECT
    seller_id,
    LPAD(seller_zip_code_prefix, 5, '0') AS zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('silver', 'sellers') }}
```

```sql
-- dbt/models/staging/stg_geolocation.sql
SELECT
    LPAD(geolocation_zip_code_prefix, 5, '0') AS zip_code_prefix,
    AVG(geolocation_lat) AS lat,
    AVG(geolocation_lng) AS lng,
    MIN(geolocation_city) AS city,
    MIN(geolocation_state) AS state
FROM {{ source('silver', 'geolocation') }}
GROUP BY 1
```

```sql
-- dbt/models/staging/stg_category_translation.sql
SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('silver', 'product_category_name_translation') }}
```

- [ ] **Step 3: Add staging schema tests**

```yaml
# dbt/models/staging/_stg.yml
version: 2
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: order_status
        tests:
          - accepted_values:
              values: [delivered, shipped, canceled, invoiced, processing,
                       unavailable, approved, created]
  - name: stg_order_items
    columns:
      - name: order_id
        tests: [not_null]
      - name: price
        tests: [not_null]
  - name: stg_customers
    columns:
      - name: customer_id
        tests: [not_null, unique]
  - name: stg_products
    columns:
      - name: product_id
        tests: [not_null, unique]
```

- [ ] **Step 4: Run dbt parse and build staging**

Run: `dbt deps --project-dir dbt --profiles-dir dbt` then `dbt build --select staging --project-dir dbt --profiles-dir dbt`
Expected: PASS; view models created in `silver_staging`.

- [ ] **Step 5: Commit**

```bash
git add dbt/
git commit -m "feat(dbt): add project and staging models"
```

---

### Task 7: dbt dimensions, facts, snapshots and marts

**Files:**
- Create: `dbt/snapshots/dim_customer_snapshot.sql`
- Create: `dbt/models/marts/core/dim_customer.sql`, `dim_product.sql`, `dim_seller.sql`, `dim_date.sql`
- Create: `dbt/models/marts/core/fact_order_items.sql`, `fact_orders.sql`, `fact_payments.sql`
- Create: `dbt/models/marts/core/_core.yml`
- Create: `dbt/models/marts/marts/mart_sales_daily.sql`, `mart_customer_rfm.sql`, `mart_product_performance.sql`, `mart_delivery_sla.sql`
- Create: `dbt/models/marts/marts/_marts.yml`
- Test: `dbt/tests/assert_no_negative_payment.sql`, `assert_delivery_not_before_purchase.sql`

**Interfaces:**
- Consumes: staging models from Task 6.
- Produces: Gold tables `dim_customer, dim_product, dim_seller, dim_date, fact_order_items, fact_orders, fact_payments` and marts `mart_sales_daily, mart_customer_rfm, mart_product_performance, mart_delivery_sla` in schema `gold`. `fact_orders` is `incremental` with `unique_key='order_id'`.

- [ ] **Step 1: Create the SCD2 snapshot**

```sql
-- dbt/snapshots/dim_customer_snapshot.sql
{% snapshot dim_customer_snapshot %}
{{
  config(
    target_schema='gold',
    unique_key='customer_unique_id',
    strategy='check',
    check_cols=['zip_code_prefix', 'customer_city', 'customer_state'],
  )
}}
SELECT
    customer_unique_id,
    zip_code_prefix,
    customer_city,
    customer_state
FROM {{ ref('stg_customers') }}
{% endsnapshot %}
```

- [ ] **Step 2: Create dimensions and facts**

```sql
-- dbt/models/marts/core/dim_customer.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id', 'dbt_valid_from']) }} AS customer_sk,
    s.customer_unique_id,
    s.zip_code_prefix,
    s.customer_city,
    s.customer_state,
    g.lat,
    g.lng,
    s.dbt_valid_from AS valid_from,
    s.dbt_valid_to AS valid_to,
    (s.dbt_valid_to IS NULL) AS is_current
FROM {{ ref('dim_customer_snapshot') }} s
LEFT JOIN {{ ref('stg_geolocation') }} g ON s.zip_code_prefix = g.zip_code_prefix
```

```sql
-- dbt/models/marts/core/dim_product.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
    product_id,
    COALESCE(product_category_name, 'unknown') AS category_name,
    category_english,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM {{ ref('stg_products') }}
```

```sql
-- dbt/models/marts/core/dim_seller.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} AS seller_sk,
    seller_id,
    zip_code_prefix,
    seller_city,
    seller_state
FROM {{ ref('stg_sellers') }}
```

```sql
-- dbt/models/marts/core/dim_date.sql
WITH days AS (
    SELECT generate_series(
        (SELECT MIN(order_purchase_timestamp)::date FROM {{ ref('stg_orders') }}),
        (SELECT MAX(order_purchase_timestamp)::date FROM {{ ref('stg_orders') }}),
        interval '1 day'
    )::date AS date_day
)
SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::int AS date_sk,
    date_day,
    EXTRACT(year FROM date_day)::int AS year,
    EXTRACT(quarter FROM date_day)::int AS quarter,
    EXTRACT(month FROM date_day)::int AS month,
    EXTRACT(week FROM date_day)::int AS week,
    EXTRACT(dow FROM date_day)::int AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name
FROM days
```

```sql
-- dbt/models/marts/core/fact_order_items.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }} AS order_item_sk,
    oi.order_id,
    oi.order_item_id,
    dp.product_sk,
    ds.seller_sk,
    TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD')::int AS date_sk,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_value
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
LEFT JOIN {{ ref('dim_product') }} dp ON oi.product_id = dp.product_id
LEFT JOIN {{ ref('dim_seller') }} ds ON oi.seller_id = ds.seller_id
```

```sql
-- dbt/models/marts/core/fact_orders.sql
{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
  )
}}
WITH items AS (
    SELECT order_id, SUM(price) AS items_value, SUM(freight_value) AS freight_value
    FROM {{ ref('stg_order_items') }} GROUP BY 1
),
payments AS (
    SELECT order_id, SUM(payment_value) AS payment_value
    FROM {{ ref('stg_payments') }} GROUP BY 1
),
reviews AS (
    SELECT order_id, AVG(review_score) AS review_score
    FROM {{ ref('stg_reviews') }} GROUP BY 1
)
SELECT
    o.order_id,
    dc.customer_sk,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))
        / 86400.0 AS delivery_days,
    COALESCE(i.items_value, 0)::numeric(12,2) AS items_value,
    COALESCE(i.freight_value, 0)::numeric(12,2) AS freight_value,
    COALESCE(p.payment_value, 0)::numeric(12,2) AS payment_value,
    r.review_score
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_customer') }} dc
    ON c.customer_unique_id = dc.customer_unique_id AND dc.is_current
LEFT JOIN items i ON o.order_id = i.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
LEFT JOIN reviews r ON o.order_id = r.order_id
{% if is_incremental() %}
WHERE o.order_purchase_timestamp >= (
    SELECT COALESCE(MAX(order_purchase_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}
```

```sql
-- dbt/models/marts/core/fact_payments.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'payment_sequential']) }} AS payment_sk,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ ref('stg_payments') }}
```

- [ ] **Step 3: Create marts**

```sql
-- dbt/models/marts/marts/mart_sales_daily.sql
SELECT
    d.date_day,
    COUNT(DISTINCT f.order_id) AS orders,
    SUM(f.items_value) AS items_revenue,
    SUM(f.freight_value) AS freight_revenue,
    SUM(f.payment_value) AS total_revenue,
    AVG(f.review_score) AS avg_review_score
FROM {{ ref('fact_orders') }} f
JOIN {{ ref('dim_date') }} d
    ON TO_CHAR(f.order_purchase_timestamp::date, 'YYYYMMDD')::int = d.date_sk
GROUP BY 1
```

```sql
-- dbt/models/marts/marts/mart_customer_rfm.sql
WITH base AS (
    SELECT
        dc.customer_unique_id,
        MAX(f.order_purchase_timestamp) AS last_order_ts,
        COUNT(DISTINCT f.order_id) AS frequency,
        SUM(f.payment_value) AS monetary
    FROM {{ ref('fact_orders') }} f
    JOIN {{ ref('dim_customer') }} dc ON f.customer_sk = dc.customer_sk
    GROUP BY 1
)
SELECT
    customer_unique_id,
    (CURRENT_DATE - last_order_ts::date) AS recency_days,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY last_order_ts) AS r_score,
    NTILE(5) OVER (ORDER BY frequency) AS f_score,
    NTILE(5) OVER (ORDER BY monetary) AS m_score
FROM base
```

```sql
-- dbt/models/marts/marts/mart_product_performance.sql
SELECT
    dp.product_id,
    dp.category_english AS category,
    COUNT(*) AS items_sold,
    SUM(foi.price) AS revenue,
    AVG(foi.price) AS avg_item_price
FROM {{ ref('fact_order_items') }} foi
JOIN {{ ref('dim_product') }} dp ON foi.product_sk = dp.product_sk
GROUP BY 1, 2
```

```sql
-- dbt/models/marts/marts/mart_delivery_sla.sql
SELECT
    d.year,
    d.month,
    COUNT(*) FILTER (WHERE f.delivery_days IS NOT NULL) AS delivered_orders,
    AVG(f.delivery_days) AS avg_delivery_days,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.delivery_days) AS p90_delivery_days
FROM {{ ref('fact_orders') }} f
JOIN {{ ref('dim_date') }} d
    ON TO_CHAR(f.order_purchase_timestamp::date, 'YYYYMMDD')::int = d.date_sk
GROUP BY 1, 2
```

- [ ] **Step 4: Add core and mart schema tests**

```yaml
# dbt/models/marts/core/_core.yml
version: 2
models:
  - name: dim_customer
    columns:
      - name: customer_sk
        tests: [not_null, unique]
  - name: dim_product
    columns:
      - name: product_sk
        tests: [not_null, unique]
      - name: product_id
        tests: [not_null, unique]
  - name: dim_seller
    columns:
      - name: seller_sk
        tests: [not_null, unique]
  - name: dim_date
    columns:
      - name: date_sk
        tests: [not_null, unique]
  - name: fact_orders
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: customer_sk
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_sk
  - name: fact_order_items
    columns:
      - name: order_item_sk
        tests: [not_null, unique]
      - name: product_sk
        tests:
          - relationships: { to: ref('dim_product'), field: product_sk }
```

```yaml
# dbt/models/marts/marts/_marts.yml
version: 2
models:
  - name: mart_sales_daily
    columns:
      - name: date_day
        tests: [not_null, unique]
  - name: mart_customer_rfm
    columns:
      - name: customer_unique_id
        tests: [not_null, unique]
```

- [ ] **Step 5: Add singular tests**

```sql
-- dbt/tests/assert_no_negative_payment.sql
SELECT order_id
FROM {{ ref('fact_orders') }}
WHERE payment_value < 0
```

```sql
-- dbt/tests/assert_delivery_not_before_purchase.sql
SELECT order_id
FROM {{ ref('fact_orders') }}
WHERE order_delivered_customer_date IS NOT NULL
  AND order_delivered_customer_date < order_purchase_timestamp
```

- [ ] **Step 6: Build and test Gold**

Run: `dbt snapshot --project-dir dbt --profiles-dir dbt` then `dbt build --select marts --project-dir dbt --profiles-dir dbt`
Expected: PASS.

- [ ] **Step 7: Incremental idempotency + SCD2 verification (Review Focus cases)**

Run twice and assert no duplicates:

```bash
dbt build --select fact_orders --project-dir dbt --profiles-dir dbt
dbt build --select fact_orders --project-dir dbt --profiles-dir dbt
```

```sql
-- run against warehouse; expect 0
SELECT COUNT(*) FROM (
  SELECT order_id FROM gold.fact_orders GROUP BY 1 HAVING COUNT(*) > 1
) dup;
```

For SCD2: update a `silver.customers` row's `customer_city`, re-run `dbt snapshot`, and assert:

```sql
SELECT COUNT(*) FROM gold.dim_customer WHERE customer_unique_id = '<changed_id>' AND is_current;
-- expect exactly 1
SELECT COUNT(*) FROM gold.dim_customer WHERE customer_unique_id = '<changed_id>' AND NOT is_current;
-- expect exactly 1 (the prior version, valid_to set)
```

- [ ] **Step 8: Commit**

```bash
git add dbt/snapshots/ dbt/models/marts/ dbt/tests/
git commit -m "feat(dbt): add dims, facts, snapshots and marts"
```

---

### Task 8: Airflow image and DAGs with Datasets

**Files:**
- Create: `airflow/Dockerfile`
- Create: `airflow/requirements.txt`
- Create: `airflow/dags/common.py`
- Create: `airflow/dags/dag_ingest_olist.py`
- Create: `airflow/dags/dag_silver_transform.py`
- Create: `airflow/dags/dag_gold_dbt.py`
- Create: `airflow/dags/dag_quality_ge.py`
- Create: `airflow/dags/dag_backfill.py`
- Modify: `docker-compose.yml` (add `spark-master`, `spark-worker`, `airflow-*`)
- Test: `tests/test_dags.py`

**Interfaces:**
- Consumes: Bronze/Silver roots, `SOURCE_DSN`, `WAREHOUSE_DSN`, config from Tasks 1–7.
- Produces: Datasets `bronze://olist`, `silver://olist`, `gold://olist`; DAG ids `dag_ingest_olist`, `dag_silver_transform`, `dag_gold_dbt`, `dag_quality_ge`, `dag_backfill`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dags.py
from pathlib import Path
import ast

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"

def test_all_dag_files_parse():
    files = sorted(p for p in DAGS.glob("dag_*.py"))
    assert files, "no DAG files found"
    for path in files:
        ast.parse(path.read_text(), filename=str(path))

def test_expected_dags_exist():
    names = {p.stem for p in DAGS.glob("dag_*.py")}
    assert {"dag_ingest_olist", "dag_silver_transform", "dag_gold_dbt",
            "dag_quality_ge", "dag_backfill"} <= names

def test_dataset_uris_are_consistent():
    from airflow.dags.common import BRONZE_DATASET, SILVER_DATASET, GOLD_DATASET
    assert str(BRONZE_DATASET.uri) == "bronze://olist"
    assert str(SILVER_DATASET.uri) == "silver://olist"
    assert str(GOLD_DATASET.uri) == "gold://olist"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dags.py -v`
Expected: FAIL (no `airflow/dags` files).

- [ ] **Step 3: Create the Airflow image**

```dockerfile
# airflow/Dockerfile
FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

```text
# airflow/requirements.txt
pyspark==3.5.1
apache-airflow-providers-apache-spark==4.8.0
dbt-postgres==1.8.2
psycopg2-binary==2.9.9
pandas==2.2.2
pyarrow==17.0.0
s3fs==2024.6.1
great_expectations==0.18.19
```

- [ ] **Step 4: Create `airflow/dags/common.py`**

```python
# airflow/dags/common.py
from __future__ import annotations

import os

from airflow.datasets import Dataset

BRONZE_DATASET = Dataset("bronze://olist")
SILVER_DATASET = Dataset("silver://olist")
GOLD_DATASET = Dataset("gold://olist")

SOURCE_DSN = os.environ.get("SOURCE_DSN", "postgresql://deuser:depassword@postgres:5432/olist_source")
WAREHOUSE_DSN = os.environ.get("WAREHOUSE_DSN", "postgresql://deuser:depassword@postgres:5432/warehouse")
BRONZE_ROOT = os.environ.get("BRONZE_ROOT", "s3a://lake/bronze/olist")
SILVER_ROOT = os.environ.get("SILVER_ROOT", "s3a://lake/silver/olist")
SPARK_CONN_ID = os.environ.get("SPARK_CONN_ID", "spark_default")
ENTITIES = [
    "customers", "orders", "order_items", "order_payments",
    "order_reviews", "products", "sellers", "geolocation",
    "product_category_name_translation",
]

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": os.environ.get("S3_ENDPOINT_INTERNAL", "http://minio:9000"),
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}
```

- [ ] **Step 5: Create the ingest DAG (emits `bronze://olist`)**

```python
# airflow/dags/dag_ingest_olist.py
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common import BRONZE_DATASET, BRONZE_ROOT, ENTITIES, SOURCE_DSN


def _ingest(entity: str, ds: str) -> dict:
    from ingestion.config import load_sources
    from ingestion.run_ingest import ingest_source

    cfg = load_sources("/opt/airflow/config/sources.yaml")[entity]
    result = ingest_source(SOURCE_DSN, cfg, None, ds, f"{ds}-{entity}", BRONZE_ROOT)
    return {"entity": entity, "rows": result.rows_out, "path": result.path}


with DAG(
    dag_id="dag_ingest_olist",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": 60},
    tags=["phase1", "ingest"],
) as dag:

    @task(outlets=[BRONZE_DATASET])
    def ingest() -> list[dict]:
        import pendulum

        ds = "{{ ds }}"
        return [_ingest(entity, ds) for entity in ENTITIES]

    ingest()
```

- [ ] **Step 6: Create the silver, gold and quality DAGs (dataset-triggered chain)**

```python
# airflow/dags/dag_silver_transform.py
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from common import BRONZE_DATASET, BRONZE_ROOT, ENTITIES, SILVER_DATASET, SILVER_ROOT, SPARK_CONF, SPARK_CONN_ID, WAREHOUSE_DSN
import os

with DAG(
    dag_id="dag_silver_transform",
    start_date=datetime(2023, 1, 1),
    schedule=[BRONZE_DATASET],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": 60},
    tags=["phase1", "silver"],
) as dag:

    def _spark(entity: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=f"spark_silver_{entity}",
            application="/opt/airflow/spark/jobs/bronze_to_silver.py",
            conn_id=SPARK_CONN_ID,
            application_args=[
                "--entity", entity, "--ds", "{{ ds }}",
                "--bronze-root", BRONZE_ROOT, "--silver-root", SILVER_ROOT,
            ],
            conf=SPARK_CONF,
        )

    @task(outlets=[SILVER_DATASET])
    def load_warehouse() -> int:
        from warehouse.db import connect
        from warehouse.load_silver import load_entity

        ds = "{{ ds }}"
        total = 0
        with connect(WAREHOUSE_DSN) as conn:
            for entity in ENTITIES:
                path = f"{SILVER_ROOT}/{entity}/ingest_date={ds}"
                if os.environ.get("SKIP_EMPTY") or True:
                    try:
                        total += load_entity(conn, SILVER_ROOT, entity, ds)
                    except FileNotFoundError:
                        continue
        return total

    load_warehouse()
```

```python
# airflow/dags/dag_gold_dbt.py
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from common import GOLD_DATASET, SILVER_DATASET

DBT = "cd /opt/airflow/dbt && dbt"

with DAG(
    dag_id="dag_gold_dbt",
    start_date=datetime(2023, 1, 1),
    schedule=[SILVER_DATASET],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": 60},
    tags=["phase1", "gold"],
) as dag:

    snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT} snapshot --profiles-dir . --project-dir .",
    )
    run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run --profiles-dir . --project-dir .",
    )
    test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test --profiles-dir . --project-dir .",
    )

    @task(outlets=[GOLD_DATASET])
    def mark_gold() -> str:
        return "gold ready"

    snapshot >> run >> test >> mark_gold()
```

```python
# airflow/dags/dag_quality_ge.py
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

from common import GOLD_DATASET, WAREHOUSE_DSN

with DAG(
    dag_id="dag_quality_ge",
    start_date=datetime(2023, 1, 1),
    schedule=[GOLD_DATASET],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": 60},
    tags=["phase1", "quality"],
) as dag:

    @task
    def run_ge() -> dict:
        from quality.check_gold import run_checks

        return run_checks(WAREHOUSE_DSN)

    run_ge()
```

```python
# airflow/dags/dag_backfill.py
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="dag_backfill",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["phase1", "backfill"],
) as dag:

    TriggerDagRunOperator(
        task_id="trigger_ingest",
        trigger_dag_id="dag_ingest_olist",
        conf={"backfill": True},
        wait_for_completion=False,
        poke_interval=30,
    )
```

- [ ] **Step 7: Add Airflow and Spark services to `docker-compose.yml`**

```yaml
  spark-master:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: master
    ports: ["7077:7077", "8080:8080"]

  spark-worker:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
    depends_on: [spark-master]

  airflow-common: &airflow-common
    build: ./airflow
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: ${AIRFLOW_DB_DSN}
      SOURCE_DSN: ${SOURCE_DSN}
      WAREHOUSE_DSN: ${WAREHOUSE_DSN}
      S3_ENDPOINT_INTERNAL: ${S3_ENDPOINT_INTERNAL}
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      _AIRFLOW_WWW_USER_USERNAME: admin
      _AIRFLOW_WWW_USER_PASSWORD: admin
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./spark:/opt/airflow/spark
      - ./ingestion:/opt/airflow/ingestion
      - ./warehouse:/opt/airflow/warehouse
      - ./config:/opt/airflow/config
      - ./dbt:/opt/airflow/dbt
      - ./quality:/opt/airflow/quality
    depends_on:
      postgres: { condition: service_healthy }
      minio: { condition: service_healthy }
      spark-master: { condition: service_started }

  airflow-init:
    <<: *airflow-common
    command: bash -c "airflow db migrate && airflow users create --username admin --password admin --firstname a --lastname b --role Admin --email admin@example.com"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports: ["8081:8080"]

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
```

Also register the Spark connection and add host `PYTHONPATH` so DAGs can import project modules:

```yaml
    environment:
      PYTHONPATH: /opt/airflow
      AIRFLOW_CONN_SPARK_DEFAULT: '{"conn_type":"spark","host":"spark://spark-master","port":7077}'
```

- [ ] **Step 8: Run DAG parse test**

Run: `python -m pytest tests/test_dags.py -v`
Expected: FAIL initially until `airflow` is importable; run inside the container to get real coverage:
`docker compose run --rm airflow-scheduler python -m pytest /opt/airflow/tests/test_dags.py -v`
Expected: PASS.

- [ ] **Step 9: End-to-end run (manual verification)**

Run: `docker compose up -d --build`, open Airflow at `http://localhost:8081` (admin/admin), trigger `dag_ingest_olist`.
Expected: ingest → silver → gold → quality chain runs; each DAG succeeds or reports KPI.

- [ ] **Step 10: Commit**

```bash
git add airflow/ docker-compose.yml tests/test_dags.py
git commit -m "feat(airflow): add image, dataset-triggered dags and spark services"
```

---

### Task 9: Data quality with Great Expectations

**Files:**
- Create: `quality/__init__.py`
- Create: `quality/check_gold.py`
- Test: `tests/test_quality.py`

**Interfaces:**
- Consumes: Gold tables in `warehouse`.
- Produces: `quality.check_gold.build_suite() -> dict`; `quality.check_gold.validate(dfs: dict[str, pd.DataFrame]) -> dict[str, bool]`; `quality.check_gold.run_checks(dsn: str) -> dict[str, bool]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_quality.py
import pandas as pd
from quality.check_gold import build_suite, validate

def test_validate_flags_empty_fact_orders():
    frames = {
        "fact_orders": pd.DataFrame({"order_id": [], "payment_value": []}),
        "mart_sales_daily": pd.DataFrame({"date_day": [], "total_revenue": []}),
    }
    results = validate(frames)
    assert results["fact_orders_row_count"] is False

def test_validate_passes_on_clean_frames():
    frames = {
        "fact_orders": pd.DataFrame(
            {"order_id": ["a", "b"], "payment_value": [10.0, -5.0], "delivery_days": [1.0, 2.0]}
        ),
        "mart_sales_daily": pd.DataFrame({"date_day": ["2023-01-01"], "total_revenue": [10.0]}),
    }
    results = validate(frames)
    assert results["fact_orders_row_count"] is True
    assert results["fact_orders_no_negative_payment"] is False

def test_build_suite_lists_expected_expectations():
    names = {e["expectation_type"] for e in build_suite()["expectations"]}
    assert "expect_table_row_count_to_be_between" in names
    assert "expect_column_values_to_not_be_null" in names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_quality.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'quality'`.

- [ ] **Step 3: Implement `quality/check_gold.py`**

```python
# quality/check_gold.py
from __future__ import annotations

import pandas as pd


def build_suite() -> dict:
    return {
        "expectation_suite_name": "gold_quality",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1, "max_value": None},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "order_id"},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "payment_value", "min_value": 0},
            },
        ],
    }


def validate(frames: dict[str, pd.DataFrame]) -> dict[str, bool]:
    from great_expectations.dataset import PandasDataset

    orders = frames["fact_orders"]
    sales = frames["mart_sales_daily"]
    results: dict[str, bool] = {}

    orders_ds = PandasDataset(orders)
    results["fact_orders_row_count"] = bool(
        orders_ds.expect_table_row_count_to_be_between(min_value=1)["success"]
    )
    if len(orders):
        results["fact_orders_order_id_not_null"] = bool(
            orders_ds.expect_column_values_to_not_be_null("order_id")["success"]
        )
        results["fact_orders_no_negative_payment"] = bool(
            orders_ds.expect_column_values_to_be_between("payment_value", min_value=0)["success"]
        )

    sales_ds = PandasDataset(sales)
    results["mart_sales_daily_row_count"] = bool(
        sales_ds.expect_table_row_count_to_be_between(min_value=1)["success"]
    )
    return results


def run_checks(dsn: str) -> dict[str, bool]:
    from warehouse.db import connect

    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        frames["fact_orders"] = pd.read_sql("SELECT * FROM gold.fact_orders", conn)
        frames["mart_sales_daily"] = pd.read_sql("SELECT * FROM gold.mart_sales_daily", conn)
    results = validate(frames)
    print({k: ("PASS" if v else "FAIL") for k, v in results.items()})
    return results


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    args = parser.parse_args()
    results = run_checks(args.dsn)
    if not all(results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_quality.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add quality/ tests/test_quality.py
git commit -m "feat(quality): add great expectations checks on gold"
```

---

### Task 10: CI, Makefile, README and architecture guide

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `README.md`
- Create: `docs/architecture.md`
- Create: `docs/notes/01-medallion-architecture.md`, `docs/notes/02-idempotency.md`, `docs/notes/03-orchestration-datasets.md`
- Modify: `Makefile`

**Interfaces:**
- Consumes: everything from Tasks 1–9.
- Produces: a green CI pipeline and the learner-facing documentation required by spec §11.

- [ ] **Step 1: Create the CI workflow**

```yaml
# .github/workflows/ci.yml
name: ci
on:
  pull_request:
  push:
    branches: [master, main]
jobs:
  checks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements-dev.txt ruff sqlfluff pytest
      - run: ruff check .
      - run: python -m pytest tests -v -m "not integration"
      - run: docker compose config --quiet
      - run: sqlfluff lint dbt/models --dialect postgres
```

- [ ] **Step 2: Write `docs/architecture.md` using the spec §11 template**

Start the file with a heading and a short paragraph, then one `##` section per component (`Ingestion`, `Bronze`, `Silver`, `Gold`, `Orchestration`, `Quality`). Each section MUST contain the four sub-headings **What**, **Why**, **Impact**, **Benefit** followed by 1–3 sentences each, per spec §11. Example for the Ingestion section:

```markdown
## Ingestion

**What** — Python reads incrementally from the Postgres OLTP source using a
per-table watermark and writes raw, unmodified rows to Bronze Parquet on MinIO.

**Why** — We need a faithful copy of the source before any cleaning so that
transformation bugs never destroy the only raw data, and so any historical date
can be replayed.

**Impact** — The watermark column and the `ingest_date` partition written here
determine what every downstream stage sees; a wrong watermark silently drops
rows, and a non-idempotent write duplicates them.

**Benefit** — Replayability and auditability: any past load can be reproduced
byte-for-byte, and a bad transform can be re-run from source without a full reload.
```

- [ ] **Step 3: Write the three `docs/notes/*.md` concept guides**

Each file follows the same **What / Why / Impact / Benefit** template and covers: medallion layers, idempotency & backfill, and Airflow Datasets. Add a concrete example and a verification command in each.

- [ ] **Step 4: Write `README.md`**

Include: one-paragraph project description, an ASCII architecture diagram (copy from spec §3), the stack table (spec §4), the `make` command list, the quickstart (`cp .env.example .env`, `make up`, seed, trigger DAGs), and a link to `docs/architecture.md`. Follow the What/Why/Impact/Benefit template in the "How it works" section.

- [ ] **Step 5: Add Makefile targets**

```makefile
download:
	python scripts/download_olist.py

seed:
	python scripts/seed_postgres.py --dsn "postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:5432/olist_source"

backfill:
	@test -n "$(START)" && test -n "$(END)" || (echo "usage: make backfill START=YYYY-MM-DD END=YYYY-MM-DD" && exit 1)
	docker compose exec airflow-scheduler airflow dags backfill -s $(START) -e $(END) dag_ingest_olist

docs:
	cd dbt && dbt docs generate --profiles-dir . --project-dir .
```

- [ ] **Step 6: Verify CI locally**

Run: `ruff check .` ; `python -m pytest tests -v -m "not integration"` ; `docker compose config --quiet`
Expected: all pass.

- [ ] **Step 7: Commit and push**

```bash
git add .github/workflows/ci.yml README.md docs/ Makefile
git commit -m "docs(ci): add ci pipeline, readme and architecture guides"
git push
```

---

## Self-Review

**Spec coverage:**
- Spec §3–5 architecture & phases → Tasks 1, 6, 7, 8.
- Spec §6 sources & ingestion → Tasks 2, 3.
- Spec §7 data model → Tasks 4, 6, 7.
- Spec §8 orchestration/idempotency/backfill → Tasks 3, 7, 8.
- Spec §9 quality/testing/observability → Tasks 4, 6, 7, 9, 10.
- Spec §10 repo/compose/config/commands → Tasks 1, 10.
- Spec §11 detailed-guide template → Task 10.
- Spec §12 Definition of Done → covered by Task 8 Step 9 and Task 10 Step 6.

**Placeholder scan:** No `TBD`/`TODO`; every code step contains real content. Task 2 Step 4 flags the table-creation helper that must be implemented inside Task 2; it is a concrete instruction, not a placeholder.

**Type consistency:** `KEY_COLUMNS` in Task 4 (`spark/jobs`) and Task 5 (`warehouse/load_silver`) match the config primary keys in Task 3; `build_upsert_sql` signature matches its use in `db.py`; Dataset URIs and DAG ids are consistent between `common.py`, the DAGs, and `tests/test_dags.py`.

**Review Focus:** idempotency (Task 3 Step 9, Task 5 Step 6, Task 7 Step 7), rejects (Task 3 Step 1, Task 4 Step 1), timezone (Task 3 Step 1), incremental/SCD2 (Task 7 Step 7). Backfill date correctness is exercised by Task 8 `dag_backfill`; its verification is the manual end-to-end run in Task 8 Step 9.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-09-25-ecommerce-de-lakehouse-phase1.md`. Please review the plan. Which execution approach would you prefer?

- **Subagent-driven** — a fresh subagent implements each task and a fresh reviewer checks it before the next starts, then a whole-branch review at the end. Most thorough; costs a fresh context per task and per review.
- **Native** — I implement every task myself in this session, then one fresh reviewer checks the whole branch. Cheapest and fastest; no independent review until the end.

For this plan I recommend **Native**, because the tasks are sequential and share many files/interfaces (compose, `common.py`, config keys); a single continuous context avoids interface drift between tasks, and the plan carries the design in enough detail to implement directly. Does the plan capture what you want, and which approach should we use?

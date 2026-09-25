from __future__ import annotations

import argparse
import csv
from pathlib import Path

from scripts.download_olist import discover_tables, table_name_from_file, verify_manifest  # noqa: F401

def create_table_from_header(cur, schema: str, table: str, path: Path) -> None:
    with path.open("r", encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
    cols = ", ".join(f'"{c}" text' for c in header)
    cur.execute(f'DROP TABLE IF EXISTS {schema}."{table}"')
    cur.execute(f'CREATE TABLE {schema}."{table}" ({cols})')

def seed(dsn: str, data_dir: Path, schema: str = "public",
         manifest: Path | None = None) -> dict[str, int]:
    if manifest is not None:
        problems = verify_manifest(data_dir, manifest)
        if problems:
            raise ValueError(f"Dataset does not match manifest: {problems}")
    import psycopg2

    counts: dict[str, int] = {}
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table, path in discover_tables(data_dir).items():
            create_table_from_header(cur, schema, table, path)
            with path.open("r", encoding="utf-8") as fh:
                cur.copy_expert(
                    f'COPY {schema}."{table}" FROM STDIN WITH (FORMAT csv, HEADER true)',
                    fh,
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
    parser.add_argument("--manifest", type=Path, default=Path("data/checksums/olist.sha256"))
    args = parser.parse_args()
    counts = seed(args.dsn, args.data_dir, args.schema, args.manifest)
    for table, count in sorted(counts.items()):
        print(f"{table}: {count}")


if __name__ == "__main__":
    main()
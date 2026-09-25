from __future__ import annotations

import argparse
import hashlib
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
    return {table_name_from_file(p.name): p for p in sorted(data_dir.glob("*.csv"))}

def verify_manifest(data_dir: Path, manifest: Path) -> list[str]:
    if not manifest.exists():
        return []
    problems: list[str] = []
    for line in manifest.read_text().splitlines():
        if not line.strip():
            continue
        digest, name = line.split(maxsplit=1)
        path = data_dir / name.strip()
        if not path.exists():
            problems.append(f"missing:{name.strip()}")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            problems.append(f"changed:{name.strip()}")
    return problems

def write_manifest(data_dir: Path, manifest: Path) -> None:
    lines = []
    for path in sorted(data_dir.glob("*.csv")):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.name}")
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text("\n".join(lines) + "\n")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dest", type=Path, default=Path("data/raw/olist"))
    parser.add_argument("--manifest", type=Path, default=Path("data/checksums/olist.sha256"))
    args = parser.parse_args()
    write_manifest(args.dest, args.manifest)
    print(f"manifest written for {len(discover_tables(args.dest))} tables")


if __name__ == "__main__":
    main()
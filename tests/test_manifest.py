import pytest
from pathlib import Path

from scripts.download_olist import verify_manifest, write_manifest
from scripts.seed_postgres import seed

def test_verify_manifest_ok(tmp_path: Path):
    (tmp_path / "olist_orders_dataset.csv").write_text("order_id\n1\n")
    write_manifest(tmp_path, tmp_path / "m.sha256")
    assert verify_manifest(tmp_path, tmp_path / "m.sha256") == []

def test_verify_manifest_detects_missing_file(tmp_path: Path):
    f = tmp_path / "olist_orders_dataset.csv"
    f.write_text("order_id\n1\n")
    write_manifest(tmp_path, tmp_path / "m.sha256")
    f.unlink()
    assert verify_manifest(tmp_path, tmp_path / "m.sha256") == \
        ["missing:olist_orders_dataset.csv"]

def test_verify_manifest_detects_changed_file(tmp_path: Path):
    f = tmp_path / "olist_orders_dataset.csv"
    f.write_text("order_id\n1\n")
    write_manifest(tmp_path, tmp_path / "m.sha256")
    f.write_text("order_id\n2\n")
    assert verify_manifest(tmp_path, tmp_path / "m.sha256") == \
        ["changed:olist_orders_dataset.csv"]

def test_seed_aborts_before_connecting_on_manifest_mismatch(tmp_path: Path):
    (tmp_path / "olist_orders_dataset.csv").write_text("order_id\n1\n")
    manifest = tmp_path / "m.sha256"
    manifest.write_text("deadbeef  olist_customers_dataset.csv\n")
    with pytest.raises(ValueError):
        seed("postgresql://invalid:invalid@localhost:0/none", tmp_path, manifest=manifest)
from pathlib import Path
from scripts.download_olist import discover_tables

def test_discover_tables_maps_olist_files(tmp_path: Path):
    for name in ["olist_orders_dataset.csv",
                 "olist_order_items_dataset.csv",
                 "product_category_name_translation.csv"]:
        (tmp_path / name).write_text("id\n1\n")
    tables = discover_tables(tmp_path)
    assert tables["orders"].name == "olist_orders_dataset.csv"
    assert tables["order_items"].name == "olist_order_items_dataset.csv"
    assert tables["product_category_name_translation"].name == "product_category_name_translation.csv"
from scripts.seed_postgres import table_name_from_file

def test_table_name_from_file_strips_prefix_and_suffix():
    assert table_name_from_file("olist_order_payments_dataset.csv") == "order_payments"
    assert table_name_from_file("olist_customers_dataset.csv") == "customers"
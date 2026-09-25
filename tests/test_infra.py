from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]

def test_compose_has_required_services():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    assert {"postgres", "garage"} <= set(services)
    assert "healthcheck" in services["postgres"]
    assert "healthcheck" in services["garage"]

def test_postgres_init_creates_three_databases():
    script = (ROOT / "docker" / "postgres" / "init" / "00-init-databases.sh").read_text()
    for db in ("olist_source", "airflow", "warehouse"):
        assert db in script

def test_env_example_lists_required_vars():
    text = (ROOT / ".env.example").read_text()
    for key in ("POSTGRES_USER", "POSTGRES_PASSWORD", "SOURCE_DSN",
                "AIRFLOW_DB_DSN", "WAREHOUSE_DSN", "GARAGE_RPC_SECRET",
                "GARAGE_ACCESS_KEY", "GARAGE_SECRET_KEY", "LAKE_BUCKET"):
        assert key in text


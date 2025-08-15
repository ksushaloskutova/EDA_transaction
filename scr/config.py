"""
Configuration loader for dataset downloader.
Reads environment variables (supports .env via python-dotenv if present).
"""
from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# Load .env if exists (safe for missing file)
load_dotenv(override=False)

@dataclass(frozen=True)
class Settings:
    # Root data directory
    data_dir: Path = Path(os.environ.get("DATA_DIR", "data")).resolve()

    # Dataset URLs from env
    tx_url: str | None = os.environ.get("TX_URL")            # transaction_fraud_data
    fx_url: str | None = os.environ.get("FX_URL")            # historical_currency_exchange
    readme_url: str | None = os.environ.get("README_URL")    # optional

    # Networking / safety
    timeout: int = int(os.environ.get("TIMEOUT", "60"))
    retries: int = int(os.environ.get("RETRIES", "3"))
    verify_ssl: bool = os.environ.get("VERIFY_SSL", "true").lower() in {"1","true","yes","y"}

    # Filenames (can be overridden via env)
    tx_filename: str = os.environ.get("TX_FILENAME", "transaction_fraud_data.csv")
    fx_filename: str = os.environ.get("FX_FILENAME", "historical_currency_exchange.csv")
    readme_filename: str = os.environ.get("README_FILENAME", "README.txt")

def get_settings() -> Settings:
    s = Settings()
    # Ensure data dir exists
    s.data_dir.mkdir(parents=True, exist_ok=True)
    return s

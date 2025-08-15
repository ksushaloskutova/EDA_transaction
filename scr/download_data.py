from __future__ import annotations
import io
import time
import logging
from pathlib import Path
import requests
import pandas as pd

from .config import get_settings  # твой конфиг

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("downloader")

def _download_bytes(url: str, timeout: int, verify_ssl: bool, retries: int) -> bytes:
    last_exc = None
    for i in range(1, retries + 1):
        try:
            with requests.get(url, timeout=timeout, verify=verify_ssl, stream=True) as r:
                r.raise_for_status()
                return r.content
        except Exception as e:
            last_exc = e
            log.warning("Попытка %d/%d для %s не удалась: %s", i, retries, url, e)
            time.sleep(min(2**i, 10))
    raise RuntimeError(f"Не удалось скачать после {retries} попыток: {url}") from last_exc

def _looks_like_parquet(b: bytes) -> bool:
    return b[:4] == b"PAR1" or b[-4:] == b"PAR1"

def _bytes_to_df(b: bytes) -> pd.DataFrame:
    if _looks_like_parquet(b):
        return pd.read_parquet(io.BytesIO(b), engine="pyarrow")
    return pd.read_csv(io.BytesIO(b), engine="python")

def download_to_csv(url: str, out_path: Path, timeout: int, verify_ssl: bool, retries: int):
    log.info("Скачиваю %s -> %s", url, out_path)
    b = _download_bytes(url, timeout, verify_ssl, retries)
    df = _bytes_to_df(b)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    log.info("Сохранено %s (строк: %s, колонок: %s)", out_path, len(df), df.shape[1])

if __name__ == "__main__":
    s = get_settings()
    download_to_csv(s.tx_url, s.data_dir / "transaction_fraud_data.csv", s.timeout, s.verify_ssl, s.retries)
    download_to_csv(s.fx_url, s.data_dir / "historical_currency_exchange.csv", s.timeout, s.verify_ssl, s.retries)

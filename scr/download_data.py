from __future__ import annotations
import io
import time
from pathlib import Path
import requests
import pandas as pd

from .config import get_settings
from .logger import get_logger

logger = get_logger("downloader")

def _safe_filename(default_name: str, url: str | None) -> str:
    if not url:
        return default_name
    name = Path(url.split("?")[0]).name
    return name or default_name

def _detect_content_type(url: str, timeout: int, verify_ssl: bool) -> str:
    try:
        resp = requests.head(url, timeout=timeout, verify=verify_ssl, allow_redirects=True)
        ct = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
        return ct
    except Exception:
        return ""

def _download(url: str, timeout: int, verify_ssl: bool, retries: int = 3) -> bytes:
    last_exc = None
    for i in range(1, retries + 1):
        try:
            with requests.get(url, timeout=timeout, verify=verify_ssl, stream=True) as r:
                r.raise_for_status()
                return r.content
        except Exception as e:
            last_exc = e
            logger.warning("Attempt %d/%d failed for %s: %s", i, retries, url, e)
            time.sleep(min(2**i, 10))
    raise RuntimeError(f"Failed to download after {retries} attempts: {url}") from last_exc

def _to_csv_bytes(data: bytes, content_type: str, url_name: str) -> tuple[bytes, str]:
    """
    Convert various inputs (Excel, CSV, text) to CSV bytes.
    Returns (csv_bytes, suggested_basename)
    """
    url_lower = url_name.lower()

    # Prefer Excel path if indicated
    if ("excel" in content_type) or url_lower.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(data))
        return df.to_csv(index=False).encode("utf-8"), "converted.csv"

    # CSV / text
    if ("csv" in content_type) or ("text" in content_type) or url_lower.endswith((".csv", ".txt", ".tsv")):
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(io.BytesIO(data), sep=sep, engine="python")
                if df.shape[1] >= 1:
                    return df.to_csv(index=False).encode("utf-8"), "converted.csv"
            except Exception:
                continue
        # Fallback single-column
        try:
            df = pd.read_csv(io.BytesIO(data), header=None)
            return df.to_csv(index=False).encode("utf-8"), "converted.csv"
        except Exception:
            pass

    # Last resort: try Excel parse
    try:
        df = pd.read_excel(io.BytesIO(data))
        return df.to_csv(index=False).encode("utf-8"), "converted.csv"
    except Exception:
        # Give up: return original
        return data, "raw.bin"

def download_all() -> None:
    s = get_settings()
    logger.info("Data directory: %s", s.data_dir)

    tasks = [
        ("transaction_fraud_data", s.tx_url, s.tx_filename),
        ("historical_currency_exchange", s.fx_url, s.fx_filename),
        ("readme", s.readme_url, s.readme_filename),
    ]

    for key, url, target_name in tasks:
        if not url:
            logger.info("Skip %s: URL not provided in env", key)
            continue
        try:
            logger.info("Downloading %s from %s", key, url)
            raw = _download(url, timeout=s.timeout, verify_ssl=s.verify_ssl, retries=s.retries)
            ct = _detect_content_type(url, timeout=s.timeout, verify_ssl=s.verify_ssl)
            csv_bytes, suggestion = _to_csv_bytes(raw, ct, url)

            out_path = s.data_dir / target_name
            if suggestion != "converted.csv" and not target_name.lower().endswith(".csv"):
                out_path = s.data_dir / _safe_filename(target_name, url)

            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(csv_bytes)
            logger.info("Saved %s (%s bytes) -> %s", key, len(csv_bytes), out_path)
        except Exception as e:
            logger.exception("Failed to process %s: %s", key, e)

if __name__ == "__main__":
    download_all()

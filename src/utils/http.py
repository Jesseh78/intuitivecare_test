from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass(frozen=True)
class HttpConfig:
    timeout_s: int = 60
    retries: int = 3
    backoff_s: float = 1.0
    user_agent: str = "intuitivecare-teste/1.0 (contact: candidate)"


def get_text(url: str, cfg: Optional[HttpConfig] = None) -> str:
    cfg = cfg or HttpConfig()
    headers = {"User-Agent": cfg.user_agent}

    last_err: Exception | None = None
    for attempt in range(1, cfg.retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=cfg.timeout_s)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(cfg.backoff_s * attempt)
    raise RuntimeError(f"GET failed after {cfg.retries} tries: {url}") from last_err


def download_file(url: str, out_path: str, cfg: Optional[HttpConfig] = None) -> None:
    cfg = cfg or HttpConfig()
    headers = {"User-Agent": cfg.user_agent}

    last_err: Exception | None = None
    for attempt in range(1, cfg.retries + 1):
        try:
            with requests.get(url, headers=headers, timeout=cfg.timeout_s, stream=True) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
            return
        except Exception as e:
            last_err = e
            time.sleep(cfg.backoff_s * attempt)
    raise RuntimeError(f"DOWNLOAD failed after {cfg.retries} tries: {url}") from last_err

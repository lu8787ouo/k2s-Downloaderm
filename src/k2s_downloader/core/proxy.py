from __future__ import annotations

import pathlib
import random
import time
from concurrent.futures import as_completed
from typing import Callable, List, Optional

import requests
from requests_futures.sessions import FuturesSession
from tqdm import tqdm

StatusCallback = Optional[Callable[[str], None]]

HTTPS_BATCH_SIZE = 50
HTTPS_TIMEOUT = 5.0
HTTPS_RETRIES = 1


def _batched(items: List[str], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _emit_status(callback: StatusCallback, message: str) -> None:
    if callback:
        callback(message)


def get_working_proxies(
    refresh: bool = False,
    *,
    status_callback: StatusCallback = None,
    max_candidates: int | None = None,
    recheck_cached: bool = False,
) -> List[Optional[str]]:
    """Return a list of working proxy endpoints.

    The first element is always ``None`` to represent a direct connection.
    Cached results are stored in ``proxies.txt`` in the current working directory.
    """

    cache_path = pathlib.Path("proxies.txt")
    cached_proxies: List[str] = []
    if cache_path.exists():
        cached_proxies = [line for line in cache_path.read_text().splitlines() if line]

    if not refresh and not recheck_cached and cached_proxies:
        return [None] + cached_proxies

    proxies: List[str] = []

    if recheck_cached and cached_proxies:
        _emit_status(status_callback, f"Revalidating {len(cached_proxies)} cached proxies ...")
        proxies.extend(cached_proxies)

    fetch_remote = False
    if recheck_cached:
        if not cached_proxies:
            _emit_status(status_callback, "No cached proxies to revalidate. Fetching fresh list ...")
            fetch_remote = True
    else:
        fetch_remote = refresh or not cached_proxies

    if fetch_remote:
        _emit_status(status_callback, "Fetching proxy candidates from proxyscrape.com ...")
        https_resp = requests.get(
            "https://api.proxyscrape.com/?request=getproxies&proxytype=https&timeout=10000&country=all&ssl=all&anonymity=all",
            timeout=30,
        )
        proxies.extend(filter(None, https_resp.text.splitlines()))

    # Remove duplicates while preserving order
    seen = set()
    deduped: List[str] = []
    for proxy in proxies:
        if proxy and proxy not in seen:
            deduped.append(proxy)
            seen.add(proxy)
    proxies = deduped

    if not proxies:
        _emit_status(status_callback, "No proxy candidates available.")
        if cached_proxies:
            return [None] + cached_proxies
        return [None]

    if max_candidates:
        proxies = proxies[:max_candidates]
        _emit_status(status_callback, f"Validating first {len(proxies)} proxies (limit {max_candidates}) ...")
    else:
        _emit_status(status_callback, f"Validating {len(proxies)} proxies ...")

    total_candidates = len(proxies)
    if total_candidates == 0:
        return [None]

    working_set: set[str] = set()
    https_progress = tqdm(total=total_candidates, desc="HTTPS check", unit="proxy") if status_callback is None else None

    remaining = list(proxies)
    attempt = 0
    validated_count = 0

    while remaining and attempt < HTTPS_RETRIES:
        attempt += 1
        next_remaining: List[str] = []
        if status_callback is not None:
            _emit_status(status_callback, f"HTTPS attempt {attempt}/{HTTPS_RETRIES} on {len(remaining)} proxies ...")

        for batch in _batched(remaining, HTTPS_BATCH_SIZE):
            session = FuturesSession(max_workers=min(len(batch), HTTPS_BATCH_SIZE))
            futures = []
            for proxy in batch:
                future = session.get(
                    "https://api.myip.com",
                    proxies={"https": f"http://{proxy}"},
                    timeout=HTTPS_TIMEOUT,
                )
                future.proxy = proxy  # type: ignore[attr-defined]
                futures.append(future)

            batch_success = 0
            for future in as_completed(futures):
                proxy_label = getattr(future, "proxy", "<unknown>")
                try:
                    future.result()
                    if proxy_label not in working_set:
                        working_set.add(proxy_label)
                        batch_success += 1
                except KeyboardInterrupt:
                    raise
                except Exception:
                    if attempt < HTTPS_RETRIES:
                        next_remaining.append(proxy_label)
                finally:
                    validated_count += 1
                    if status_callback is not None and validated_count % 1000 == 0:
                        _emit_status(
                            status_callback,
                            f"HTTPS validated {validated_count} proxy attempts so far ...",
                        )

            session.close()
            if https_progress:
                https_progress.update(batch_success)
            sleep_time = random.uniform(0.2, 0.4)
            time.sleep(sleep_time)

        if status_callback is not None and next_remaining:
            _emit_status(status_callback, f"HTTPS pending {len(next_remaining)} proxies after attempt {attempt}.")
        remaining = next_remaining

    if https_progress:
        https_progress.close()

    working_proxies = list(working_set)
    if not working_proxies:
        _emit_status(status_callback, "No proxies passed HTTPS validation.")
        cache_path.write_text("")
        return [None]

    cache_path.write_text("\n".join(working_proxies))
    _emit_status(status_callback, f"Found {len(working_proxies)} working proxies.")

    return [None] + working_proxies

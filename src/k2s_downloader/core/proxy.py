from __future__ import annotations

import pathlib
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional

import requests
from tqdm import tqdm

StatusCallback = Optional[Callable[[str], None]]

# Public HTTP(S) proxy lists. Each one is plain text containing ip:port entries
# (optionally prefixed with a scheme); a source that fails to load is skipped.
PROXY_SOURCES = [
    "https://api.proxyscrape.com/?request=getproxies&proxytype=https&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=yes&anonymity=all",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text&timeout=10000",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt",
]

# Cloudflare's trace endpoint echoes the caller's IP ("ip=1.2.3.4"), which lets us
# confirm the proxy really changes our exit IP. (api.myip.com, used previously, is dead.)
IP_CHECK_URL = "https://www.cloudflare.com/cdn-cgi/trace"
# Cheap K2S API call: proves the proxy can actually reach K2S and isn't blocked by it.
K2S_CHECK_URL = "https://k2s.cc/api/v2/getFilesInfo"

CHECK_TIMEOUT = 5.0
K2S_TIMEOUT = 8.0
CHECK_WORKERS = 200

_PROXY_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{2,5})")


def _emit_status(callback: StatusCallback, message: str) -> None:
    if callback:
        callback(message)


def _fetch_candidates(status_callback: StatusCallback) -> List[str]:
    candidates: List[str] = []
    for url in PROXY_SOURCES:
        host = url.split("/")[2]
        try:
            text = requests.get(url, timeout=30).text
        except requests.RequestException as exc:
            _emit_status(status_callback, f"Proxy source {host} failed: {exc.__class__.__name__}")
            continue
        found = _PROXY_RE.findall(text)
        _emit_status(status_callback, f"Proxy source {host}: {len(found)} candidates")
        candidates.extend(found)
    return candidates


def _get_own_ip() -> Optional[str]:
    try:
        match = re.search(r"ip=(\S+)", requests.get(IP_CHECK_URL, timeout=10).text)
    except requests.RequestException:
        return None
    return match.group(1) if match else None


def _check_proxy(proxy: str, own_ip: Optional[str]) -> bool:
    prox = {"https": f"http://{proxy}"}
    try:
        trace = requests.get(IP_CHECK_URL, proxies=prox, timeout=CHECK_TIMEOUT)
        if not trace.ok or "ip=" not in trace.text:
            return False
        if own_ip and f"ip={own_ip}" in trace.text:
            return False  # transparent proxy: K2S would still see our own IP
        k2s = requests.post(K2S_CHECK_URL, json={"ids": []}, proxies=prox, timeout=K2S_TIMEOUT)
        return k2s.status_code < 500 and k2s.headers.get("content-type", "").startswith("application/json")
    except Exception:
        return False


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
        _emit_status(status_callback, f"Fetching proxy candidates from {len(PROXY_SOURCES)} sources ...")
        proxies.extend(_fetch_candidates(status_callback))

    # Remove duplicates while preserving order
    proxies = list(dict.fromkeys(p for p in proxies if p))

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

    own_ip = _get_own_ip()
    if own_ip is None:
        _emit_status(status_callback, "Could not determine own IP; transparent proxies won't be filtered.")

    working: List[str] = []
    progress = tqdm(total=len(proxies), desc="Proxy check", unit="proxy") if status_callback is None else None

    # One flat pool instead of fixed batches: a batch has to wait for its slowest
    # (timed-out) proxy, which made validating thousands of candidates very slow.
    with ThreadPoolExecutor(max_workers=CHECK_WORKERS) as pool:
        futures = {pool.submit(_check_proxy, proxy, own_ip): proxy for proxy in proxies}
        for checked, future in enumerate(as_completed(futures), start=1):
            if future.result():
                working.append(futures[future])
            if progress:
                progress.update(1)
                progress.set_postfix(working=len(working))
            elif checked % 500 == 0:
                _emit_status(status_callback, f"Checked {checked}/{len(proxies)} proxies, {len(working)} working ...")

    if progress:
        progress.close()

    if not working:
        _emit_status(status_callback, "No proxies passed validation.")
        cache_path.write_text("")
        return [None]

    cache_path.write_text("\n".join(working))
    _emit_status(status_callback, f"Found {len(working)} working proxies.")

    return [None] + working

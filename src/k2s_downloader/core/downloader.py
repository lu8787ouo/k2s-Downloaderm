from __future__ import annotations

import contextlib
import io
import json
import math
import os
import random
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

import requests
from shutil import which
from tqdm import tqdm

from . import k2s_client
from .proxy import get_working_proxies

MEDIA_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".mov",
    ".avi",
    ".flv",
    ".wmv",
    ".webm",
    ".mpg",
    ".mpeg",
    ".m4v",
    ".mp3",
    ".aac",
    ".wav",
    ".flac",
    ".ogg",
}

StatusCallback = Optional[Callable[[str], None]]
ProgressCallback = Optional[Callable[[int, int, int, int], None]]
CaptchaCallback = k2s_client.CaptchaCallback


class DownloadCancelled(RuntimeError):
    """Raised when a download is cancelled by the user."""


def _emit_status(callback: StatusCallback, message: str) -> None:
    if callback:
        callback(message)


def parse_size(size: str) -> int:
    units = {
        "B": 1,
        "KB": 2**10,
        "MB": 2**20,
        "GB": 2**30,
        "TB": 2**40,
        "": 1,
        "KIB": 10**3,
        "MIB": 10**6,
        "GIB": 10**9,
        "TIB": 10**12,
    }
    normalized = str(size).strip()
    match = re.match(r"^([\d\.]+)\s*([a-zA-Z]{0,3})$", normalized)
    if not match:
        raise ValueError(f"Invalid size value: {size}")
    number, unit = float(match.group(1)), match.group(2).upper()
    return int(number * units[unit])


def human_readable_bytes(num: int) -> str:
    units = ["bytes", "KB", "MB", "GB", "TB"]
    value = float(num)
    for unit in units:
        if value < 1024.0:
            return f"{value:3.3f} {unit}"
        value /= 1024.0
    return f"{value:3.3f} PB"


class Downloader:
    def __init__(
        self,
        *,
        tmp_dir: Path | str = "tmp",
        url_cache_path: Path | str = "urls.json",
        block_size: int = 32 * 1024,
        status_callback: StatusCallback = None,
        progress_callback: ProgressCallback = None,
        proxy_state_callback: Optional[Callable[[Sequence[Optional[str]], Sequence[int]], None]] = None,
        show_console_progress: bool = False,
    ) -> None:
        self.tmp_dir = Path(tmp_dir)
        self.url_cache_path = Path(url_cache_path)
        self.block_size = block_size
        self.status_callback = status_callback
        self.progress_callback = progress_callback
        self.show_console_progress = show_console_progress
        self.proxy_state_callback = proxy_state_callback

        self.stop_event = threading.Event()
        self._progress_lock = threading.Lock()
        self._proxy_state_lock = threading.Lock()
        self._bytes_downloaded = 0
        self._total_bytes = 0
        self._ranges_total = 0
        self._done_count = 0

        self.proxies: List[Optional[str]] = []
        self.proxy_locks: List[threading.Lock] = []
        self.working_proxy_indexes: List[int] = []
        self.url_locks: List[threading.Lock] = []
        self._active_proxy_indexes: set[int] = set()

    @staticmethod
    def extract_file_id(url: str) -> str:
        pattern = re.compile(r"https?://(k2s.cc|keep2share.cc)/file/(.*?)(\?|/|$)")
        match = pattern.search(url)
        if not match:
            raise ValueError("Invalid URL")
        return match.group(2)

    @staticmethod
    def _resolve_filename(user_filename: Optional[str], original_name: str) -> str:
        if not user_filename:
            return original_name
        path = Path(user_filename)
        if path.suffix:
            return user_filename
        suffix = "".join(Path(original_name).suffixes)
        return f"{user_filename}{suffix}" if suffix else user_filename

    def log(self, message: str) -> None:
        _emit_status(self.status_callback, message)

    def refresh_proxies(self, *, refresh: bool = False) -> None:
        self.proxies = get_working_proxies(refresh=refresh, status_callback=self.status_callback)
        self.proxy_locks = [threading.Lock() for _ in self.proxies]
        self.working_proxy_indexes = []
        self._active_proxy_indexes = set()
        self._notify_proxy_state()

    def should_check_media(self, filename: str) -> bool:
        return Path(filename).suffix.lower() in MEDIA_EXTENSIONS

    def _notify_proxy_state(self) -> None:
        if self.proxy_state_callback:
            with self._proxy_state_lock:
                proxies = list(self.proxies)
                active = sorted(self._active_proxy_indexes)
            self.proxy_state_callback(proxies, active)

    def cancel(self) -> None:
        self.stop_event.set()

    def download(
        self,
        url: str,
        *,
        filename: Optional[str] = None,
        threads: int = 20,
        split_size: int = 20 * 1024 * 1024,
        captcha_callback: Optional[CaptchaCallback] = None,
        ensure_media_check: bool = True,
    ) -> Path:
        
        if split_size < 5 * 1024 * 1024:
            raise ValueError("Split size must be at least 5M")

        if not self.proxies:
            self.refresh_proxies()

        file_id = self.extract_file_id(url)
        original_name = k2s_client.get_name(file_id)
        resolved_name = self._resolve_filename(filename, original_name)

        urls = []

        if self.url_cache_path.exists():
            try:
                self.url_cache_path.unlink()
            except OSError:
                pass

        urls = k2s_client.generate_download_urls(
            file_id,
            count=threads,
            proxies=self.proxies,
            captcha_callback=captcha_callback,
            status_callback=self.status_callback,
        )
        self._cache_urls(file_id, urls)

        self.stop_event.clear()
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        redownloaded = False
        current_split = split_size

        while True:
            result = self._download_once(urls, resolved_name, threads, current_split)
            if self.stop_event.is_set():
                raise DownloadCancelled("Download cancelled")

            if ensure_media_check and self.should_check_media(resolved_name) and which("ffmpeg"):
                if not self._check_media(Path(resolved_name)):
                    if not redownloaded:
                        self.log("Video appears corrupted. Retrying with a larger chunk size ...")
                        redownloaded = True
                        current_split *= 2
                        continue
                    self.log("Video is still corrupted after retry.")
            break

        return result

    def _load_cached_urls(self, file_id: str) -> List[str]:
        if not self.url_cache_path.exists():
            return []
        try:
            with self.url_cache_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            return data.get(file_id, [])
        except Exception:
            return []

    def _cache_urls(self, file_id: str, urls: Sequence[str]) -> None:
        if self.url_cache_path.exists():
            try:
                with self.url_cache_path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
            except Exception:
                data = {}
        else:
            data = {}

        data[file_id] = list(urls)
        with self.url_cache_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=4)

    def _download_once(
        self,
        urls: Sequence[str],
        filename: str,
        threads: int,
        bytes_per_split: int,
    ) -> Path:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
        }
        size_in_bytes = requests.head(urls[-1], allow_redirects=True, headers=headers).headers.get("Content-Length")
        if not size_in_bytes:
            raise RuntimeError("Size cannot be determined.")

        total_size = int(size_in_bytes)
        if total_size < 0:
            total_size = total_size + 2**32
        self._total_bytes = total_size
        self._bytes_downloaded = 0
        self._done_count = 0

        split_count = max(1, math.ceil(total_size / bytes_per_split))
        ranges = self._build_ranges(total_size, split_count)
        self._ranges_total = len(ranges)
        self.url_locks = [threading.Lock() for _ in range(threads)]

        progress_bar = tqdm(
            desc=f"[0/{len(ranges)}] Downloaded",
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            disable=not self.show_console_progress,
        )

        stop = False

        def report_progress(delta: int) -> None:
            if delta == 0:
                return
            with self._progress_lock:
                self._bytes_downloaded += delta
                downloaded = self._bytes_downloaded
                done = self._done_count
            if self.progress_callback:
                self.progress_callback(downloaded, self._total_bytes, done, self._ranges_total)
            if not progress_bar.disable:
                progress_bar.update(delta)

        def download_chunk(index: str, range_meta: Dict[str, object], thread_index: int) -> None:
            nonlocal stop

            if self.stop_event.is_set():
                stop = True
                return

            chunk_range = range_meta["range"]  # type: ignore[index]
            expected_bytes = int(range_meta["bytes"])  # type: ignore[arg-type]
            tmp_filename = self.tmp_dir / f"{filename}.part{str(index).zfill(len(str(split_count)))}"

            chunk_start_time = time.time()
            buffer = io.BytesIO()

            proxy_idx = 0
            added_to_active = False
            for known in self.working_proxy_indexes:
                if not self.proxy_locks[known].locked():
                    proxy_idx = known
                    break
                proxy_idx = random.randint(0, len(self.proxies) - 1)

            while self.proxy_locks[proxy_idx].locked():
                proxy_idx = random.randint(0, len(self.proxies) - 1)

            self.proxy_locks[proxy_idx].acquire()
            proxy_value = self.proxies[proxy_idx]
            prox = {"https": f"http://{proxy_value}"} if proxy_value else None
            added_to_active = True
            with self._proxy_state_lock:
                self._active_proxy_indexes.add(proxy_idx)
            self._notify_proxy_state()

            try:
                with contextlib.suppress(Exception):
                    response = requests.get(
                        urls[thread_index],
                        headers={"Range": f"bytes={chunk_range}", "User-Agent": headers["User-Agent"]},
                        stream=True,
                        proxies=prox,
                        timeout=20,
                    )

                    for data in response.iter_content(self.block_size):
                        if self.stop_event.is_set():
                            stop = True
                            break
                        if chunk_start_time + 20 < time.time():
                            break
                        chunk_start_time = time.time()
                        buffer.write(data)
                        report_progress(len(data))

                chunk_bytes = len(buffer.getvalue())
                if not math.isclose(chunk_bytes, expected_bytes, abs_tol=1):
                    report_progress(-chunk_bytes)
                    range_meta["inUse"] = False
                    if self.url_locks[thread_index].locked():
                        self.url_locks[thread_index].release()
                    return

                tmp_filename.write_bytes(buffer.getvalue())
                if proxy_idx not in self.working_proxy_indexes:
                    self.working_proxy_indexes.append(proxy_idx)
                    self._notify_proxy_state()

                range_meta["inUse"] = False
                range_meta["downloaded"] = True

                with self._progress_lock:
                    self._done_count += 1
                    done = self._done_count
                if not progress_bar.disable:
                    progress_bar.desc = f"[{done}/{len(ranges)}] Downloaded"
                if self.progress_callback:
                    self.progress_callback(self._bytes_downloaded, self._total_bytes, done, self._ranges_total)
            finally:
                if added_to_active:
                    with self._proxy_state_lock:
                        self._active_proxy_indexes.discard(proxy_idx)
                    self._notify_proxy_state()
                if self.url_locks[thread_index].locked():
                    self.url_locks[thread_index].release()
                if self.proxy_locks[proxy_idx].locked():
                    self.proxy_locks[proxy_idx].release()

        try:
            while self._done_count < len(ranges):
                if stop:
                    break
                for idx, meta in ranges.items():
                    if self.stop_event.is_set():
                        stop = True
                        break
                    if meta["inUse"] or meta["downloaded"]:
                        continue

                    part_path = self.tmp_dir / f"{filename}.part{str(idx).zfill(len(str(split_count)))}"
                    if part_path.exists():
                        existing = part_path.read_bytes()
                        if math.isclose(len(existing), meta["bytes"], abs_tol=1):
                            if not meta["downloaded"]:
                                report_progress(int(meta["bytes"]))
                                meta["downloaded"] = True
                                with self._progress_lock:
                                    self._done_count += 1
                                    done = self._done_count
                                if not progress_bar.disable:
                                    progress_bar.desc = f"[{done}/{len(ranges)}] Downloaded"
                                continue
                        else:
                            part_path.unlink()

                    for thread_index in range(threads):
                        if self.url_locks[thread_index].locked():
                            continue
                        self.url_locks[thread_index].acquire()
                        meta["inUse"] = True
                        threading.Thread(
                            target=download_chunk,
                            args=(idx, meta, thread_index),
                            daemon=True,
                        ).start()
                        break
                time.sleep(0.05)
        except KeyboardInterrupt:
            self.cancel()
            stop = True
        finally:
            for lock in self.url_locks:
                if lock.locked():
                    lock.release()
            for lock in self.proxy_locks:
                if lock.locked():
                    lock.release()
            self._notify_proxy_state()
            progress_bar.close()

        if stop:
            raise DownloadCancelled("Download cancelled")

        target_path = Path(filename)
        if target_path.exists():
            target_path.unlink()

        with target_path.open("wb") as handle:
            for idx in range(len(ranges)):
                part_path = self.tmp_dir / f"{filename}.part{str(idx).zfill(len(str(split_count)))}"
                with part_path.open("rb") as chunk:
                    handle.write(chunk.read())
                part_path.unlink()

        self.log(f"Finished writing {filename}")
        self.log(f"File Size: {human_readable_bytes(target_path.stat().st_size)}")
        return target_path

    @staticmethod
    def _build_ranges(total_value: int, split_count: int) -> Dict[str, Dict[str, object]]:
        range_dict: Dict[str, Dict[str, object]] = {}
        for i in range(split_count):
            start = int(round(1 + i * total_value / (split_count * 1.0), 0))
            end = int(round(1 + i * total_value / (split_count * 1.0) + total_value / (split_count * 1.0) - 1, 0))
            range_dict[str(i)] = {
                "inUse": False,
                "downloaded": False,
                "range": f"{start}-{end}",
                "bytes": end - start + 1,
            }
        first = range_dict["0"]
        _, end = str(first["range"]).split("-")
        first["range"] = f"0-{end}"
        first["bytes"] = int(first["bytes"]) + 1
        return range_dict

    @staticmethod
    def _check_media(video_path: Path) -> bool:
        command = [
            "ffmpeg",
            "-i",
            str(video_path),
            "-c",
            "copy",
            "-f",
            "null",
            os.devnull,
            "-v",
            "warning",
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return result.returncode == 0 and not result.stdout


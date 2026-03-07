from __future__ import annotations

import threading
import time
from typing import Optional

from PySide6.QtCore import QThread, Signal

from ..core.downloader import DownloadCancelled, Downloader
from ..core.proxy import get_working_proxies


class DownloadWorker(QThread):  # pragma: no cover - Qt integration
    progress = Signal(object, object, int, int)
    status = Signal(str)
    error = Signal(str)
    succeeded = Signal(str)
    proxy_state = Signal(list, list)
    captcha_required = Signal(bytes, str, str)
    stopped = Signal()

    def __init__(
        self,
        url: str,
        *,
        filename: Optional[str],
        threads: int,
        split_size: int,
        ensure_media_check: bool,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.url = url
        self.filename = filename
        self.threads = threads
        self.split_size = split_size
        self.ensure_media_check = ensure_media_check

        self._downloader: Optional[Downloader] = None
        self._captcha_event = threading.Event()
        self._captcha_response = ""
        self._cancelled = False
        self._progress_emit_interval = 0.12
        self._proxy_emit_interval = 1.0
        self._progress_lock = threading.Lock()
        self._pending_progress: tuple[int, int, int, int] | None = None
        self._last_progress_emit_at = 0.0
        self._last_done_count = -1
        self._proxy_lock = threading.Lock()
        self._pending_proxy_state: tuple[list[str], list[str]] | None = None
        self._last_proxy_emit_at = 0.0
        self._last_proxy_signature: tuple[int, tuple[str, ...]] | None = None
        self._proxy_labels_cache: list[str] = []
        self._proxy_count = 0

    def _proxy_state_callback(self, proxies, active_indexes):
        if not self._proxy_labels_cache or len(proxies) != self._proxy_count:
            self._proxy_count = len(proxies)
            self._proxy_labels_cache = [f"[{idx}] {value or 'LOCAL'}" for idx, value in enumerate(proxies)]
        labels = self._proxy_labels_cache
        active = [labels[i] for i in active_indexes if 0 <= i < len(labels)]
        self._emit_proxy_state(labels, active)

    def _emit_progress(self, force: bool = False) -> None:
        payload = None
        now = time.monotonic()
        with self._progress_lock:
            if self._pending_progress is None:
                return
            downloaded, total, done, total_parts = self._pending_progress
            interval_ok = now - self._last_progress_emit_at >= self._progress_emit_interval
            progress_done = total > 0 and downloaded >= total
            should_emit = force or interval_ok or done != self._last_done_count or progress_done
            if not should_emit:
                return
            payload = self._pending_progress
            self._pending_progress = None
            self._last_progress_emit_at = now
            self._last_done_count = done

        if payload is not None:
            self.progress.emit(*payload)

    def _progress_callback(self, downloaded: int, total: int, done: int, total_parts: int) -> None:
        with self._progress_lock:
            self._pending_progress = (downloaded, total, done, total_parts)
        self._emit_progress()

    def _emit_proxy_state(self, labels: list[str], active: list[str], force: bool = False) -> None:
        emit_payload: tuple[list[str], list[str]] | None = None
        now = time.monotonic()
        signature = (len(labels), tuple(active))

        with self._proxy_lock:
            self._pending_proxy_state = (labels, active)
            if signature == self._last_proxy_signature and not force:
                return

            interval_ok = now - self._last_proxy_emit_at >= self._proxy_emit_interval
            if not (force or interval_ok):
                return

            emit_payload = self._pending_proxy_state
            self._pending_proxy_state = None
            self._last_proxy_emit_at = now
            self._last_proxy_signature = signature

        if emit_payload is not None:
            self.proxy_state.emit(*emit_payload)

    def _flush_pending_signals(self) -> None:
        self._emit_progress(force=True)
        pending_proxy: tuple[list[str], list[str]] | None
        with self._proxy_lock:
            pending_proxy = self._pending_proxy_state
            self._pending_proxy_state = None
        if pending_proxy is not None:
            self.proxy_state.emit(*pending_proxy)

    def _captcha_callback(self, image_bytes: bytes, challenge: str, captcha_url: str) -> str:
        self._captcha_event.clear()
        self.captcha_required.emit(image_bytes, challenge, captcha_url)
        self._captcha_event.wait()
        return self._captcha_response

    def submit_captcha(self, response: str) -> None:
        self._captcha_response = response
        self._captcha_event.set()

    def cancel(self) -> None:
        self._cancelled = True
        if self._downloader:
            self._downloader.cancel()
        self._captcha_response = ""
        self._captcha_event.set()

    def run(self) -> None:
        downloader = Downloader(
            status_callback=self.status.emit,
            progress_callback=self._progress_callback,
            proxy_state_callback=self._proxy_state_callback,
            show_console_progress=False,
        )
        self._downloader = downloader

        try:
            output_path = downloader.download(
                self.url,
                filename=self.filename,
                threads=self.threads,
                split_size=self.split_size,
                captcha_callback=self._captcha_callback,
                ensure_media_check=self.ensure_media_check,
            )
        except DownloadCancelled:
            if not self._cancelled:
                self.status.emit("Download cancelled")
        except Exception as exc:  # pragma: no cover - runtime failure path
            self.error.emit(str(exc))
        else:
            self.succeeded.emit(str(output_path))
        finally:
            self._flush_pending_signals()
            self._downloader = None
            self._captcha_event.set()
            self.stopped.emit()


class ProxyLoaderWorker(QThread):  # pragma: no cover - proxy refresh
    status = Signal(str)
    completed = Signal(list)
    error = Signal(str)

    def __init__(
        self,
        refresh: bool = False,
        *,
        max_candidates: int | None = None,
        recheck_cached: bool = False,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.refresh = refresh
        self.max_candidates = max_candidates
        self.recheck_cached = recheck_cached

    def run(self) -> None:
        try:
            proxies = get_working_proxies(
                refresh=self.refresh,
                status_callback=self.status.emit,
                max_candidates=self.max_candidates,
                recheck_cached=self.recheck_cached,
            )
        except Exception as exc:  # pragma: no cover - network failure path
            self.error.emit(str(exc))
        else:
            self.completed.emit(proxies)
        finally:
            self.status.emit('Proxy refresh finished' if self.refresh else 'Proxy load finished')

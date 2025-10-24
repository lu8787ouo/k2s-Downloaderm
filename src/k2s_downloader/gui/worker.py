from __future__ import annotations

import threading
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

    def _proxy_state_callback(self, proxies, active_indexes):
        labels = [f"[{idx}] {value or 'LOCAL'}" for idx, value in enumerate(proxies)]
        active = [labels[i] for i in active_indexes if 0 <= i < len(labels)]
        self.proxy_state.emit(labels, active)

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
            progress_callback=self.progress.emit,
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


from __future__ import annotations

import time

from PySide6.QtCore import Qt
from PySide6.QtGui import QCloseEvent, QPixmap
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QSizePolicy,
    QSpinBox,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from .worker import DownloadWorker, ProxyLoaderWorker
from ..core.downloader import human_readable_bytes


class MainWindow(QMainWindow):  # pragma: no cover - GUI wiring
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("K2S Downloaderm")
        self._worker: DownloadWorker | None = None
        self._progress_scale = 1024
        self._download_start_time: float | None = None
        self._proxy_loader: ProxyLoaderWorker | None = None
        self._proxy_load_start: float | None = None
        self._smoothed_rate: float = 0.0
        self._smoothed_eta_seconds: float | None = None
        self._last_progress_time: float | None = None
        self._last_downloaded: int = 0
        self._current_available: list[str] = []
        self._current_active: list[str] = []
        self._current_available_raw: list[str | None] = []
        self._collapsed_height: int | None = None
        self._collapsed_height: int | None = None
        self._setup_ui()
        self._start_proxy_loader(False)

    def _setup_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(24, 24, 24, 24)
        root.setSpacing(20)

        header = QFrame()
        header.setObjectName("Header")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(12)

        title_block = QVBoxLayout()
        title_block.setContentsMargins(0, 0, 0, 0)
        self.title_label = QLabel("K2S Downloaderm")
        self.title_label.setObjectName("TitleLabel")
        self.subtitle_label = QLabel("Parallel Keep2Share downloader")
        self.subtitle_label.setObjectName("SubtitleLabel")
        title_block.addWidget(self.title_label)
        title_block.addWidget(self.subtitle_label)
        header_layout.addLayout(title_block)
        header_layout.addStretch()

        root.addWidget(header)

        content_layout = QHBoxLayout()
        content_layout.setSpacing(20)
        root.addLayout(content_layout)

        settings_card = QFrame()
        settings_card.setObjectName("Card")
        settings_layout = QVBoxLayout(settings_card)
        settings_layout.setContentsMargins(20, 20, 20, 20)
        settings_layout.setSpacing(14)

        source_heading = QLabel("Download Source")
        source_heading.setObjectName("SectionHeading")
        settings_layout.addWidget(source_heading)

        self.url_edit = QLineEdit()
        self.url_edit.setPlaceholderText("https://k2s.cc/file/xxxx")
        settings_layout.addWidget(self.url_edit)

        self.filename_edit = QLineEdit()
        self.filename_edit.setPlaceholderText("Optional override name")
        settings_layout.addWidget(self.filename_edit)

        controls_grid = QGridLayout()
        controls_grid.setHorizontalSpacing(14)
        controls_grid.setVerticalSpacing(10)

        threads_caption = QLabel("Threads")
        threads_caption.setObjectName("CaptionLabel")
        self.thread_spin = QSpinBox()
        self.thread_spin.setRange(1, 128)
        self.thread_spin.setValue(20)
        controls_grid.addWidget(threads_caption, 0, 0)
        controls_grid.addWidget(self.thread_spin, 1, 0)

        split_caption = QLabel("Split size (MB)")
        split_caption.setObjectName("CaptionLabel")
        self.split_spin = QSpinBox()
        self.split_spin.setRange(1, 1024)
        self.split_spin.setValue(20)
        self.split_spin.setSuffix(" MB")
        controls_grid.addWidget(split_caption, 0, 1)
        controls_grid.addWidget(self.split_spin, 1, 1)

        settings_layout.addLayout(controls_grid)

        self.ffmpeg_check = QCheckBox("Run ffmpeg integrity check when applicable")
        self.ffmpeg_check.setChecked(True)
        settings_layout.addWidget(self.ffmpeg_check)

        proxy_heading = QLabel("Proxy")
        proxy_heading.setObjectName("SectionHeading")
        settings_layout.addWidget(proxy_heading)

        proxy_summary_row = QHBoxLayout()
        proxy_summary_row.setSpacing(10)
        self.proxy_summary_label = QLabel("Available: 0 | Active: 0")
        self.proxy_summary_label.setObjectName("HintLabel")
        self.proxy_refresh_button = QPushButton("Refresh proxies")
        self.proxy_refresh_button.clicked.connect(self._trigger_proxy_refresh)
        proxy_summary_row.addWidget(self.proxy_summary_label)
        proxy_summary_row.addStretch()
        proxy_summary_row.addWidget(self.proxy_refresh_button)
        settings_layout.addLayout(proxy_summary_row)

        limit_row = QHBoxLayout()
        limit_row.setSpacing(10)
        limit_label = QLabel("Max to validate")
        limit_label.setObjectName("CaptionLabel")
        self.proxy_limit_spin = QSpinBox()
        self.proxy_limit_spin.setRange(0, 100000)
        self.proxy_limit_spin.setSingleStep(1000)
        self.proxy_limit_spin.setSpecialValueText("All")
        self.proxy_limit_spin.setValue(1000)
        limit_row.addWidget(limit_label)
        limit_row.addWidget(self.proxy_limit_spin)
        limit_row.addStretch()
        settings_layout.addLayout(limit_row)

        self.revalidate_checkbox = QCheckBox("Revalidate cached proxies")
        settings_layout.addWidget(self.revalidate_checkbox)

        settings_layout.addStretch()

        content_layout.addWidget(settings_card, 1)

        progress_card = QFrame()
        progress_card.setObjectName("Card")
        progress_layout = QVBoxLayout(progress_card)
        progress_layout.setContentsMargins(20, 20, 20, 20)
        progress_layout.setSpacing(14)

        status_block = QVBoxLayout()
        status_block.setContentsMargins(0, 0, 0, 0)
        self.status_label = QLabel("Idle")
        self.status_label.setObjectName("SectionHeading")
        self.parts_label = QLabel("0 / 0 parts")
        self.parts_label.setObjectName("HintLabel")
        self.size_label = QLabel("0 / 0")
        self.size_label.setObjectName("HintLabel")
        status_block.addWidget(self.status_label)
        status_block.addWidget(self.parts_label)
        status_block.addWidget(self.size_label)
        progress_layout.addLayout(status_block)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        progress_layout.addWidget(self.progress_bar)

        self.metrics_label = QLabel("Elapsed: 0s | ETA: -- | Speed: --")
        progress_layout.addWidget(self.metrics_label)

        self.log_view = QPlainTextEdit()
        self.log_view.setObjectName("LogView")
        self.log_view.setReadOnly(True)
        self.log_view.setMinimumHeight(200)
        progress_layout.addWidget(self.log_view, 1)

        button_row = QHBoxLayout()
        button_row.setSpacing(12)
        self.start_button = QPushButton("Start download")
        self.start_button.clicked.connect(self.start_download)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setEnabled(False)
        self.cancel_button.clicked.connect(self.cancel_download)
        button_row.addWidget(self.start_button)
        button_row.addWidget(self.cancel_button)
        progress_layout.addLayout(button_row)

        content_layout.addWidget(progress_card, 1)

        toggle_row = QHBoxLayout()
        toggle_row.setContentsMargins(0, 0, 0, 0)
        toggle_row.setSpacing(6)
        self.dev_toggle = QToolButton()
        self.dev_toggle.setObjectName("DevToggle")
        self.dev_toggle.setCheckable(True)
        self.dev_toggle.setArrowType(Qt.ArrowType.RightArrow)
        self.dev_toggle.setText("Developer tools")
        self.dev_toggle.setToolTip("Show developer diagnostics")
        self.dev_toggle.toggled.connect(self._toggle_dev_panel)

        toggle_row.addWidget(self.dev_toggle)
        toggle_row.addStretch()
        root.addLayout(toggle_row)

        self.dev_group = QFrame()
        self.dev_group.setObjectName("Card")
        dev_layout = QVBoxLayout(self.dev_group)
        dev_layout.setContentsMargins(20, 16, 20, 16)
        dev_layout.setSpacing(12)

        self.dev_tabs = QTabWidget()
        dev_layout.addWidget(self.dev_tabs)

        proxy_tab = QWidget()
        proxy_tab_layout = QVBoxLayout(proxy_tab)
        proxy_tab_layout.setSpacing(10)
        proxy_tab_layout.setContentsMargins(0, 0, 0, 0)
        self.dev_available_label = QLabel("Available proxies: 0")
        proxy_tab_layout.addWidget(self.dev_available_label)
        self.dev_available_list = QPlainTextEdit()
        self.dev_available_list.setObjectName("ProxyList")
        self.dev_available_list.setReadOnly(True)
        self.dev_available_list.setMaximumHeight(140)
        proxy_tab_layout.addWidget(self.dev_available_list)
        self.dev_active_label = QLabel("Active proxies: 0")
        proxy_tab_layout.addWidget(self.dev_active_label)
        self.dev_active_list = QPlainTextEdit()
        self.dev_active_list.setObjectName("ProxyActiveList")
        self.dev_active_list.setReadOnly(True)
        self.dev_active_list.setMaximumHeight(140)
        proxy_tab_layout.addWidget(self.dev_active_list)
        self.dev_tabs.addTab(proxy_tab, "Proxies")

        info_tab = QWidget()
        info_layout = QVBoxLayout(info_tab)
        info_layout.setSpacing(8)
        info_layout.setContentsMargins(0, 0, 0, 0)
        self.info_runtime_label = QLabel("Runtime data will appear here during downloads.")
        self.info_runtime_label.setObjectName("HintLabel")
        info_layout.addWidget(self.info_runtime_label)
        info_layout.addStretch()
        self.dev_tabs.addTab(info_tab, "Info")

        self.dev_group.setVisible(False)
        self.dev_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.dev_group.setMaximumHeight(0)
        root.addWidget(self.dev_group)

        self.captcha_group = QFrame()
        self.captcha_group.setObjectName("Card")
        captcha_layout = QVBoxLayout(self.captcha_group)
        captcha_layout.setContentsMargins(20, 16, 20, 16)
        captcha_layout.setSpacing(10)
        captcha_title = QLabel("Captcha required")
        captcha_title.setObjectName("SectionHeading")
        captcha_layout.addWidget(captcha_title)
        self.captcha_image = QLabel("Waiting for captcha…")
        self.captcha_image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        captcha_layout.addWidget(self.captcha_image)
        self.captcha_input = QLineEdit()
        self.captcha_input.setPlaceholderText("Enter captcha response")
        self.captcha_input.returnPressed.connect(self.submit_captcha)
        captcha_layout.addWidget(self.captcha_input)
        self.captcha_submit = QPushButton("Submit captcha")
        self.captcha_submit.clicked.connect(self.submit_captcha)
        captcha_layout.addWidget(self.captcha_submit)
        self.captcha_group.setVisible(False)
        root.addWidget(self.captcha_group)
        self._collapsed_height = self.sizeHint().height()

        self._collapsed_height = self.sizeHint().height()

    def _start_proxy_loader(self, refresh: bool) -> None:
        if self._proxy_loader:
            return
        if refresh and self._worker:
            QMessageBox.information(self, "Busy", "Cannot refresh proxies while a download is running.")
            return

        limit_value = self.proxy_limit_spin.value()
        max_candidates = None if limit_value == 0 else limit_value
        recheck_cached = self.revalidate_checkbox.isChecked()
        limit_text = "All" if max_candidates is None else str(limit_value)

        self.proxy_refresh_button.setEnabled(False)
        self.proxy_limit_spin.setEnabled(False)
        self.revalidate_checkbox.setEnabled(False)
        self._proxy_load_start = time.monotonic()
        if refresh:
            self._append_log(f"Refreshing proxy list (limit={limit_text}, revalidate={recheck_cached})...")
        else:
            self._append_log(f"Loading proxy list (limit={limit_text}, revalidate={recheck_cached})...")
        loader = ProxyLoaderWorker(
            refresh=refresh,
            max_candidates=max_candidates,
            recheck_cached=recheck_cached,
        )
        loader.status.connect(self._append_log)
        loader.completed.connect(self._handle_proxy_loader_completed)
        loader.error.connect(self._handle_proxy_loader_error)
        loader.finished.connect(self._on_proxy_loader_finished)
        self._proxy_loader = loader
        loader.start()

    def _trigger_proxy_refresh(self) -> None:
        self._start_proxy_loader(True)

    def _toggle_dev_panel(self, state: bool) -> None:
        if state:
            self._collapsed_height = self.size().height()# 儲存展開前的高度
            self.dev_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            self.dev_group.setMaximumHeight(16777215)
            self.dev_group.updateGeometry()
            self.dev_group.setVisible(state)
            self.dev_toggle.setArrowType(Qt.ArrowType.DownArrow)
        else:
            self.dev_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            self.dev_group.setMaximumHeight(0)
            self.dev_group.setMinimumHeight(0)
            self.dev_group.setVisible(state)
            self.dev_group.updateGeometry()
            if self._collapsed_height is not None:
                self.setMinimumHeight(0)           # 解除可能留下的 min height 限制
                self.resize(self.width(), self._collapsed_height)
            self.dev_toggle.setArrowType(Qt.ArrowType.RightArrow)

    def start_download(self) -> None:
        if self._worker:
            return

        url = self.url_edit.text().strip()
        if not url:
            QMessageBox.warning(self, "Missing URL", "Please enter a k2s download URL.")
            return

        filename = self.filename_edit.text().strip() or None
        threads = self.thread_spin.value()
        split_size = self.split_spin.value() * 1024 * 1024
        ensure_check = self.ffmpeg_check.isChecked()

        self.log_view.clear()
        self._append_log("Starting download...")
        self._download_start_time = time.monotonic()
        self._smoothed_rate = 0.0
        self._smoothed_eta_seconds = None
        self._last_progress_time = self._download_start_time
        self._last_downloaded = 0
        self._update_proxy_state([], [])

        worker = DownloadWorker(
            url,
            filename=filename,
            threads=threads,
            split_size=split_size,
            ensure_media_check=ensure_check,
        )
        worker.progress.connect(self._update_progress)
        worker.status.connect(self._append_log)
        worker.error.connect(self._handle_error)
        worker.succeeded.connect(self._handle_finished)
        worker.proxy_state.connect(self._update_proxy_state)
        worker.captcha_required.connect(self._show_captcha)
        worker.stopped.connect(self._reset_state)

        self._worker = worker
        self.start_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.status_label.setText("Preparing download…")
        worker.start()

    def cancel_download(self) -> None:
        if self._worker:
            self._worker.cancel()
            self._append_log("Cancellation requested…")

    def submit_captcha(self) -> None:
        if not self._worker:
            return
        response = self.captcha_input.text().strip()
        if not response:
            QMessageBox.information(self, "Captcha", "Please enter a captcha response.")
            return
        self._worker.submit_captcha(response)
        self.captcha_group.setVisible(False)
        self.captcha_input.clear()

    def _update_progress(self, downloaded: int, total: int, done: int, total_parts: int) -> None:
        max_value = max(1, total // self._progress_scale)
        self.progress_bar.setRange(0, max_value)
        self.progress_bar.setValue(max(0, downloaded // self._progress_scale))

        self.status_label.setText("Downloading")
        self.parts_label.setText(f"{done}/{total_parts} parts")
        self.size_label.setText(
            f"{human_readable_bytes(downloaded)} / {human_readable_bytes(total)}"
        )

        if self._download_start_time is not None:
            elapsed = max(0.0, time.monotonic() - self._download_start_time)
        else:
            elapsed = 0.0
        current_time = time.monotonic()
        instant_rate = 0.0
        if self._last_progress_time is not None:
            dt = current_time - self._last_progress_time
            delta = downloaded - self._last_downloaded
            if dt > 0 and delta >= 0:
                instant_rate = delta / dt
        if instant_rate > 0:
            alpha = 0.3
            if self._smoothed_rate <= 0:
                self._smoothed_rate = instant_rate
            else:
                self._smoothed_rate = (1 - alpha) * self._smoothed_rate + alpha * instant_rate
        speed_text = self._format_speed(self._smoothed_rate)

        remaining = None
        if self._smoothed_rate > 0:
            remaining = (total - downloaded) / self._smoothed_rate
        if remaining is None and elapsed > 0 and downloaded > 0:
            avg_rate = downloaded / elapsed
            if avg_rate > 0:
                remaining = (total - downloaded) / avg_rate
        if remaining is not None:
            beta = 0.9
            if self._smoothed_eta_seconds is None:
                self._smoothed_eta_seconds = remaining
            else:
                self._smoothed_eta_seconds = (1 - beta) * self._smoothed_eta_seconds + beta * remaining
            eta_text = self._format_duration(self._smoothed_eta_seconds)
        else:
            eta_text = "--"

        self.metrics_label.setText(
            f"Elapsed: {self._format_duration(elapsed)} | ETA: {eta_text} | Speed: {speed_text}"
        )
        self.info_runtime_label.setText(
            f"Threads: {self.thread_spin.value()} • Split: {self.split_spin.value()} MB • Active proxies: {len(self._current_active)}"
        )
        self._last_progress_time = current_time
        self._last_downloaded = downloaded

    def _update_proxy_state(self, proxies: list[str], active: list[str]) -> None:
        self._current_available = proxies
        self._current_active = active
        available_text = "\n".join(proxies) if proxies else "(none)"
        active_text = "\n".join(active) if active else "(none)"
        available_count = max(len(self._current_available_raw) - 1, 0) if self._current_available_raw else max(len(proxies) - 1, 0)
        active_count = len(active)
        self.dev_available_label.setText(f"Available proxies: {available_count}")
        self.dev_available_list.setPlainText(available_text)
        self.dev_active_label.setText(f"Active proxies: {active_count}")
        self.dev_active_list.setPlainText(active_text)
        limit_value = self.proxy_limit_spin.value()
        limit_text = "All" if limit_value == 0 else str(limit_value)
        self.proxy_summary_label.setText(f"Available: {available_count} | Active: {active_count} | Limit: {limit_text}")

    def _format_duration(self, seconds: float | None) -> str:
        if seconds is None:
            return "--"
        seconds = max(0, int(seconds))
        hours, rem = divmod(seconds, 3600)
        minutes, secs = divmod(rem, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:d}:{secs:02d}"

    def _format_speed(self, speed: float) -> str:
        if speed <= 0:
            return "--"
        units = ["B/s", "KiB/s", "MiB/s", "GiB/s"]
        value = speed
        for unit in units:
            if value < 1024 or unit == units[-1]:
                if unit == "B/s":
                    return f"{value:.0f} {unit}"
                return f"{value:.2f} {unit}"
            value /= 1024
        return f"{value:.2f} {units[-1]}"

    def _handle_proxy_loader_completed(self, proxies: list) -> None:
        self._current_available_raw = proxies
        labels = [f"[{idx}] {value or 'LOCAL'}" for idx, value in enumerate(proxies)]
        self._update_proxy_state(labels, self._current_active)
        if self._proxy_load_start is not None:
            elapsed = time.monotonic() - self._proxy_load_start
            self._append_log(f"Proxy refresh completed in {self._format_duration(elapsed)}")

    def _handle_proxy_loader_error(self, message: str) -> None:
        self._append_log(f"Proxy refresh failed: {message}")
        QMessageBox.warning(self, "Proxy refresh failed", message)

    def _on_proxy_loader_finished(self) -> None:
        self.proxy_refresh_button.setEnabled(True)
        self.proxy_limit_spin.setEnabled(True)
        self.revalidate_checkbox.setEnabled(True)
        self._proxy_loader = None
        self._proxy_load_start = None

    def _append_log(self, message: str) -> None:
        self.log_view.appendPlainText(message)

    def _handle_error(self, message: str) -> None:
        QMessageBox.critical(self, "Download failed", message)

    def _handle_finished(self, output_path: str) -> None:
        self._append_log(f"Download finished: {output_path}")
        if self._download_start_time is not None:
            elapsed = time.monotonic() - self._download_start_time
            self._append_log(f"Completed in {self._format_duration(elapsed)}")
        QMessageBox.information(self, "Download complete", f"Saved to\n{output_path}")

    def _show_captcha(self, image_bytes: bytes, challenge: str, captcha_url: str) -> None:
        pixmap = QPixmap()
        pixmap.loadFromData(image_bytes)
        self.captcha_image.setPixmap(pixmap)
        self.captcha_group.setVisible(True)
        self.captcha_input.clear()
        self.captcha_input.setFocus()

    def _reset_state(self) -> None:
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.status_label.setText("Idle")
        self.parts_label.setText("0 / 0 parts")
        self.size_label.setText("0 / 0")
        self.progress_bar.setValue(0)
        self.metrics_label.setText("Elapsed: 0s | ETA: -- | Speed: --")
        self._download_start_time = None
        self._smoothed_rate = 0.0
        self._smoothed_eta_seconds = None
        self._last_progress_time = None
        self._last_downloaded = 0
        self.captcha_group.setVisible(False)
        self._update_proxy_state([], [])
        self.info_runtime_label.setText("Runtime data will appear here during downloads.")
        if self._worker:
            self._worker.wait(100)
        self._worker = None

    def closeEvent(self, event: QCloseEvent) -> None:
        if self._worker:
            self._worker.cancel()
            self._worker.wait(1000)
        super().closeEvent(event)

from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication

from .main_window import MainWindow


def _resource_path(*parts: str) -> Path:
    bundle_base = getattr(sys, "_MEIPASS", None)
    if bundle_base:
        return Path(bundle_base, *parts)

    project_root = Path(__file__).resolve().parents[3]
    candidate = project_root.joinpath(*parts)
    if candidate.exists():
        return candidate

    return project_root.joinpath("src", *parts)


def _load_stylesheet(app: QApplication) -> None:
    style_path = _resource_path("resources", "style.qss")
    if style_path.exists():
        try:
            app.setStyleSheet(style_path.read_text(encoding="utf-8"))
        except OSError:
            print(f"Failed to load stylesheet from {style_path}")


def main() -> int:  # pragma: no cover - Qt entry
    app = QApplication(sys.argv)
    _load_stylesheet(app)
    window = MainWindow()
    icon_path = _resource_path("assets", "icon", "icon.ico")
    if icon_path.exists():
        icon = QIcon(str(icon_path))
        app.setWindowIcon(icon)
        window.setWindowIcon(icon)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

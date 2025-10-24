from __future__ import annotations

import argparse
from pathlib import Path

from .core.downloader import DownloadCancelled, Downloader, parse_size


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="K2S Downloader")
    parser.add_argument("url", help="k2s url to download", action="store")
    parser.add_argument("--filename", help="Filename to save as", action="store")
    parser.add_argument(
        "--threads",
        dest="batch_count",
        type=int,
        default=20,
        help="Number of connections to use (default 20)",
    )
    parser.add_argument(
        "--split-size",
        dest="size",
        default="20M",
        help="Size to split at (default 20M)",
    )
    parser.add_argument(
        "--no-ffmpeg-check",
        dest="skip_media_check",
        action="store_true",
        help="Skip ffmpeg integrity verification",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        split_size = parse_size(args.size)
    except ValueError as exc:
        parser.error(str(exc))

    downloader = Downloader(status_callback=print, show_console_progress=True)

    try:
        downloader.download(
            args.url,
            filename=args.filename,
            threads=args.batch_count,
            split_size=split_size,
            ensure_media_check=not args.skip_media_check,
        )
    except DownloadCancelled:
        print("Download cancelled.")
        return 1
    except Exception as exc:  # pragma: no cover - runtime failure path
        parser.exit(1, f"Error: {exc}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

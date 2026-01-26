"""Save streaming data to organized file structure with automatic cleanup."""

import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from streamer.consumers.base import BaseConsumer
from streamer.settings import settings
from streamer.storage import Storage
from streamer.types import Channel, DataType


class FileConsumer(BaseConsumer):
    """Save streaming data to files organized by date/time with cleanup."""

    def __init__(
        self,
        storage: Storage,
        name: str = "file",
        base_path: str = "output/file_consumer",
        cleanup_interval: int = 300,  # 5 minutes
    ) -> None:
        """Initialize FileConsumer.

        Args:
            storage: Storage instance
            name: Consumer name
            base_path: Base directory for saving files
            cleanup_interval: Interval in seconds to run cleanup task
        """
        super().__init__(storage, name)
        self._base_path = Path(base_path)
        self._cleanup_interval = cleanup_interval
        self._cleanup_task: asyncio.Task[None] | None = None

    def validate(self) -> None:
        """Validate file consumer settings."""

    async def setup(self) -> None:
        """Set up the file consumer."""
        self.logger.info(
            "Setting up file consumer with base path: %s",
            self._base_path,
        )
        # Create base directory if it doesn't exist
        self._base_path.mkdir(parents=True, exist_ok=True)

    async def start(self) -> None:
        """Start the file consumer."""
        self.logger.info("Starting file consumer")
        self._is_running = True

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_worker())

    async def consume(
        self,
        channel: Channel,
        data_type: DataType,
        data: List[Dict[str, Any]],
    ) -> None:
        """Save streaming data to file."""
        if not self._is_running or not data:
            return

        try:
            # Get current timestamp
            now = datetime.now()

            # Create directory structure: base_path/year/month/day/hour/
            data_dir = (
                self._base_path
                / f"{now.year:04d}"
                / f"{now.month:02d}"
                / f"{now.day:02d}"
                / f"{now.hour:02d}"
            )
            data_dir.mkdir(parents=True, exist_ok=True)

            # Create filename with timestamp: YYYYMMDD_HHMMSS_mmmmmm.jsonl
            timestamp_str = now.strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{timestamp_str}_{channel}_{data_type}.jsonl"
            file_path = data_dir / filename

            # Write all data as a single JSON object
            # with channel and data_type as metadata
            with file_path.open("w", encoding="utf-8") as f:
                block = {
                    "exchange": settings.exchange,
                    "channel": channel,
                    "data_type": data_type,
                    "timestamp": now.isoformat(),
                    "data": data,
                }
                f.write(json.dumps(block, ensure_ascii=False) + "\n")

            self.logger.debug(
                "Saved %d items to %s",
                len(data),
                file_path.relative_to(self._base_path),
            )

        except Exception as e:
            self.logger.error("Error saving data to file: %s", e)

    async def _cleanup_worker(self) -> None:
        """Background task to clean up old files."""
        while self._is_running:
            try:
                await self._cleanup_old_files()
            except Exception as e:
                self.logger.error("Error during cleanup: %s", e)

            await asyncio.sleep(self._cleanup_interval)

    def _is_old_hour_dir(self, hour_dir: Path, cutoff_time: datetime) -> bool:
        """Determine if an hour_dir is older than cutoff_time."""
        try:
            dir_year = int(hour_dir.parent.parent.parent.name)
            dir_month = int(hour_dir.parent.parent.name)
            dir_day = int(hour_dir.parent.name)
            dir_hour = int(hour_dir.name)
            dir_datetime = datetime(dir_year, dir_month, dir_day, dir_hour)
            return dir_datetime < cutoff_time
        except ValueError:
            return False

    def _remove_hour_dir(self, hour_dir: Path) -> tuple[int, int]:
        """Remove files in hour_dir and the hour_dir itself. Returns (#files, #dirs)."""
        local_removed_files = 0
        for file_path in hour_dir.rglob("*"):
            if file_path.is_file():
                file_path.unlink()
                local_removed_files += 1
        try:
            hour_dir.rmdir()
        except OSError:
            # Directory may not be empty or removable
            return local_removed_files, 0
        else:
            # Try to remove parent directories if empty
            self._remove_empty_dirs(hour_dir.parent)
            return local_removed_files, 1

    def _walk_hour_dirs(self) -> List[Path]:
        """Yield all hour-level directories in the data hierarchy."""
        hour_dirs: list[Path] = []
        if not self._base_path.exists():
            return hour_dirs
        for year_dir in self._base_path.iterdir():
            if not year_dir.is_dir():
                continue

            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue

                for day_dir in month_dir.iterdir():
                    if not day_dir.is_dir():
                        continue

                    for hour_dir in day_dir.iterdir():
                        if not hour_dir.is_dir():
                            continue
                        hour_dirs.append(hour_dir)
        return hour_dirs

    async def _cleanup_old_files(self) -> None:
        """Remove files older than 4 hours."""
        if not self._base_path.exists():
            return

        cutoff_time = datetime.now() - timedelta(hours=4)
        removed_files = 0
        removed_dirs = 0

        hour_dirs = self._walk_hour_dirs()

        for hour_dir in hour_dirs:
            if self._is_old_hour_dir(hour_dir, cutoff_time):
                try:
                    f, d = self._remove_hour_dir(hour_dir)
                    removed_files += f
                    removed_dirs += d
                except OSError as e:
                    self.logger.warning(
                        "Error processing directory %s: %s", hour_dir, e
                    )
                except Exception as e:
                    self.logger.warning(
                        "Error processing directory %s: %s", hour_dir, e
                    )

        if removed_files > 0 or removed_dirs > 0:
            self.logger.info(
                "Cleanup completed: removed %d files, %d directories",
                removed_files,
                removed_dirs,
            )

    def _remove_empty_dirs(self, start_path: Path) -> None:
        """Remove empty directories upwards from start_path."""
        current = start_path
        while current != self._base_path and current.exists():
            try:
                current.rmdir()  # Will fail if not empty
                current = current.parent
            except OSError:
                # Directory not empty, stop
                break

    async def stop(self) -> None:
        """Stop the file consumer."""
        self.logger.info("Stopping file consumer")
        self._is_running = False

        # Cancel cleanup task
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cleanup_task

        # Final cleanup
        await self._cleanup_old_files()

"""Cross-platform inter-process file locks and crash-safe atomic JSON I/O (zero dependencies)."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

if os.name == "nt":
    import msvcrt
else:
    import fcntl


_IN_PROCESS_LOCKS_GUARD = threading.Lock()
_IN_PROCESS_LOCKS: Dict[str, threading.RLock] = {}


def _get_thread_lock(key: str) -> threading.RLock:
    with _IN_PROCESS_LOCKS_GUARD:
        lk = _IN_PROCESS_LOCKS.get(key)
        if lk is None:
            lk = threading.RLock()
            _IN_PROCESS_LOCKS[key] = lk
        return lk


class CrossProcessFileLock:
    """Advisory exclusive lock safe across both threads and separate Python processes."""

    def __init__(
        self,
        lock_path: Path,
        timeout: float = 10.0,
        poll_interval: float = 0.02,
        blocking: bool = True,
    ) -> None:
        self.lock_path = Path(lock_path)
        self.timeout = max(0.0, float(timeout))
        self.poll_interval = max(0.005, float(poll_interval))
        self.blocking = bool(blocking)
        self._thread_lock = _get_thread_lock(str(self.lock_path.resolve()))
        self._fh: Optional[Any] = None
        self._acquired = False

    def acquire(self) -> bool:
        if self._acquired:
            return True
        if not self._thread_lock.acquire(blocking=self.blocking, timeout=self.timeout if self.blocking else -1):
            return False

        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout
        fh = None
        try:
            fh = self.lock_path.open("a+b")
            # Ensure at least 1 byte exists for Windows msvcrt byte-range locking
            fh.seek(0, os.SEEK_END)
            if fh.tell() == 0:
                fh.write(b"\0")
                fh.flush()

            while True:
                try:
                    if os.name == "nt":
                        fh.seek(0)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._fh = fh
                    self._acquired = True
                    return True
                except OSError:
                    if not self.blocking or time.monotonic() >= deadline:
                        fh.close()
                        self._thread_lock.release()
                        return False
                    time.sleep(self.poll_interval)
        except Exception:
            if fh is not None:
                try:
                    fh.close()
                except OSError:
                    pass
            self._thread_lock.release()
            raise

    def release(self) -> None:
        if not self._acquired:
            return
        fh = self._fh
        self._fh = None
        self._acquired = False
        try:
            if fh is not None:
                try:
                    if os.name == "nt":
                        fh.seek(0)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
                finally:
                    try:
                        fh.close()
                    except OSError:
                        pass
        finally:
            self._thread_lock.release()

    def __enter__(self) -> "CrossProcessFileLock":
        if not self.acquire():
            raise TimeoutError(f"Timed out acquiring cross-process lock: {self.lock_path}")
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        self.release()


def atomic_write_json(path: Path, data: Any, retries: int = 8) -> None:
    """Atomically write JSON with Windows antivirus/concurrent-read retry resilience."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    tmp = target.with_name(f".{target.name}.{os.getpid()}.{uuid.uuid4().hex[:6]}.tmp")
    try:
        tmp.write_text(payload, encoding="utf-8")
        delay = 0.01
        for attempt in range(retries):
            try:
                os.replace(str(tmp), str(target))
                return
            except PermissionError:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
                delay = min(0.15, delay * 1.8)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def read_json_safe(path: Path, default: Any, retries: int = 4) -> Any:
    """Read JSON resiliently against concurrent atomic replacements on Windows."""
    target = Path(path)
    if not target.exists():
        return default
    delay = 0.01
    for attempt in range(retries):
        try:
            raw = target.read_text(encoding="utf-8")
            if not raw.strip():
                return default
            return json.loads(raw)
        except (PermissionError, OSError, json.JSONDecodeError):
            if attempt == retries - 1:
                return default
            time.sleep(delay)
            delay = min(0.08, delay * 1.6)
    return default

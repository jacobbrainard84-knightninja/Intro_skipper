#!/usr/bin/env python3
"""
Intro/Outro Skipper for Steam Deck
Detects intro and outro segments in TV show episodes and generates
skip data for VLC media player.

Usage:
    python3 skip_intro.py --video-dir /path/to/show/season1
    python3 skip_intro.py --video-dir /path/to/show/season1 --show-type anime
    python3 skip_intro.py --video-dir /path/to/show/season1 --show-type cold_open
    python3 skip_intro.py --video-dir /path/to/show --intro-search-end 900
    python3 skip_intro.py --import-timestamps timestamps.json --video-dir /path/to/show
"""

__version__ = "4.1.0"

import argparse
import gc
import glob
import hashlib
import json
import logging
import os
import platform
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

# ============================================================
# Python 3.8 compatibility shim for BooleanOptionalAction
# (introduced in Python 3.9 — polyfill keeps identical CLI syntax
#  on older interpreters without any external dependencies)
# ============================================================
try:
    from argparse import BooleanOptionalAction as _BooleanOptionalAction
except ImportError:  # Python < 3.9
    class _BooleanOptionalAction(argparse.Action):  # type: ignore[no-redef]
        """Minimal backport of argparse.BooleanOptionalAction."""

        def __init__(self, option_strings, dest, default=None, **kwargs):
            _opts: List[str] = []
            for opt in option_strings:
                _opts.append(opt)
                if opt.startswith("--"):
                    _opts.append(f"--no-{opt[2:]}")
            kwargs.pop("nargs", None)
            super().__init__(
                option_strings=_opts, dest=dest, nargs=0,
                default=default, **kwargs)

        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest,
                    not (option_string or "").startswith("--no-"))

        def format_usage(self):
            return " | ".join(self.option_strings)

# ============================================================
# Type Aliases
# ============================================================

Config = Dict[str, Any]
SegmentResult = Tuple[float, float, float]
SegmentDict = Dict[str, Tuple[float, float]]
DetectionResults = Dict[str, SegmentDict]
DurationMap = Dict[str, float]

# ============================================================
# Constants
# ============================================================

VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".m4v", ".wmv", ".flv", ".webm",
    ".ts", ".mpg", ".mpeg", ".3gp", ".ogv", ".vob", ".mts", ".m2ts",
    ".divx", ".asf",
}

_SYSTEM = platform.system()

if _SYSTEM == "Windows":
    _appdata = os.environ.get("APPDATA", os.path.expanduser("~"))
    DATA_DIR = os.path.join(_appdata, "intro_skipper")
    VLC_CONF_DIR = os.path.join(_appdata, "vlc", "lua", "extensions")
    _VLC_INTF_DIR = os.path.join(_appdata, "vlc", "lua", "intf")
elif _SYSTEM == "Darwin":
    DATA_DIR = os.path.expanduser(
        "~/Library/Application Support/intro_skipper")
    VLC_CONF_DIR = os.path.expanduser(
        "~/Library/Application Support/org.videolan.vlc/lua/extensions")
    _VLC_INTF_DIR = os.path.expanduser(
        "~/Library/Application Support/org.videolan.vlc/lua/intf")
else:
    DATA_DIR = os.path.expanduser("~/.config/intro_skipper")
    VLC_CONF_DIR = os.path.expanduser("~/.local/share/vlc/lua/extensions")
    _VLC_INTF_DIR = os.path.expanduser("~/.local/share/vlc/lua/intf")

CACHE_DB = os.path.join(DATA_DIR, "cache.db")
SKIP_DATA_FILE = os.path.join(DATA_DIR, "skip_data.json")
LOG_FILE = os.path.join(DATA_DIR, "intro_skipper.log")

SCHEMA_VERSION = 2
TIMESTAMP_PRECISION = 3

_AUTO_METHODS = frozenset({
    "fingerprint", "graph-fallback", "fingerprint-fallback",
})


@dataclass(frozen=True)
class DetectionConstants:
    MIN_EPISODE_DURATION: float = 60.0
    MIN_FINGERPRINT_STD: float = 1e-8
    DEFAULT_FFMPEG_TIMEOUT: int = 300
    FFPROBE_TIMEOUT: int = 60
    DB_TIMEOUT: int = 60
    BATCH_SIZE: int = 30
    MEMORY_WARNING_MB: int = 100
    DEFAULT_MAX_FINGERPRINT_MB: int = 512
    MAX_RAW_AUDIO_BYTES: int = 500 * 1024 * 1024
    GRAPH_MAX_MATRIX_MB: int = 500
    FFT_CANDIDATE_COUNT: int = 250
    FFT_CLIP_SIGMA: float = 5.0
    EXTRACTION_WORKERS: int = 4
    GRAPH_TOP_K: int = 3
    SUBPROCESS_POLL_INTERVAL: float = 0.25
    MIN_AUDIO_RMS: float = 1e-5
    MAX_INTRO_FRACTION: float = 0.4
    MIN_AUDIO_DURATION_RATIO: float = 0.8
    VLC_SEEK_DELAY_MS: int = 500
    # Maximum byte length of a normalized JSON key (very long stems are
    # truncated with a suffix so the JSON file stays human-readable)
    MAX_KEY_LENGTH: int = 120


CONSTANTS = DetectionConstants()

_EP_PATTERNS = [
    re.compile(r"[Ss](\d+)[.\s]*[Ee](\d+)"),
    re.compile(r"(\d+)[Xx](\d+)"),
    re.compile(r"[Ss](\d+)\s*-\s*[Ee](\d+)"),
]

# ============================================================
# Detection parameters by show type
# ============================================================

_COMMON: Config = {
    "sample_rate": 22050,
    "hop_length": 512,
    "n_bands": 8,
    "frame_size_multiplier": 4,
    "comparison_window": 10,
    "per_episode_threshold_factor": 0.9,
    "min_episodes_agree": 2,
    "refinement_steps": 4,
    "use_graph_consensus": True,
}


def _cfg(**overrides: Any) -> Config:
    c = _COMMON.copy()
    c.update(overrides)
    return c


SHOW_CONFIGS: Dict[str, Config] = {
    "standard": _cfg(
        intro_search_start=0, intro_search_end=420,
        outro_search_duration=150, min_segment_duration=15,
        max_segment_duration=120, similarity_threshold=0.80),
    "anime": _cfg(
        intro_search_start=0, intro_search_end=210,
        outro_search_duration=150, min_segment_duration=60,
        max_segment_duration=105, similarity_threshold=0.73),
    "sitcom": _cfg(
        intro_search_start=0, intro_search_end=180,
        outro_search_duration=90, min_segment_duration=10,
        max_segment_duration=70, comparison_window=8,
        similarity_threshold=0.80),
    "comedy": _cfg(
        intro_search_start=0, intro_search_end=240,
        outro_search_duration=120, min_segment_duration=15,
        max_segment_duration=90, similarity_threshold=0.78),
    "drama": _cfg(
        intro_search_start=0, intro_search_end=480,
        outro_search_duration=150, min_segment_duration=30,
        max_segment_duration=150, similarity_threshold=0.78),
    "scifi": _cfg(
        intro_search_start=0, intro_search_end=420,
        outro_search_duration=150, min_segment_duration=30,
        max_segment_duration=120, similarity_threshold=0.78),
    "horror": _cfg(
        intro_search_start=0, intro_search_end=420,
        outro_search_duration=120, min_segment_duration=20,
        max_segment_duration=120, similarity_threshold=0.75,
        per_episode_threshold_factor=0.85),
    "reality": _cfg(
        intro_search_start=0, intro_search_end=180,
        outro_search_duration=90, min_segment_duration=10,
        max_segment_duration=70, comparison_window=8,
        similarity_threshold=0.80),
    "animated": _cfg(
        intro_search_start=0, intro_search_end=180,
        outro_search_duration=120, min_segment_duration=15,
        max_segment_duration=75, comparison_window=8,
        similarity_threshold=0.78),
    "cold_open": _cfg(
        intro_search_start=0, intro_search_end=1200,
        outro_search_duration=120, min_segment_duration=15,
        max_segment_duration=120, similarity_threshold=0.75,
        per_episode_threshold_factor=0.85),
    "late_intro": _cfg(
        intro_search_start=300, intro_search_end=1200,
        outro_search_duration=120, min_segment_duration=15,
        max_segment_duration=120, similarity_threshold=0.78),
    "documentary": _cfg(
        intro_search_start=0, intro_search_end=300,
        outro_search_duration=120, min_segment_duration=5,
        max_segment_duration=180, similarity_threshold=0.72,
        per_episode_threshold_factor=0.85),
}

logger = logging.getLogger(__name__)
_logging_configured = False
_shutdown_requested = False
_active_processes: List[subprocess.Popen] = []
_process_lock = threading.Lock()


def _signal_handler(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        # Snapshot the list while holding the lock, then operate outside
        # it to prevent deadlock when a worker thread owns _process_lock.
        with _process_lock:
            procs = list(_active_processes)
        for proc in procs:
            try:
                proc.kill()
            except OSError:
                pass
        sys.stderr.write("\nForced shutdown.\n")
        os._exit(130)
    _shutdown_requested = True
    with _process_lock:
        procs = list(_active_processes)
    for proc in procs:
        try:
            proc.terminate()
        except OSError:
            pass
    sys.stderr.write(
        "\nShutdown requested, finishing current operation… "
        "(press Ctrl+C again to force)\n")


def _setup_logging():
    global _logging_configured
    if _logging_configured:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout),
        ],
    )
    _logging_configured = True


# ============================================================
# Utility helpers
# ============================================================


def _parse_episode_tag(name: str) -> Optional[Tuple[int, int]]:
    for pat in _EP_PATTERNS:
        m = pat.search(name)
        if m:
            return int(m.group(1)), int(m.group(2))
    return None


def _snap_timestamp(t: float) -> float:
    return round(t, TIMESTAMP_PRECISION)


def compute_config_hash(config: Config) -> str:
    relevant_keys = [
        "frame_size_multiplier", "hop_length", "intro_search_end",
        "intro_search_start", "n_bands", "outro_search_duration",
        "sample_rate",
    ]
    config_str = "|".join(
        f"{k}={config.get(k, '')}" for k in relevant_keys)
    return hashlib.sha256(
        config_str.encode(), usedforsecurity=False
    ).hexdigest()[:16]


def get_skip_key(filepath: str, use_full_path: bool = False) -> str:
    """Return the dictionary key written into ``skip_data.json``.

    The algorithm mirrors the Lua ``normalize_key`` function exactly so
    that Python (write side) and Lua (read side) always agree:

    1. Take the filename stem (no extension).
    2. Lower-case.
    3. Collapse every common word-separator
       (space, tab, underscore, hyphen, colon, period, en-dash, em-dash)
       into a single dot.
    4. Strip any characters that are not ASCII alphanumeric or a dot.
    5. Remove leading / trailing dots.
    6. Truncate very long stems to ``CONSTANTS.MAX_KEY_LENGTH`` chars.

    When *use_full_path* is True the resolved absolute path is returned
    unchanged so that multiple shows with identical episode filenames can
    coexist in the same JSON file.
    """
    if use_full_path:
        return str(Path(filepath).resolve())

    stem = Path(filepath).stem.lower()
    # Collapse all common separator characters into a single dot.
    # En-dash (U+2013) and em-dash (U+2014) are included as literals;
    # Python's re engine is Unicode-aware so this is safe.
    stem = re.sub(r"[ \t_.\-\u2013\u2014:]+", ".", stem)
    # Remove anything that is not an ASCII letter, digit, or dot.
    stem = re.sub(r"[^a-z0-9.]", "", stem)
    # Collapse runs of dots that survived the previous step.
    stem = re.sub(r"\.{2,}", ".", stem)
    stem = stem.strip(".")

    max_len = CONSTANTS.MAX_KEY_LENGTH
    if len(stem) > max_len:
        stem = stem[: max_len - 3] + "..."

    return stem


# Legacy alias kept for any internal callers that still use the old name.
def _normalize_filename(name: str) -> str:
    """Normalise a *filename* (not a full path) to a skip-data key."""
    return get_skip_key(name)


def escape_lua_string(s: str) -> str:
    s = s.replace("\\", "/")
    out: List[str] = []
    for ch in s:
        if ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ch == "\0":
            out.append("\\0")
        elif ord(ch) < 32:
            out.append(f"\\{ord(ch)}")
        else:
            out.append(ch)
    return "".join(out)


def format_time(seconds: float) -> str:
    if seconds < 0:
        return f"-{format_time(-seconds)}"
    if seconds < 3600:
        return f"{int(seconds // 60)}:{int(seconds % 60):02d}"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}:{m:02d}:{s:02d}"


def _remove_if_exists(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def validate_segments(
    segments: SegmentDict, duration: float,
) -> SegmentDict:
    validated: SegmentDict = {}
    tolerance = 0.5

    if "intro" in segments:
        s, e = segments["intro"]
        s = max(0.0, s)
        e = min(e, duration)
        if s < e <= duration + tolerance:
            frac = (e - s) / duration if duration > 0 else 0
            if frac > CONSTANTS.MAX_INTRO_FRACTION:
                logger.warning(
                    "Intro is %.0f%% of episode (%s–%s / %s)",
                    frac * 100, format_time(s), format_time(e),
                    format_time(duration))
            validated["intro"] = (_snap_timestamp(s), _snap_timestamp(e))
        else:
            logger.warning("Intro OOB: %s–%s (dur %s)",
                           format_time(s), format_time(e),
                           format_time(duration))

    if "outro" in segments:
        s, e = segments["outro"]
        s = max(0.0, s)
        e = min(e, duration)
        if s < e <= duration + tolerance:
            if "intro" not in validated or s >= validated["intro"][1]:
                validated["outro"] = (
                    _snap_timestamp(s), _snap_timestamp(e))
            else:
                logger.warning("Outro overlaps intro, discarding")
        else:
            logger.warning("Outro OOB: %s–%s (dur %s)",
                           format_time(s), format_time(e),
                           format_time(duration))

    return validated


def validate_episode_set(video_paths: List[str]) -> None:
    basenames = [os.path.basename(p) for p in video_paths]
    if len(basenames) >= 3:
        prefix = os.path.commonprefix(basenames)
        avg_len = sum(len(b) for b in basenames) / len(basenames)
        if len(prefix) < 3 and avg_len > 10:
            logger.warning("Files don't share a common prefix")
    seasons: Set[int] = set()
    for name in basenames:
        tag = _parse_episode_tag(name)
        if tag:
            seasons.add(tag[0])
    if len(seasons) > 1:
        logger.warning("Multiple seasons: %s", sorted(seasons))


def select_reference_episode(
    episodes: List[Dict[str, Any]],
    available_fps: Dict[str, np.ndarray],
) -> Optional[str]:
    valid = [ep for ep in episodes if ep["path"] in available_fps]
    if not valid:
        return None
    durations = [ep["duration"] for ep in valid]
    median_dur = float(np.median(durations))

    def score(ep: Dict[str, Any]) -> float:
        d = 1.0 / (1.0 + abs(ep["duration"] - median_dur)
                   / max(median_dur, 1.0))
        fp = available_fps[ep["path"]]
        q = min(float(np.std(fp)) * 2.0, 1.0) if fp.size > 0 else 0.0
        return d * 0.7 + q * 0.3

    return max(valid, key=score)["path"]


def _estimate_fingerprint_mb(
    episodes: List[Dict[str, Any]], config: Config,
) -> float:
    sr = config["sample_rate"]
    hl = config["hop_length"]
    nb = config.get("n_bands", 8)
    i_start = config.get("intro_search_start", 0)
    i_end = config["intro_search_end"]
    o_dur = config["outro_search_duration"]
    total = 0
    for ep in episodes:
        dur = ep["duration"]
        id_ = min(i_end, dur * 0.5) - i_start
        if id_ > 0:
            total += max(1, int(id_ * sr / hl)) * nb * 4
        od_ = min(o_dur, dur)
        if od_ > 0:
            total += max(1, int(od_ * sr / hl)) * nb * 4
    return total / (1024 * 1024)


class ProgressTracker:
    def __init__(self, total: int, operation: str):
        self.total = total
        self.operation = operation
        self.start_time = time.monotonic()
        self.completed = 0

    def update(self, current: int, extra: str = ""):
        self.completed = current
        elapsed = time.monotonic() - self.start_time
        msg = f"  [{current}/{self.total}] {self.operation}"
        if current > 0 and elapsed > 2.0:
            eta = elapsed / current * (self.total - current)
            msg += f" (ETA: {format_time(eta)})"
        if extra:
            msg += f" {extra}"
        logger.info(msg)


# ============================================================
# Database
# ============================================================


class FingerprintCache:
    def __init__(self, db_path: str = CACHE_DB):
        self.db_path = db_path
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()
        self._pending_writes = 0
        self._init_db()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path, timeout=CONSTANTS.DB_TIMEOUT,
                isolation_level="DEFERRED", check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(
                f"PRAGMA busy_timeout={CONSTANTS.DB_TIMEOUT * 1000}")
            self._conn.commit()
        return self._conn

    def close(self):
        if self._conn is not None:
            try:
                self._conn.commit()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _get_schema_version(self, conn: sqlite3.Connection) -> int:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version "
            "(id INTEGER PRIMARY KEY CHECK (id = 1), "
            "version INTEGER NOT NULL)")
        row = conn.execute(
            "SELECT version FROM schema_version WHERE id=1"
        ).fetchone()
        return row[0] if row else 0

    def _set_schema_version(self, conn: sqlite3.Connection, ver: int):
        conn.execute(
            "INSERT OR REPLACE INTO schema_version (id, version) "
            "VALUES (1, ?)", (ver,))

    def _init_db(self):
        conn = self._get_conn()
        try:
            ver = self._get_schema_version(conn)
            if ver < 1:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS fingerprints (
                        file_hash TEXT PRIMARY KEY, file_path TEXT,
                        file_name TEXT, duration REAL,
                        fingerprint BLOB, sample_rate INTEGER,
                        n_bands INTEGER DEFAULT 8,
                        n_frames INTEGER DEFAULT 0,
                        config_hash TEXT DEFAULT '',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS skip_segments (
                        file_path TEXT, segment_type TEXT,
                        start_time REAL, end_time REAL,
                        confidence REAL, method TEXT,
                        PRIMARY KEY (file_path, segment_type)
                    )""")
                self._set_schema_version(conn, 1)
            if ver < 2:
                cols = {r[1] for r in conn.execute(
                    "PRAGMA table_info(fingerprints)")}
                if "config_hash" not in cols:
                    conn.execute(
                        "ALTER TABLE fingerprints "
                        "ADD COLUMN config_hash TEXT DEFAULT ''")
                if "n_frames" not in cols:
                    conn.execute(
                        "ALTER TABLE fingerprints "
                        "ADD COLUMN n_frames INTEGER DEFAULT 0")
                self._set_schema_version(conn, 2)
            conn.commit()
        except Exception as exc:
            logger.error("DB init failed: %s", exc)
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    def file_hash(self, filepath: str, suffix: str = "",
                  config_hash: str = "") -> str:
        with open(filepath, "rb") as f:
            stat = os.fstat(f.fileno())
            h = hashlib.sha256(usedforsecurity=False)
            h.update(os.path.basename(filepath).encode())
            h.update(str(stat.st_size).encode())
            h.update(str(stat.st_mtime_ns).encode())
            if config_hash:
                h.update(config_hash.encode())
            h.update(f.read(65536))
            if stat.st_size > 65536:
                f.seek(-65536, 2)
                h.update(f.read(65536))
        d = h.hexdigest()
        return f"{d}{suffix}" if suffix else d

    def get_fingerprint(
        self, filepath: str, cache_suffix: str = "",
        config_hash: str = "",
    ) -> Optional[Tuple[np.ndarray, int, int, int]]:
        try:
            fhash = self.file_hash(filepath, cache_suffix, config_hash)
        except (OSError, IOError):
            return None
        with self._lock:
            row = self._get_conn().execute(
                "SELECT fingerprint, sample_rate, n_bands, n_frames, "
                "config_hash FROM fingerprints WHERE file_hash=?",
                (fhash,)).fetchone()
        if row and row[0]:
            stored_cfg = row[4] or ""
            if config_hash and stored_cfg and stored_cfg != config_hash:
                return None
            fp = np.frombuffer(row[0], dtype=np.float32).copy()
            sr = row[1]
            nb = row[2] if row[2] else 8
            nf = row[3] if row[3] else (len(fp) // nb if nb > 0 else 0)
            return fp, sr, nb, nf
        return None

    def store_fingerprint(
        self, filepath: str, fingerprint: np.ndarray,
        duration: float, sample_rate: int, n_bands: int,
        n_frames: int, cache_suffix: str = "",
        config_hash: str = "",
    ):
        try:
            fhash = self.file_hash(filepath, cache_suffix, config_hash)
        except (OSError, IOError) as exc:
            logger.warning("Cannot hash %s: %s", filepath, exc)
            return
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO fingerprints "
                "(file_hash, file_path, file_name, duration, "
                "fingerprint, sample_rate, n_bands, n_frames, "
                "config_hash) VALUES (?,?,?,?,?,?,?,?,?)",
                (fhash, filepath, os.path.basename(filepath), duration,
                 fingerprint.flatten().astype(np.float32).tobytes(),
                 sample_rate, n_bands, n_frames, config_hash))
            self._pending_writes += 1
            if self._pending_writes >= 10:
                conn.commit()
                self._pending_writes = 0

    def batch_commit(self):
        with self._lock:
            self._get_conn().commit()
            self._pending_writes = 0

    def get_skip_segments(
        self, filepath: str,
    ) -> Dict[str, Tuple[float, float, float]]:
        with self._lock:
            rows = self._get_conn().execute(
                "SELECT segment_type, start_time, end_time, confidence "
                "FROM skip_segments WHERE file_path=?",
                (filepath,)).fetchall()
        return {r[0]: (r[1], r[2], r[3]) for r in rows}

    def store_skip_segment(
        self, filepath: str, segment_type: str,
        start_time: float, end_time: float,
        confidence: float, method: str,
    ):
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO skip_segments "
                "(file_path, segment_type, start_time, end_time, "
                "confidence, method) VALUES (?,?,?,?,?,?)",
                (filepath, segment_type,
                 _snap_timestamp(start_time),
                 _snap_timestamp(end_time),
                 confidence, method))
            conn.commit()

    def has_cached_results(self, filepath: str) -> bool:
        return len(self.get_skip_segments(filepath)) > 0

    def clear_cache(self, older_than_days: Optional[int] = None):
        methods_tuple = tuple(_AUTO_METHODS)
        placeholders = ",".join("?" * len(methods_tuple))
        with self._lock:
            conn = self._get_conn()
            if older_than_days is not None:
                conn.execute(
                    "DELETE FROM fingerprints "
                    "WHERE created_at < datetime('now', ?)",
                    (f"-{older_than_days} days",))
                conn.execute(
                    f"DELETE FROM skip_segments "
                    f"WHERE method IN ({placeholders}) "
                    f"AND file_path NOT IN "
                    f"(SELECT DISTINCT file_path FROM fingerprints)",
                    methods_tuple)
            else:
                conn.execute("DELETE FROM fingerprints")
                conn.execute(
                    f"DELETE FROM skip_segments "
                    f"WHERE method IN ({placeholders})",
                    methods_tuple)
            conn.commit()
        logger.info("Cache cleared")


# ============================================================
# FFmpeg / FFprobe
# ============================================================


def _find_binary(name: str) -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    explicit = os.path.join(script_dir, "ffmpeg-custom", name)
    if os.path.isfile(explicit) and os.access(explicit, os.X_OK):
        return explicit
    for pattern in (
        os.path.join(script_dir, f"ffmpeg-*-static/{name}"),
        os.path.join(script_dir, f"ffmpeg-*/{name}"),
        os.path.join(script_dir, f"ffmpeg*/{name}"),
    ):
        for match in sorted(glob.glob(pattern), reverse=True):
            if os.path.isfile(match) and os.access(match, os.X_OK):
                return match
    return name


def validate_ffmpeg(ffmpeg_path: str) -> bool:
    try:
        r = subprocess.run(
            [ffmpeg_path, "-version"],
            capture_output=True, text=True, timeout=10)
        return r.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return False


FFMPEG = _find_binary("ffmpeg")
FFPROBE = _find_binary("ffprobe")


def _run_subprocess_interruptible(
    cmd: List[str], timeout: int, capture_output: bool = True,
) -> subprocess.CompletedProcess:
    """Run *cmd* with cooperative shutdown/timeout and pipe-drain threads.

    Background reader threads continuously drain stdout and stderr so
    the subprocess never blocks on the OS pipe buffer (typically 64 KB
    on Linux).  Without this, a verbose ffmpeg run producing more than
    64 KB of output causes a circular wait: ffmpeg blocks trying to
    write, Python blocks waiting for ffmpeg to exit — a silent deadlock.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE if capture_output else subprocess.DEVNULL,
        stderr=subprocess.PIPE if capture_output else subprocess.DEVNULL,
    )
    with _process_lock:
        _active_processes.append(proc)

    stdout_chunks: List[bytes] = []
    stderr_chunks: List[bytes] = []

    def _drain(stream, collector: List[bytes]):
        try:
            for chunk in iter(lambda: stream.read(65536), b""):
                collector.append(chunk)
        except OSError:
            pass

    t_out = t_err = None
    if capture_output:
        t_out = threading.Thread(
            target=_drain, args=(proc.stdout, stdout_chunks), daemon=True)
        t_err = threading.Thread(
            target=_drain, args=(proc.stderr, stderr_chunks), daemon=True)
        t_out.start()
        t_err.start()

    try:
        deadline = time.monotonic() + timeout
        while proc.poll() is None:
            if _shutdown_requested:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                raise KeyboardInterrupt("Shutdown requested")
            if time.monotonic() > deadline:
                proc.kill()
                proc.wait()
                raise subprocess.TimeoutExpired(cmd, timeout)
            time.sleep(CONSTANTS.SUBPROCESS_POLL_INTERVAL)

        if t_out is not None:
            t_out.join(timeout=5)
        if t_err is not None:
            t_err.join(timeout=5)

        out_bytes = b"".join(stdout_chunks)
        err_bytes = b"".join(stderr_chunks)
        return subprocess.CompletedProcess(
            args=cmd, returncode=proc.returncode,
            stdout=out_bytes.decode("utf-8", errors="replace"),
            stderr=err_bytes.decode("utf-8", errors="replace"),
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            try:
                proc.wait(timeout=2)
            except Exception:
                pass
        with _process_lock:
            try:
                _active_processes.remove(proc)
            except ValueError:
                pass


def get_duration(video_path: str) -> Optional[float]:
    cmd = [FFPROBE, "-v", "error", "-show_entries", "format=duration",
           "-of", "json", video_path]
    try:
        result = _run_subprocess_interruptible(
            cmd, timeout=CONSTANTS.FFPROBE_TIMEOUT)
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
        duration = float(data["format"]["duration"])
        return duration if duration > 0 else None
    except (FileNotFoundError, KeyboardInterrupt,
            subprocess.TimeoutExpired):
        return None
    except (subprocess.SubprocessError, OSError, json.JSONDecodeError,
            KeyError, ValueError):
        return None


def extract_audio_segment(
    video_path: str, start: float, duration: float,
    output_path: str, sample_rate: int = 22050,
    timeout: Optional[int] = None,
) -> bool:
    if timeout is None:
        timeout = CONSTANTS.DEFAULT_FFMPEG_TIMEOUT
    cmd = [
        FFMPEG, "-v", "error", "-ss", str(start), "-i", video_path,
        "-t", str(duration), "-vn", "-ac", "1", "-ar", str(sample_rate),
        "-f", "f32le", "-acodec", "pcm_f32le", output_path, "-y"]
    try:
        result = _run_subprocess_interruptible(cmd, timeout=timeout)
        if result.returncode != 0:
            _remove_if_exists(output_path)
            return False
        if (not os.path.isfile(output_path)
                or os.path.getsize(output_path) == 0):
            _remove_if_exists(output_path)
            return False
        file_size = os.path.getsize(output_path)
        extracted_dur = (file_size // 4) / sample_rate
        if extracted_dur < duration * CONSTANTS.MIN_AUDIO_DURATION_RATIO:
            logger.warning(
                "Short extraction for %s (%.1fs vs %.1fs)",
                os.path.basename(video_path), extracted_dur, duration)
        return True
    except (FileNotFoundError, KeyboardInterrupt):
        _remove_if_exists(output_path)
        return False
    except subprocess.TimeoutExpired:
        logger.error("Extraction timeout %ss for %s",
                     timeout, video_path)
        _remove_if_exists(output_path)
        return False
    except (subprocess.SubprocessError, OSError):
        _remove_if_exists(output_path)
        return False


def load_raw_audio(path: str) -> np.ndarray:
    try:
        file_size = os.path.getsize(path)
        if file_size > CONSTANTS.MAX_RAW_AUDIO_BYTES:
            return np.array([], dtype=np.float32)
        audio = np.fromfile(path, dtype=np.float32)
        if not np.all(np.isfinite(audio)):
            audio = np.nan_to_num(audio, nan=0.0, posinf=1.0, neginf=-1.0)
        return audio
    except (MemoryError, OSError, ValueError):
        return np.array([], dtype=np.float32)


# ============================================================
# Audio Fingerprinting
# ============================================================


def compute_energy_fingerprint(
    audio: np.ndarray, sample_rate: int,
    hop_length: int = 512, n_bands: int = 8,
    frame_size_multiplier: int = 4,
) -> np.ndarray:
    if len(audio) == 0:
        return np.zeros((1, n_bands), dtype=np.float32)
    frame_size = hop_length * frame_size_multiplier
    # Standard sliding-window count: floor((N - W) / H) + 1
    n_frames = max(1, (len(audio) - frame_size) // hop_length + 1)
    required = (n_frames - 1) * hop_length + frame_size
    pad = max(0, required - len(audio))
    if pad > 0:
        audio = np.pad(audio, (0, pad), mode="constant")
    frames = np.lib.stride_tricks.sliding_window_view(
        audio, frame_size)[::hop_length][:n_frames]
    window = np.hanning(frame_size).astype(np.float32)
    spectra = np.abs(np.fft.rfft(frames * window, axis=1))
    total_bins = spectra.shape[1]
    fp = np.zeros((n_frames, n_bands), dtype=np.float32)
    for b in range(n_bands):
        lo = b * total_bins // n_bands
        hi = (b + 1) * total_bins // n_bands
        if hi > lo:
            fp[:, b] = np.sqrt(np.mean(spectra[:, lo:hi] ** 2, axis=1))
    mx = fp.max()
    if mx > CONSTANTS.MIN_FINGERPRINT_STD:
        fp /= mx
    return fp


def reshape_fingerprint(
    fp: np.ndarray, n_bands: int, n_frames: int,
) -> Optional[np.ndarray]:
    if n_bands <= 0 or n_frames <= 0 or fp.size == 0:
        return None
    if fp.ndim == 2:
        if fp.shape == (n_frames, n_bands):
            return fp
        if fp.shape == (n_bands, n_frames):
            return fp.T
        if fp.shape[1] == n_bands:
            return fp
        if fp.shape[0] == n_bands:
            return fp.T
        return None
    expected = n_frames * n_bands
    if len(fp) < expected:
        if len(fp) < expected * 0.9:
            return None
        fp = np.pad(fp, (0, expected - len(fp)))
    elif len(fp) > expected:
        fp = fp[:expected]
    return fp.reshape(n_frames, n_bands)


def compare_fingerprints(
    fp1: np.ndarray, fp2: np.ndarray,
    window_seconds: float = 10,
    hop_length: int = 512, sample_rate: int = 22050,
) -> np.ndarray:
    if (fp1.size == 0 or fp2.size == 0 or fp1.ndim != 2
            or fp2.ndim != 2 or fp1.shape[1] != fp2.shape[1]
            or fp1.shape[0] < 2 or fp2.shape[0] < 2):
        return np.zeros((1, 1), dtype=np.float32)
    if not np.all(np.isfinite(fp1)):
        fp1 = np.nan_to_num(fp1, nan=0.0, posinf=1.0, neginf=0.0)
    if not np.all(np.isfinite(fp2)):
        fp2 = np.nan_to_num(fp2, nan=0.0, posinf=1.0, neginf=0.0)
    fpw = max(1, int(window_seconds * sample_rate / hop_length))
    if fp1.shape[0] < fpw or fp2.shape[0] < fpw:
        fpw = min(fp1.shape[0], fp2.shape[0])
        if fpw < 1:
            return np.zeros((1, 1), dtype=np.float32)
    n1 = fp1.shape[0] // fpw
    n2 = fp2.shape[0] // fpw
    if n1 == 0 or n2 == 0:
        return np.zeros((max(1, n1), max(1, n2)), dtype=np.float32)
    # float32 is sufficient for 8-band correlation and halves peak memory
    w1 = fp1[:n1 * fpw].reshape(n1, -1).astype(np.float32)
    w2 = fp2[:n2 * fpw].reshape(n2, -1).astype(np.float32)
    eps = np.float32(CONSTANTS.MIN_FINGERPRINT_STD)
    m1 = w1.mean(axis=1, keepdims=True)
    s1 = w1.std(axis=1, keepdims=True)
    m2 = w2.mean(axis=1, keepdims=True)
    s2 = w2.std(axis=1, keepdims=True)
    valid1 = np.atleast_1d(s1.flatten() > eps)
    valid2 = np.atleast_1d(s2.flatten() > eps)
    s1 = np.where(s1 > eps, s1, np.float32(1.0))
    s2 = np.where(s2 > eps, s2, np.float32(1.0))
    w1n = (w1 - m1) / s1
    w2n = (w2 - m2) / s2
    D = np.float32(w1n.shape[1])
    sim = (w1n @ w2n.T) / D
    np.clip(sim, 0.0, 1.0, out=sim)
    sim[~valid1, :] = 0.0
    sim[:, ~valid2] = 0.0
    return sim


# ============================================================
# Graph-Based Segment Consensus
# ============================================================


def _chunk_fingerprints(
    fp: np.ndarray, chunk_seconds: float, hop_seconds: float,
    sample_rate: int, hop_length: int,
) -> List[Tuple[float, np.ndarray]]:
    fps = sample_rate / hop_length
    cf = max(1, int(chunk_seconds * fps))
    hf = max(1, int(hop_seconds * fps))
    if fp.shape[0] < cf:
        return [(0.0, fp)]
    out: List[Tuple[float, np.ndarray]] = []
    pos = 0
    while pos + cf <= fp.shape[0]:
        out.append((pos / fps, fp[pos:pos + cf]))
        pos += hf
    return out


def _pearson_matrix(vectors: np.ndarray) -> np.ndarray:
    v = vectors.astype(np.float64)
    v -= v.mean(axis=1, keepdims=True)
    norms = np.linalg.norm(v, axis=1, keepdims=True)
    valid = norms.flatten() > CONSTANTS.MIN_FINGERPRINT_STD
    norms[norms < CONSTANTS.MIN_FINGERPRINT_STD] = 1.0
    v /= norms
    corr = v @ v.T
    np.clip(corr, 0.0, 1.0, out=corr)
    np.fill_diagonal(corr, 0.0)
    invalid = ~valid
    corr[invalid, :] = 0.0
    corr[:, invalid] = 0.0
    return corr.astype(np.float32)


def detect_segments_graph(
    fingerprints: Dict[str, np.ndarray], config: Config,
    segment_type: str = "intro",
) -> Tuple[Optional[str], Optional[SegmentResult],
           Dict[str, SegmentResult]]:
    if len(fingerprints) < 2:
        return None, None, {}
    chunk_sec = config["comparison_window"]
    hop_sec = chunk_sec / 2
    sr = config["sample_rate"]
    hl = config["hop_length"]
    threshold = config["similarity_threshold"]
    min_dur = config["min_segment_duration"]
    max_dur = config["max_segment_duration"]
    top_k = CONSTANTS.GRAPH_TOP_K
    paths = list(fingerprints.keys())
    n_eps = len(paths)

    chunks_meta: List[Tuple[int, float]] = []
    flat_rows: List[np.ndarray] = []
    ep_ranges: Dict[int, Tuple[int, int]] = {}
    for ei, path in enumerate(paths):
        ep_chunks = _chunk_fingerprints(
            fingerprints[path], chunk_sec, hop_sec, sr, hl)
        lo = len(chunks_meta)
        for st, cfp in ep_chunks:
            chunks_meta.append((ei, st))
            flat_rows.append(cfp.flatten())
        ep_ranges[ei] = (lo, len(chunks_meta))

    n = len(chunks_meta)
    if n < 2:
        return None, None, {}
    est_mb = n * n * 8 / (1024 * 1024)
    if est_mb > CONSTANTS.GRAPH_MAX_MATRIX_MB:
        return None, None, {}

    vectors = np.array(flat_rows, dtype=np.float64)
    del flat_rows
    corr = _pearson_matrix(vectors)
    del vectors
    for ei in range(n_eps):
        lo, hi = ep_ranges[ei]
        corr[lo:hi, lo:hi] = 0.0
    corr[corr < threshold] = 0.0

    episode_ids = np.array([m[0] for m in chunks_meta], dtype=np.int32)
    degree = np.zeros(n, dtype=np.float64)
    for e in range(n_eps):
        lo, hi = ep_ranges[e]
        if lo >= hi:
            continue
        block = corr[:, lo:hi]
        k = min(top_k, hi - lo)
        if k <= 0:
            continue
        if k == 1:
            ta = block.max(axis=1)
        else:
            ti = np.argpartition(block, -k, axis=1)[:, -k:]
            ta = np.take_along_axis(block, ti, axis=1).mean(axis=1)
        mask = episode_ids != e
        degree[mask] += ta[mask]
    degree /= max(n_eps - 1, 1)

    per_episode: Dict[str, SegmentResult] = {}
    for ei, path in enumerate(paths):
        lo, hi = ep_ranges[ei]
        if lo >= hi:
            continue
        scores = degree[lo:hi]
        starts = np.array([chunks_meta[i][1] for i in range(lo, hi)])
        nc = len(scores)
        min_c = max(1, int(np.ceil((min_dur - chunk_sec) / hop_sec)) + 1)
        max_c = min(
            max(min_c, int(np.floor((max_dur - chunk_sec) / hop_sec)) + 1),
            nc)
        if min_c > nc:
            continue
        best_avg = 0.0
        best_si = 0
        best_len = min_c
        cs = np.concatenate(([0.0], np.cumsum(scores)))
        for length in range(min_c, max_c + 1):
            if length > nc:
                break
            ws = cs[length:nc + 1] - cs[:nc - length + 1]
            wa = ws / length
            idx = int(np.argmax(wa))
            if wa[idx] > best_avg:
                best_avg = float(wa[idx])
                best_si = idx
                best_len = length
        if best_avg > 0.05:
            per_episode[path] = (
                float(starts[best_si]),
                float(starts[best_si + best_len - 1]) + chunk_sec,
                best_avg)

    if not per_episode:
        return None, None, {}
    best_path = max(per_episode, key=lambda p: per_episode[p][2])
    consensus = per_episode[best_path]
    logger.info(
        "  Graph %s: %s–%s (%.2f) %d/%d eps, ref: %s",
        segment_type, format_time(consensus[0]),
        format_time(consensus[1]), consensus[2],
        len(per_episode), n_eps, os.path.basename(best_path))
    return best_path, consensus, per_episode


# ============================================================
# Segment Detection
# ============================================================


def find_common_segments(
    similarities: List[Tuple[str, np.ndarray]], config: Config,
    segment_type: str = "intro",
) -> Optional[SegmentResult]:
    if not similarities:
        return None
    ws = config["comparison_window"]
    threshold = config["similarity_threshold"]
    min_dur = config["min_segment_duration"]
    max_dur = config["max_segment_duration"]
    min_agree = config["min_episodes_agree"]
    n_windows = max(s.shape[0] for _, s in similarities)
    if n_windows == 0:
        return None
    agreement = np.zeros(n_windows, dtype=np.float32)
    best_scores = np.zeros(n_windows, dtype=np.float32)
    coverage = np.zeros(n_windows, dtype=np.float32)
    for _, sm in similarities:
        for i in range(min(sm.shape[0], n_windows)):
            coverage[i] += 1
            ms = float(np.max(sm[i, :])) if sm.shape[1] > 0 else 0.0
            if ms >= threshold:
                agreement[i] += 1
                best_scores[i] = max(best_scores[i], ms)
    safe_cov = np.maximum(coverage, 1.0)
    norm_agr = agreement / safe_cov
    min_frac = min_agree / max(len(similarities), 1)
    agreeing = norm_agr >= min_frac
    if not np.any(agreeing):
        fallback_agree = max(1, min_agree - 1)
        fb = fallback_agree / max(len(similarities), 1)
        logger.warning(
            "No segments met min_agree=%d; retrying with %d",
            min_agree, fallback_agree)
        agreeing = norm_agr >= fb
        if not np.any(agreeing):
            return None
    regions: List[Tuple[int, int]] = []
    start: Optional[int] = None
    for i in range(len(agreeing)):
        if agreeing[i] and start is None:
            start = i
        elif not agreeing[i] and start is not None:
            regions.append((start, i - 1))
            start = None
    if start is not None:
        regions.append((start, len(agreeing) - 1))
    best_region: Optional[SegmentResult] = None
    best_score = 0.0
    for rs, re_ in regions:
        s_sec = rs * ws
        e_sec = (re_ + 1) * ws
        dur = e_sec - s_sec
        if dur < min_dur or dur > max_dur:
            continue
        ac = float(np.mean(best_scores[rs:re_ + 1]))
        aa = float(np.mean(norm_agr[rs:re_ + 1]))
        sc = ac * aa
        if sc > best_score:
            best_score = sc
            best_region = (s_sec, e_sec, ac)
    return best_region


def find_per_episode_segment(
    ref_segment: SegmentResult, ref_fp: np.ndarray,
    target_fp: np.ndarray, config: Config,
    ref_region_duration: Optional[float] = None,
    target_region_duration: Optional[float] = None,
) -> Optional[SegmentResult]:
    if (ref_fp.size == 0 or target_fp.size == 0
            or ref_fp.ndim != 2 or target_fp.ndim != 2):
        return None
    hl = config["hop_length"]
    sr = config["sample_rate"]
    tf = config.get("per_episode_threshold_factor", 0.9)
    threshold = config["similarity_threshold"] * tf
    refine = config.get("refinement_steps", 4)
    fps = sr / hl
    ref_s, ref_e, _ = ref_segment
    seg_dur = ref_e - ref_s
    rsf = int(ref_s * fps)
    rend = min(int(ref_e * fps), ref_fp.shape[0])
    if rsf >= ref_fp.shape[0] or rend <= rsf:
        return None
    seg_frames = rend - rsf
    nb = ref_fp.shape[1]
    ref_flat = ref_fp[rsf:rend].flatten().astype(np.float64)
    seg_len = len(ref_flat)
    if seg_len == 0:
        return None
    r_std = np.std(ref_flat)
    if r_std < CONSTANTS.MIN_FINGERPRINT_STD:
        return None
    r_norm = (ref_flat - np.mean(ref_flat)) / r_std
    if target_fp.shape[0] < seg_frames:
        return None
    t_flat = target_fp.flatten().astype(np.float64)
    t_total = len(t_flat)
    best_corr = 0.0
    best_frame = 0

    if t_total >= seg_len and seg_len >= 64:
        fft_n = 1
        while fft_n < t_total + seg_len - 1:
            fft_n *= 2
        t_std = np.std(t_flat)
        if t_std > CONSTANTS.MIN_FINGERPRINT_STD:
            t_scr = (t_flat - np.mean(t_flat)) / t_std
            # Clip to prevent loud audio spikes from hijacking the FFT
            np.clip(t_scr, -CONSTANTS.FFT_CLIP_SIGMA,
                    CONSTANTS.FFT_CLIP_SIGMA, out=t_scr)
        else:
            t_scr = t_flat.copy()
        rp = np.zeros(fft_n, dtype=np.float64)
        rp[:seg_len] = r_norm
        tp = np.zeros(fft_n, dtype=np.float64)
        tp[:t_total] = t_scr
        raw = np.real(np.fft.irfft(
            np.fft.rfft(tp) * np.conj(np.fft.rfft(rp)), n=fft_n))
        n_valid = t_total - seg_len + 1
        if n_valid <= 0:
            return None
        n_cand = min(CONSTANTS.FFT_CANDIDATE_COUNT, n_valid)
        # If every valid position is a candidate, skip the partition
        # entirely — argpartition against its own length is redundant
        # and triggers undefined behaviour in older NumPy builds when
        # the array contains repeated values (e.g. pure silence).
        if n_cand >= n_valid:
            top_idx = np.arange(n_valid)
        else:
            top_idx = np.argpartition(raw[:n_valid], -n_cand)[-n_cand:]
        seen: Set[int] = set()
        for idx in top_idx:
            fi = int(idx) // nb
            if fi in seen:
                continue
            seen.add(fi)
            ai = fi * nb
            if ai + seg_len > t_total:
                continue
            chunk = t_flat[ai:ai + seg_len]
            cs = np.std(chunk)
            if cs < CONSTANTS.MIN_FINGERPRINT_STD:
                continue
            cn = (chunk - np.mean(chunk)) / cs
            c = float(np.dot(r_norm, cn) / seg_len)
            if c > best_corr:
                best_corr = c
                best_frame = fi
    else:
        wf = max(1, int(config["comparison_window"] * sr / hl))
        step = max(1, wf // max(1, refine))
        n_pos = (target_fp.shape[0] - seg_frames) // step + 1
        for k in range(n_pos):
            j = k * step
            jf = j * nb
            ef = jf + seg_len
            if ef > t_total:
                break
            chunk = t_flat[jf:ef]
            cs = np.std(chunk)
            if cs < CONSTANTS.MIN_FINGERPRINT_STD:
                continue
            cn = (chunk - np.mean(chunk)) / cs
            c = float(np.dot(r_norm, cn) / seg_len)
            if c > best_corr:
                best_corr = c
                best_frame = j

    if best_corr >= threshold:
        return (_snap_timestamp(best_frame / fps),
                _snap_timestamp(best_frame / fps + seg_dur),
                best_corr)
    return None


def extract_fingerprint_for_region(
    video_path: str, start_time: float, duration: float,
    config: Config, cache: FingerprintCache,
    cache_suffix: str, config_hash: str,
    ffmpeg_timeout: Optional[int] = None,
) -> Optional[np.ndarray]:
    if _shutdown_requested:
        return None
    cached = cache.get_fingerprint(video_path, cache_suffix, config_hash)
    if cached is not None:
        fp_flat, sr_, nb_, nf_ = cached
        reshaped = reshape_fingerprint(fp_flat, nb_, nf_)
        if reshaped is not None:
            return reshaped
    try:
        with tempfile.TemporaryDirectory(prefix="intro_skipper_") as tmp:
            tmp_path = os.path.join(tmp, "audio.raw")
            if not extract_audio_segment(
                video_path, start_time, duration, tmp_path,
                config["sample_rate"], timeout=ffmpeg_timeout):
                return None
            audio = load_raw_audio(tmp_path)
            if len(audio) == 0:
                return None
            rms = float(np.sqrt(np.mean(audio ** 2)))
            if rms < CONSTANTS.MIN_AUDIO_RMS:
                logger.warning(
                    "Near-silent audio (RMS=%.2e) for %s at %s–%s "
                    "— skipping fingerprint",
                    rms, os.path.basename(video_path),
                    format_time(start_time),
                    format_time(start_time + duration))
                return None
            nb = config.get("n_bands", 8)
            fsm = config.get("frame_size_multiplier", 4)
            fp = compute_energy_fingerprint(
                audio, config["sample_rate"], config["hop_length"],
                nb, fsm)
            del audio
            cache.store_fingerprint(
                video_path, fp, duration, config["sample_rate"],
                nb, fp.shape[0], cache_suffix, config_hash)
            return fp
    except (MemoryError, SystemExit):
        raise
    except Exception as exc:
        logger.error("FP error for %s: %s", video_path, exc)
        return None


def _extract_fingerprint_worker(
    args_tuple: Tuple,
) -> Tuple[str, Optional[np.ndarray], Optional[float]]:
    (vp, st, rd, cfg, cache, sfx, ch, fft) = args_tuple
    fp = extract_fingerprint_for_region(
        vp, st, rd, cfg, cache, sfx, ch, ffmpeg_timeout=fft)
    return vp, fp, rd


# ============================================================
# Main detection pipeline
# ============================================================


def _log_memory_usage(ifp: Dict, ofp: Dict):
    total = sum(f.nbytes for f in ifp.values())
    total += sum(f.nbytes for f in ofp.values())
    mb = total / (1024 * 1024)
    if mb > CONSTANTS.MEMORY_WARNING_MB:
        logger.warning("Fingerprints using %.0f MB", mb)


def _detect_with_fallback(
    fps: Dict[str, np.ndarray], episodes: List[Dict[str, Any]],
    config: Config, seg_type: str, use_graph: bool,
) -> Tuple[Optional[str], Optional[np.ndarray],
           Optional[SegmentResult], Dict[str, SegmentResult]]:
    fp_paths = list(fps.keys())
    ref_path: Optional[str] = None
    ref_fp: Optional[np.ndarray] = None
    segment: Optional[SegmentResult] = None
    graph_res: Dict[str, SegmentResult] = {}
    if len(fp_paths) < 2:
        return None, None, None, {}
    if use_graph:
        ref_path, segment, graph_res = detect_segments_graph(
            fps, config, seg_type)
        if ref_path is not None:
            ref_fp = fps[ref_path]
    if segment is None:
        sims: List[Tuple[str, np.ndarray]] = []
        ref_path = select_reference_episode(episodes, fps)
        if ref_path:
            ref_fp = fps[ref_path]
            others = [p for p in fp_paths if p != ref_path]
            for op in others:
                if _shutdown_requested:
                    break
                sim = compare_fingerprints(
                    ref_fp, fps[op], config["comparison_window"],
                    config["hop_length"], config["sample_rate"])
                sims.append((op, sim))
            segment = find_common_segments(sims, config, seg_type)
        graph_res = {}
    return ref_path, ref_fp, segment, graph_res


def _refine_and_store(
    segment: SegmentResult, ref_path: str, ref_fp: np.ndarray,
    all_fps: Dict[str, np.ndarray], region_durs: Dict[str, float],
    config: Config, cache: FingerprintCache, seg_type: str,
    ref_offset: float,
    ep_offsets: Optional[Dict[str, float]],
    graph_res: Dict[str, SegmentResult],
    results: DetectionResults, dur_map: DurationMap,
):
    rs = _snap_timestamp(segment[0] + ref_offset)
    re_ = _snap_timestamp(segment[1] + ref_offset)
    rc = segment[2]
    results.setdefault(ref_path, {})[seg_type] = (rs, re_)
    cache.store_skip_segment(ref_path, seg_type, rs, re_, rc,
                             "fingerprint")
    logger.info("  %s: %s %s–%s (%.2f) [ref]",
                os.path.basename(ref_path), seg_type,
                format_time(rs), format_time(re_), rc)

    for op in [p for p in all_fps if p != ref_path]:
        if _shutdown_requested:
            break
        epo = (ep_offsets.get(op, ref_offset)
               if ep_offsets is not None else ref_offset)
        per_ep = find_per_episode_segment(
            segment, ref_fp, all_fps[op], config,
            region_durs.get(ref_path), region_durs.get(op))
        if per_ep:
            es = _snap_timestamp(per_ep[0] + epo)
            ee = _snap_timestamp(per_ep[1] + epo)
            results.setdefault(op, {})[seg_type] = (es, ee)
            cache.store_skip_segment(op, seg_type, es, ee,
                                     per_ep[2], "fingerprint")
        elif op in graph_res:
            gi = graph_res[op]
            gs = _snap_timestamp(gi[0] + epo)
            ge = _snap_timestamp(gi[1] + epo)
            gc_ = gi[2] * 0.7
            results.setdefault(op, {})[seg_type] = (gs, ge)
            cache.store_skip_segment(op, seg_type, gs, ge, gc_,
                                     "graph-fallback")
        elif seg_type == "intro":
            results.setdefault(op, {})[seg_type] = (rs, re_)
            cache.store_skip_segment(op, seg_type, rs, re_,
                                     rc * 0.5, "fingerprint-fallback")
        elif seg_type == "outro":
            ep_dur = dur_map.get(op)
            ref_dur = dur_map.get(ref_path)
            if ep_dur is not None and ref_dur is not None:
                # Anchor on the outro END, not the start, so the
                # fallback start doesn't drift by the full outro length.
                dist_from_end = ref_dur - re_
                slen = re_ - rs
                fb_e = _snap_timestamp(ep_dur - dist_from_end)
                fb_s = _snap_timestamp(fb_e - slen)
                if 0 <= fb_s < fb_e <= ep_dur:
                    results.setdefault(op, {})[seg_type] = (fb_s, fb_e)
                    cache.store_skip_segment(
                        op, seg_type, fb_s, fb_e,
                        rc * 0.5, "fingerprint-fallback")


def _extract_parallel(
    tasks: List[Tuple], label: str, parallel: bool,
) -> List[Tuple[str, Optional[np.ndarray], Optional[float]]]:
    out: List[Tuple[str, Optional[np.ndarray], Optional[float]]] = []
    if parallel and len(tasks) > 1:
        nw = min(CONSTANTS.EXTRACTION_WORKERS, len(tasks))
        prog = ProgressTracker(len(tasks), f"{label} fingerprints")
        done = 0
        with ThreadPoolExecutor(max_workers=nw) as pool:
            futs = {pool.submit(_extract_fingerprint_worker, t): t
                    for t in tasks}
            for f in as_completed(futs):
                if _shutdown_requested:
                    for ff in futs:
                        ff.cancel()
                    break
                done += 1
                path, fp, rdur = f.result()
                prog.update(done, os.path.basename(path))
                out.append((path, fp, rdur))
    else:
        prog = ProgressTracker(len(tasks), f"{label} fingerprints")
        for idx, task in enumerate(tasks, 1):
            if _shutdown_requested:
                break
            path, fp, rdur = _extract_fingerprint_worker(task)
            prog.update(idx, os.path.basename(path))
            out.append((path, fp, rdur))
    return out


def detect_segments(
    video_paths: List[str], config: Config,
    cache: FingerprintCache, force: bool = False,
    parallel_extract: bool = True,
    max_fingerprint_mb: Optional[float] = None,
    ffmpeg_timeout: Optional[int] = None,
) -> Tuple[DetectionResults, DurationMap]:
    if len(video_paths) < 2:
        logger.warning("Need ≥ 2 episodes.")
        return {}, {}
    use_graph = config.get("use_graph_consensus", True)
    intro_start = config.get("intro_search_start", 0)
    intro_end = config["intro_search_end"]
    cfg_hash = compute_config_hash(config)
    if ffmpeg_timeout is None:
        ffmpeg_timeout = CONSTANTS.DEFAULT_FFMPEG_TIMEOUT

    # 1. Gather durations
    episodes: List[Dict[str, Any]] = []
    dur_map: DurationMap = {}
    for vp in video_paths:
        if _shutdown_requested:
            return {}, {}
        dur = get_duration(vp)
        if dur is not None and dur >= CONSTANTS.MIN_EPISODE_DURATION:
            episodes.append({"path": vp, "duration": dur})
            dur_map[vp] = dur
    if len(episodes) < 2:
        return {}, dur_map

    # 2. Memory budget
    if max_fingerprint_mb is None:
        max_fingerprint_mb = float(CONSTANTS.DEFAULT_MAX_FINGERPRINT_MB)
    est = _estimate_fingerprint_mb(episodes, config)
    if est > max_fingerprint_mb:
        config = config.copy()
        config["use_graph_consensus"] = False
        use_graph = False
        per_ep = est / len(episodes) if episodes else 1
        safe = max(2, int(max_fingerprint_mb / per_ep))
        if len(episodes) > safe:
            return detect_segments_batched(
                [e["path"] for e in episodes], config, cache, safe,
                force=force, parallel_extract=parallel_extract,
                max_fingerprint_mb=max_fingerprint_mb,
                ffmpeg_timeout=ffmpeg_timeout)

    # 3. Check cache
    if not force:
        cached: DetectionResults = {}
        all_cached = True
        for ep in episodes:
            segs = cache.get_skip_segments(ep["path"])
            if segs:
                cached[ep["path"]] = {
                    st: (s, e) for st, (s, e, _) in segs.items()}
            else:
                all_cached = False
        if all_cached and cached:
            logger.info("All cached. Use --force to redo.")
            return cached, dur_map

    # 4. Intro fingerprints — always freed in finally to prevent leaks
    results: DetectionResults = {}
    intro_fps: Dict[str, np.ndarray] = {}
    intro_durs: Dict[str, float] = {}
    try:
        tasks_i: List[Tuple] = []
        for ep in episodes:
            if _shutdown_requested:
                return {}, dur_map
            se = min(intro_end, ep["duration"] * 0.5)
            if intro_start >= se:
                continue
            sd = se - intro_start
            sfx = f":intro:{int(intro_start)}-{int(se)}"
            tasks_i.append((ep["path"], intro_start, sd, config, cache,
                            sfx, cfg_hash, ffmpeg_timeout))
        for path, fp, rdur in _extract_parallel(
                tasks_i, "Intro", parallel_extract):
            if fp is not None and rdur is not None:
                intro_fps[path] = fp
                intro_durs[path] = rdur
        cache.batch_commit()

        (irp, irf, iseg, igr) = _detect_with_fallback(
            intro_fps, episodes, config, "intro", use_graph)
        ioff = config.get("intro_search_start", 0)
        if iseg and irf is not None and irp:
            ioffs = {e["path"]: ioff for e in episodes}
            _refine_and_store(
                iseg, irp, irf, intro_fps, intro_durs, config, cache,
                "intro", ioff, ioffs, igr, results, dur_map)
    finally:
        del intro_fps
        gc.collect()

    # 5. Outro fingerprints
    outro_fps: Dict[str, np.ndarray] = {}
    outro_offs: Dict[str, float] = {}
    outro_durs: Dict[str, float] = {}
    try:
        tasks_o: List[Tuple] = []
        for ep in episodes:
            if _shutdown_requested:
                return {}, dur_map
            ss = max(0.0, ep["duration"] - config["outro_search_duration"])
            sd = ep["duration"] - ss
            sfx = f":outro:{int(ss)}-{int(ss + sd)}"
            tasks_o.append((ep["path"], ss, sd, config, cache,
                            sfx, cfg_hash, ffmpeg_timeout))
            outro_offs[ep["path"]] = ss
        for path, fp, rdur in _extract_parallel(
                tasks_o, "Outro", parallel_extract):
            if fp is not None and rdur is not None:
                outro_fps[path] = fp
                outro_durs[path] = rdur
        cache.batch_commit()

        _log_memory_usage(outro_fps, outro_fps)

        (orp, orf, oseg, ogr) = _detect_with_fallback(
            outro_fps, episodes, config, "outro", use_graph)
        if oseg and orf is not None and orp:
            ooff = outro_offs.get(orp, 0.0)
            _refine_and_store(
                oseg, orp, orf, outro_fps, outro_durs, config, cache,
                "outro", ooff, outro_offs, ogr, results, dur_map)
    finally:
        del outro_fps
        gc.collect()

    # 6. Validate all segments against episode durations
    validated: DetectionResults = {}
    for fp_, segs in results.items():
        d = dur_map.get(fp_)
        if d is not None:
            v = validate_segments(segs, d)
            if v:
                validated[fp_] = v
        elif segs:
            validated[fp_] = segs
    return validated, dur_map


def detect_segments_batched(
    video_paths: List[str], config: Config,
    cache: FingerprintCache,
    batch_size: Optional[int] = None, force: bool = False,
    parallel_extract: bool = True,
    max_fingerprint_mb: Optional[float] = None,
    ffmpeg_timeout: Optional[int] = None,
) -> Tuple[DetectionResults, DurationMap]:
    if batch_size is None:
        batch_size = CONSTANTS.BATCH_SIZE
    if len(video_paths) <= batch_size:
        return detect_segments(
            video_paths, config, cache, force=force,
            parallel_extract=parallel_extract,
            max_fingerprint_mb=max_fingerprint_mb,
            ffmpeg_timeout=ffmpeg_timeout)
    all_res: DetectionResults = {}
    all_dur: DurationMap = {}
    valid: List[str] = []
    pre_dur: Dict[str, float] = {}
    for vp in video_paths:
        if _shutdown_requested:
            return {}, {}
        d = get_duration(vp)
        if d is not None and d >= CONSTANTS.MIN_EPISODE_DURATION:
            valid.append(vp)
            pre_dur[vp] = d
    if len(valid) < 2:
        return detect_segments(
            video_paths, config, cache, force=force,
            parallel_extract=parallel_extract,
            max_fingerprint_mb=max_fingerprint_mb,
            ffmpeg_timeout=ffmpeg_timeout)
    med = float(np.median(list(pre_dur.values())))
    ref = min(valid, key=lambda p: abs(pre_dur[p] - med))
    remaining = [p for p in valid if p != ref]
    eff = batch_size - 1
    ref_done = False
    for i in range(0, len(remaining), eff):
        if _shutdown_requested:
            break
        batch = [ref] + remaining[i:i + eff]
        br, bd = detect_segments(
            batch, config, cache, force=force,
            parallel_extract=parallel_extract,
            max_fingerprint_mb=max_fingerprint_mb,
            ffmpeg_timeout=ffmpeg_timeout)
        if not ref_done:
            all_res.update(br)
            ref_done = True
        else:
            # Preserve the reference episode's first-batch result.
            all_res.update({k: v for k, v in br.items() if k != ref})
        all_dur.update(bd)
        gc.collect()
    return all_res, all_dur


# ============================================================
# Import
# ============================================================


def match_episode_key(
    video_name: str, episode_keys: List[str],
) -> Optional[str]:
    vl = video_name.lower()
    vn = get_skip_key(video_name)
    vtag = _parse_episode_tag(vl)
    if vtag:
        for key in episode_keys:
            kt = _parse_episode_tag(key.lower())
            if kt and kt == vtag:
                return key
    for key in sorted(episode_keys, key=len, reverse=True):
        if len(key) >= 3:
            kn = get_skip_key(key)
            if kn in vn or vn in kn:
                return key
    if vtag:
        for key in episode_keys:
            kt = _parse_episode_tag(key)
            if kt and kt == vtag:
                return key
    return None


def _validate_ts(s: float, e: float, label: str, fn: str) -> bool:
    return not (s < 0 or e <= s)


def import_timestamps_file(
    filepath: str, video_dir: str, cache: FingerprintCache,
) -> DetectionResults:
    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError,
            PermissionError, OSError) as exc:
        logger.error("Error reading %s: %s", filepath, exc)
        return {}
    vdir = Path(video_dir)
    try:
        videos = sorted(
            v for v in vdir.iterdir()
            if v.suffix.lower() in VIDEO_EXTENSIONS)
    except (PermissionError, OSError):
        return {}
    results: DetectionResults = {}
    if "intro_end" in data or "intro_duration" in data:
        i_s = float(data.get("intro_start", 0))
        i_e = float(data.get("intro_end", data.get("intro_duration", 0)))
        o_dur = float(data.get("outro_duration", 0))
        for video in videos:
            vp = str(video)
            results[vp] = {}
            if i_e > 0 and _validate_ts(i_s, i_e, "intro", video.name):
                results[vp]["intro"] = (
                    _snap_timestamp(i_s), _snap_timestamp(i_e))
                cache.store_skip_segment(
                    vp, "intro", i_s, i_e, 1.0, "manual")
            if o_dur > 0:
                dur = get_duration(vp)
                if dur and dur > o_dur:
                    os_ = _snap_timestamp(dur - o_dur)
                    results[vp]["outro"] = (os_, _snap_timestamp(dur))
                    cache.store_skip_segment(
                        vp, "outro", os_, dur, 1.0, "manual")
        return results
    if "episodes" in data:
        ep_data = data["episodes"]
        ep_keys = list(ep_data.keys())
        for video in videos:
            vp = str(video)
            mk = match_episode_key(video.stem, ep_keys)
            if mk is None:
                continue
            ep = ep_data[mk]
            results[vp] = {}
            if "intro_start" in ep and "intro_end" in ep:
                if _validate_ts(ep["intro_start"], ep["intro_end"],
                                "intro", video.stem):
                    results[vp]["intro"] = (
                        _snap_timestamp(float(ep["intro_start"])),
                        _snap_timestamp(float(ep["intro_end"])))
                    cache.store_skip_segment(
                        vp, "intro", ep["intro_start"],
                        ep["intro_end"], 1.0, "database")
            if "outro_start" in ep:
                o_end = ep.get("outro_end") or get_duration(vp)
                if o_end and _validate_ts(
                        ep["outro_start"], o_end, "outro", video.stem):
                    results[vp]["outro"] = (
                        _snap_timestamp(float(ep["outro_start"])),
                        _snap_timestamp(float(o_end)))
                    cache.store_skip_segment(
                        vp, "outro", ep["outro_start"],
                        o_end, 1.0, "database")
    return results


# ============================================================
# VLC Integration
# ============================================================


def _collect_videos_recursive(video_dir: Path) -> List[str]:
    out: List[str] = []
    try:
        for item in sorted(video_dir.rglob("*")):
            if item.is_file() and item.suffix.lower() in VIDEO_EXTENSIONS:
                out.append(str(item))
    except (PermissionError, OSError):
        pass
    return out


def generate_skip_data(
    results: DetectionResults, output_path: str = SKIP_DATA_FILE,
    durations: Optional[DurationMap] = None,
    use_full_path: bool = False,
) -> str:
    """Write skip-data JSON, merging with any file already on disk.

    Keys are normalized filename stems produced by ``get_skip_key`` so
    that Python (write) and Lua (read) always agree.  Any legacy raw
    keys present in an older file are migrated to normalized form on the
    first merge, preventing duplicate entries from accumulating across
    runs.
    """
    skip: Dict[str, Dict[str, float]] = {}
    if os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, dict):
                for old_key, val in existing.items():
                    # Migrate any raw-filename keys from older versions
                    # by re-normalizing them through the current function.
                    norm_key = (old_key if use_full_path
                                else get_skip_key(old_key))
                    skip[norm_key] = val
        except (json.JSONDecodeError, OSError):
            logger.warning(
                "Existing skip data at %s is corrupted — overwriting",
                output_path)

    for fpath, segs in results.items():
        key = get_skip_key(fpath, use_full_path)
        entry: Dict[str, float] = {}
        if "intro" in segs:
            entry["intro_start"] = _snap_timestamp(segs["intro"][0])
            entry["intro_end"] = _snap_timestamp(segs["intro"][1])
        if "outro" in segs:
            entry["outro_start"] = _snap_timestamp(segs["outro"][0])
            entry["outro_end"] = _snap_timestamp(segs["outro"][1])
        if entry:
            skip[key] = entry

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(skip, f, indent=2, ensure_ascii=False)
    logger.info("Skip data written to %s (%d entries)",
                output_path, len(skip))
    return output_path


def generate_vlc_conf(
    results: DetectionResults, output_path: Optional[str] = None,
    durations: Optional[DurationMap] = None,
):
    if output_path is None:
        output_path = os.path.expanduser(
            "~/.config/vlc/intro-skipper.conf")
    if durations is None:
        durations = {}
    conf_dir = os.path.dirname(output_path)
    if conf_dir:
        os.makedirs(conf_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for fpath, segs in results.items():
            name = os.path.basename(fpath)
            ie = segs.get("intro", (0.0, 0.0))[1]
            od = 0
            if "outro" in segs:
                dur = durations.get(fpath)
                if dur is None and os.path.isfile(fpath):
                    dur = get_duration(fpath)
                if dur and dur > 0:
                    od = round(dur - segs["outro"][0])
                else:
                    od = round(segs["outro"][1] - segs["outro"][0])
            f.write(f"{name}={round(ie)},{od}\n")
    logger.info("VLC conf written to %s", output_path)


def _lua_json_parser() -> str:
    return r'''
function parse_json_string(str, pos)
    if str:sub(pos, pos) ~= '"' then return nil, pos end
    pos = pos + 1
    local result = {}
    while pos <= #str do
        local c = str:sub(pos, pos)
        if c == '"' then return table.concat(result), pos + 1
        elseif c == '\\' then
            pos = pos + 1; local esc = str:sub(pos, pos)
            if     esc == 'n'  then table.insert(result, '\n')
            elseif esc == 'r'  then table.insert(result, '\r')
            elseif esc == 't'  then table.insert(result, '\t')
            elseif esc == '\\' then table.insert(result, '\\')
            elseif esc == '"'  then table.insert(result, '"')
            elseif esc == '/'  then table.insert(result, '/')
            elseif esc == 'u'  then
                local hex = str:sub(pos+1, pos+4)
                local cp = tonumber(hex, 16)
                if cp then
                    if cp < 128 then table.insert(result, string.char(cp))
                    elseif cp < 0x800 then
                        table.insert(result, string.char(
                            0xC0+math.floor(cp/64), 0x80+(cp%64)))
                    else table.insert(result, string.char(
                            0xE0+math.floor(cp/4096),
                            0x80+(math.floor(cp/64)%64), 0x80+(cp%64)))
                    end; pos = pos + 4
                end
            else table.insert(result, esc) end
        else table.insert(result, c) end
        pos = pos + 1
    end; return nil, pos
end
function parse_json_number(str, pos)
    local s = pos
    if str:sub(pos,pos) == '-' then pos = pos+1 end
    while pos <= #str and str:sub(pos,pos):match('[0-9]') do pos=pos+1 end
    if pos <= #str and str:sub(pos,pos) == '.' then pos=pos+1
        while pos <= #str and str:sub(pos,pos):match('[0-9]') do pos=pos+1 end end
    if pos <= #str and str:sub(pos,pos):match('[eE]') then pos=pos+1
        if str:sub(pos,pos):match('[+-]') then pos=pos+1 end
        while pos <= #str and str:sub(pos,pos):match('[0-9]') do pos=pos+1 end end
    return tonumber(str:sub(s, pos-1)), pos
end
function skip_ws(str, pos)
    while pos <= #str and str:sub(pos,pos):match('%s') do pos=pos+1 end
    return pos
end
function parse_json_object(content)
    local data = {}; local pos = skip_ws(content, 1)
    if content:sub(pos,pos) ~= '{' then return data end; pos = pos+1
    local first = true
    while pos <= #content do
        pos = skip_ws(content, pos)
        if content:sub(pos,pos) == '}' then break end
        if not first then
            if content:sub(pos,pos) == ',' then pos=pos+1; pos=skip_ws(content,pos)
            else break end end; first = false
        local fname, np = parse_json_string(content, pos)
        if not fname then break end; pos = skip_ws(content, np)
        if content:sub(pos,pos) ~= ':' then break end; pos = skip_ws(content, pos+1)
        if content:sub(pos,pos) ~= '{' then break end; pos = pos+1
        local entry = {}; local fi = true
        while pos <= #content do
            pos = skip_ws(content, pos)
            if content:sub(pos,pos) == '}' then pos=pos+1; break end
            if not fi then
                if content:sub(pos,pos) == ',' then pos=pos+1; pos=skip_ws(content,pos)
                else break end end; fi = false
            local key, np2 = parse_json_string(content, pos)
            if not key then break end; pos = skip_ws(content, np2)
            if content:sub(pos,pos) ~= ':' then break end; pos = skip_ws(content, pos+1)
            local val, np3 = parse_json_number(content, pos)
            if val then entry[key] = val; pos = np3 else break end
        end
        if fname and next(entry) then data[fname] = entry end
    end; return data
end
'''


def _lua_helpers() -> str:
    # NOTE ON UTF-8 IN LUA CHARACTER CLASSES
    # Lua's string library is byte-oriented; character class brackets
    # such as [...] treat each byte independently.  Placing a multibyte
    # UTF-8 sequence (e.g. en-dash = E2 80 93) inside [...] matches ANY
    # of those individual bytes, corrupting unrelated characters that
    # share one of those byte values (e.g. the Euro sign € = E2 82 AC
    # shares the leading E2 byte).
    #
    # Fix: substitute the multibyte sequences with explicit chained
    # gsub calls BEFORE handling single-byte ASCII separators.
    return r'''
function format_time(seconds)
    if not seconds then return "?" end
    if seconds >= 3600 then
        return string.format("%d:%02d:%02d", math.floor(seconds/3600),
            math.floor((seconds%3600)/60), math.floor(seconds%60))
    end
    return string.format("%d:%02d", math.floor(seconds/60),
        math.floor(seconds%60))
end

-- Produce the same key that Python's get_skip_key() writes.
-- Steps must match exactly:
--   1. percent-decode URI escapes
--   2. strip extension
--   3. lower-case
--   4. multibyte dash sequences → "."  (must be done BEFORE bracket classes)
--   5. remaining single-byte separators → "."
--   6. strip non-alphanumeric / non-dot bytes
--   7. collapse repeated dots
--   8. trim leading / trailing dots
function normalize_key(name)
    if not name then return "" end

    -- 1. percent-decode
    name = name:gsub("%%(%x%x)", function(h)
        return string.char(tonumber(h, 16))
    end)

    -- 2. strip extension (last dot-segment)
    name = name:gsub("%.[^%.]+$", "")

    -- 3. lower-case
    name = name:lower()

    -- 4. multibyte UTF-8 dashes — substituted individually to avoid
    --    corrupting other characters that share the same lead byte.
    --    en-dash U+2013 = bytes E2 80 93
    name = name:gsub("\xe2\x80\x93", ".")
    --    em-dash U+2014 = bytes E2 80 94
    name = name:gsub("\xe2\x80\x94", ".")

    -- 5. single-byte ASCII separators
    name = name:gsub("[ \t_%-.,:]+", ".")

    -- 6. remove everything that is not a-z, 0-9, or dot
    name = name:gsub("[^a-z0-9.]+", "")

    -- 7. collapse duplicate dots
    name = name:gsub("%.%.+", ".")

    -- 8. trim
    name = name:gsub("^%.+", ""):gsub("%.+$", "")

    return name
end

function find_skip_data(fn)
    if not fn then return nil end
    local nfn = normalize_key(fn)
    if nfn == "" then return nil end

    -- Primary: normalized key (exact match with Python output)
    if skip_data[nfn] then return skip_data[nfn] end

    -- Secondary: raw filename (use_full_path mode / legacy entries)
    if skip_data[fn] then return skip_data[fn] end

    -- Last resort: normalize every stored key and compare
    for key, val in pairs(skip_data) do
        if normalize_key(key) == nfn then return val end
    end
    return nil
end

function has_next_item()
    local pok, plist = pcall(function()
        return vlc.playlist.get("normal")
    end)
    if not pok or not plist then return false end
    if plist and plist.children then return #plist.children > 1 end
    return false
end
'''


def install_vlc_extension(silent: bool = False) -> str:
    escaped = escape_lua_string(SKIP_DATA_FILE)
    jp = _lua_json_parser()
    helpers = _lua_helpers()
    delay_us = CONSTANTS.VLC_SEEK_DELAY_MS * 1000

    # ------------------------------------------------------------------
    # Extension dialog — shares the VLC GUI thread.
    # NO mwait/msleep here: a synchronous sleep freezes the entire
    # player window for its duration.  The slight positional stutter of
    # an immediate seek is far preferable to a frozen UI.
    # ------------------------------------------------------------------
    lua_ext = f'''--[[
Intro/Outro Skipper for VLC  (extension dialog)
Version {__version__}
]]
function descriptor()
    return {{title="Intro/Outro Skipper",version="{__version__}",
        author="Intro Skipper",shortdesc="Skip Intro/Outro",
        description="Skip intro and outro segments.",
        capabilities={{"input-listener"}}}}
end
local SKIP_DATA_FILE = "{escaped}"
local skip_data = {{}}
local intro_skipped = false
local outro_skipped = false
local auto_skip_enabled = true
local dialog, status_label, file_label, info_label = nil
local manual_intro_input, manual_outro_input = nil
{jp}
{helpers}
function activate()
    load_skip_data(); open_dialog()
end
function deactivate()
    if dialog then dialog:delete(); dialog=nil end
    status_label=nil; file_label=nil; info_label=nil
end
function close() vlc.deactivate() end
function meta_changed() end
function input_changed()
    intro_skipped=false; outro_skipped=false; update_dialog_info()
end
function load_skip_data()
    skip_data={{}}
    local f=io.open(SKIP_DATA_FILE,"r")
    if not f then return end
    local c=f:read("*all"); f:close()
    skip_data=parse_json_object(c)
end
function count_entries()
    local c=0; for _ in pairs(skip_data) do c=c+1 end; return c
end
function get_current_filename()
    local item=vlc.input.item(); if not item then return nil end
    local uri=item:uri(); if not uri then return nil end
    local fn=uri:match("([^/\\\\]+)$")
    if fn then
        fn=fn:gsub("%%(%x%x)",function(h)
            return string.char(tonumber(h,16)) end)
    end
    return fn
end
-- No sleep here — extension runs on the GUI thread.
function seek_to(s)
    local input=vlc.object.input(); if not input then return false end
    return pcall(function()
        vlc.var.set(input,"time",math.floor(s*1000000)) end)
end
function do_skip_intro()
    local fn=get_current_filename(); if not fn then return false,"No file" end
    local d=find_skip_data(fn); if not d then return false,"No data" end
    if not d.intro_end then return false,"No intro" end
    if seek_to(d.intro_end) then intro_skipped=true; return true,"Skipped" end
    return false,"Failed"
end
function do_skip_outro()
    local fn=get_current_filename(); if not fn then return false,"No file" end
    local d=find_skip_data(fn); if not d then return false,"No data" end
    if not d.outro_start then return false,"No outro" end
    -- Safe next(): inspect playlist size before advancing.
    local advanced=false
    local pl=vlc.playlist
    local pok,items=pcall(function()
        return pl.get and pl:get("normal") end)
    local cok,cur=pcall(function()
        return pl.current and pl:current() end)
    if pok and items and items.children
            and cok and cur
            and #items.children > 1 then
        advanced=pcall(function() pl:next() end)
    end
    if advanced then outro_skipped=true; return true,"Next" end
    -- Last episode: seek to outro end or item duration.
    if d.outro_end then
        if seek_to(d.outro_end) then outro_skipped=true; return true,"End" end
    end
    local item=vlc.input.item()
    if item then
        local dur=item:duration()
        if dur and dur>0 then
            if seek_to(dur/1000000) then
                outro_skipped=true; return true,"End"
            end
        end
    end
    return false,"Failed"
end
function update_dialog_info()
    if not dialog then return end
    local fn=get_current_filename() or "Nothing playing"
    if file_label then file_label:set_text(fn) end
    local d=find_skip_data(fn); local info=""
    if d then
        if d.intro_start and d.intro_end then
            info=info.."Intro: "..format_time(d.intro_start)
                .." - "..format_time(d.intro_end).."<br>"
        end
        if d.outro_start then
            info=info.."Outro: "..format_time(d.outro_start)
            if d.outro_end then
                info=info.." - "..format_time(d.outro_end)
            end
        end
    else info="No skip data" end
    info=info.."<br><i>Auto-skip: "
        ..(auto_skip_enabled and "ON" or "OFF").."</i>"
    if info_label then info_label:set_text(info) end
end
function open_dialog()
    if dialog then
        dialog:delete(); dialog=nil
        status_label=nil; file_label=nil; info_label=nil
    end
    dialog=vlc.dialog(descriptor().title)
    dialog:add_label(
        "<center><h3>Intro/Outro Skipper</h3></center>",1,1,2,1)
    status_label=dialog:add_label(
        "Loaded "..count_entries().." entries",1,2,2,1)
    dialog:add_button("Reload",function()
        load_skip_data()
        if status_label then
            status_label:set_text("Reloaded: "..count_entries())
        end
        update_dialog_info() end,1,3,1,1)
    dialog:add_button("Skip Intro",function()
        local ok,msg=do_skip_intro()
        if status_label then status_label:set_text(msg) end
        end,2,3,1,1)
    dialog:add_label("<center><b>Current:</b></center>",1,4,2,1)
    file_label=dialog:add_label(
        get_current_filename() or "Nothing",1,5,2,1)
    info_label=dialog:add_label("",1,6,2,1)
    update_dialog_info()
    dialog:add_button("Skip Outro",function()
        local ok,msg=do_skip_outro()
        if status_label then status_label:set_text(msg) end
        end,1,7,1,1)
    dialog:add_button("Toggle Auto",function()
        auto_skip_enabled=not auto_skip_enabled
        if status_label then
            status_label:set_text(
                "Auto: "..(auto_skip_enabled and "ON" or "OFF"))
        end
        update_dialog_info() end,2,7,1,1)
    dialog:add_label("<center><b>Manual</b></center>",1,8,2,1)
    dialog:add_label("Intro end (s):",1,9,1,1)
    manual_intro_input=dialog:add_text_input("",2,9,1,1)
    dialog:add_label("Outro start (s):",1,10,1,1)
    manual_outro_input=dialog:add_text_input("",2,10,1,1)
    dialog:add_button("Apply",function()
        local fn=get_current_filename()
        if not fn then
            if status_label then status_label:set_text("No file") end
            return
        end
        local iv=tonumber(manual_intro_input:get_text())
        local ov=tonumber(manual_outro_input:get_text())
        if iv or ov then
            if not skip_data[fn] then skip_data[fn]={{}} end
            if iv then
                skip_data[fn].intro_start=0
                skip_data[fn].intro_end=iv
            end
            if ov then
                skip_data[fn].outro_start=ov
                local item=vlc.input.item()
                if item then
                    local dur=item:duration()
                    if dur and dur>0 then
                        skip_data[fn].outro_end=dur/1000000
                    end
                end
            end
            intro_skipped=false; outro_skipped=false
            if status_label then
                status_label:set_text("Applied (session)")
            end
            update_dialog_info()
        else
            if status_label then
                status_label:set_text("Enter numbers")
            end
        end
    end,1,11,2,1)
    dialog:add_button("Refresh",function()
        update_dialog_info() end,1,12,1,1)
    dialog:add_button("Close",close,2,12,1,1)
end
'''

    # ------------------------------------------------------------------
    # Background interface script — its own isolated event loop.
    # mwait IS safe here because this script never touches the GUI.
    # seek_and_play() waits for VLC to settle after each seek and
    # explicitly unpauses if VLC entered a paused state post-seek.
    # safe_skip_outro() checks playlist length before calling next()
    # so it never wraps around on the last episode.
    # ------------------------------------------------------------------
    intf = f'''--[[
Intro Skipper Interface Script  (background auto-skipper)
Version {__version__}
Load via:  vlc --extraintf lua --lua-intf intro_skipper_intf
]]
local SKIP_DATA_FILE="{escaped}"
local skip_data={{}}
local intro_skipped=false
local outro_skipped=false
local last_filename=""
local check_count=0
local running=true
local AUTO_SKIP_GRACE_SEC=5
local POLL_ACTIVE_US=250000
local POLL_IDLE_US=2000000
local SEEK_SETTLE_US={delay_us}
{jp}
{helpers}
function load_data()
    skip_data={{}}
    local f=io.open(SKIP_DATA_FILE,"r")
    if not f then return end
    local c=f:read("*all"); f:close()
    skip_data=parse_json_object(c)
end
function get_fn(uri)
    if not uri then return nil end
    local fn=uri:match("([^/\\\\]+)$")
    if fn then
        fn=fn:gsub("%%(%x%x)",function(h)
            return string.char(tonumber(h,16)) end)
    end
    return fn
end
-- Seek then wait for VLC to settle; unpause if needed.
-- Safe to call only from the background intf loop (never GUI thread).
function seek_and_play(target_s, input)
    local ok=pcall(function()
        vlc.var.set(input,"time",math.floor(target_s*1000000)) end)
    if ok and vlc.misc and vlc.misc.mwait and vlc.misc.mdate then
        vlc.misc.mwait(vlc.misc.mdate()+SEEK_SETTLE_US)
    end
    pcall(function()
        -- state 3 = paused in VLC; 1 = playing
        if vlc.var.get(input,"state")==3 then
            vlc.var.set(input,"state",1)
        end
    end)
    return ok
end
-- Advance playlist only when there is a next item.
-- Falls back to seeking to outro_end or item duration on the last episode.
function safe_skip_outro(input, d, item)
    local advanced=false
    local pok,pl=pcall(function() return vlc.playlist end)
    if pok and pl then
        local iok,items=pcall(function()
            return pl.get and pl:get("normal") end)
        local cok,cur=pcall(function()
            return pl.current and pl:current() end)
        if iok and items and items.children
                and cok and cur
                and #items.children > 1 then
            advanced=pcall(function() pl:next() end)
        end
    end
    if advanced then return end
    if d.outro_end then
        seek_and_play(d.outro_end, input)
    else
        local dok,dur=pcall(function() return item:duration() end)
        if dok and dur and dur>0 then
            seek_and_play(dur/1000000, input)
        end
    end
end
load_data()
while running do
    local ok,err=pcall(function()
        if not vlc or not vlc.misc then running=false; return end
        local playing=false
        if vlc.input and vlc.object then
            local iok,item=pcall(function() return vlc.input.item() end)
            if iok and item then
                local uok,uri=pcall(function() return item:uri() end)
                if uok and uri then
                    local fn=get_fn(uri)
                    if fn then
                        playing=true
                        if fn ~= last_filename then
                            intro_skipped=false
                            outro_skipped=false
                            last_filename=fn
                        end
                        local d=find_skip_data(fn)
                        if d then
                            local inok,input=pcall(function()
                                return vlc.object.input() end)
                            if inok and input then
                                local tok,tus=pcall(function()
                                    return vlc.var.get(input,"time") end)
                                if tok and tus then
                                    local t=tus/1000000
                                    -- Intro auto-skip
                                    if not intro_skipped and d.intro_end then
                                        local is=d.intro_start or 0
                                        local grace_end=math.min(
                                            is+AUTO_SKIP_GRACE_SEC,
                                            d.intro_end-0.5)
                                        if t>=is and t<grace_end then
                                            if seek_and_play(d.intro_end,input) then
                                                intro_skipped=true
                                            end
                                        elseif t>=d.intro_end then
                                            intro_skipped=true
                                        end
                                    end
                                    -- Outro auto-skip
                                    if not outro_skipped and d.outro_start then
                                        if t>=d.outro_start then
                                            outro_skipped=true
                                            safe_skip_outro(input,d,item)
                                        end
                                    end
                                end
                            end
                        end
                    end
                end
            end
        end
        check_count=check_count+1
        if check_count>=60 then check_count=0; load_data() end
        if vlc.misc and vlc.misc.mwait and vlc.misc.mdate then
            vlc.misc.mwait(vlc.misc.mdate()
                +(playing and POLL_ACTIVE_US or POLL_IDLE_US))
        else
            running=false
        end
    end)
    if not ok then running=false end
end
'''

    os.makedirs(VLC_CONF_DIR, exist_ok=True)
    ext_path = os.path.join(VLC_CONF_DIR, "intro-skipper.lua")
    with open(ext_path, "w", encoding="utf-8") as f:
        f.write(lua_ext)

    os.makedirs(_VLC_INTF_DIR, exist_ok=True)
    with open(os.path.join(_VLC_INTF_DIR, "intro_skipper_intf.lua"),
              "w", encoding="utf-8") as f:
        f.write(intf)

    if not silent:
        logger.info("VLC extension installed to %s", ext_path)
    return ext_path


def export_for_media_server(
    results: DetectionResults, output_path: str,
    format_type: str = "plex",
    durations: Optional[DurationMap] = None,
):
    if format_type == "plex":
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("filename,intro_start,intro_end,"
                    "outro_start,outro_end\n")
            for fp, segs in sorted(results.items()):
                n = os.path.basename(fp)
                is_ = (_snap_timestamp(segs["intro"][0])
                       if "intro" in segs else "")
                ie_ = (_snap_timestamp(segs["intro"][1])
                       if "intro" in segs else "")
                os_ = (_snap_timestamp(segs["outro"][0])
                       if "outro" in segs else "")
                oe_ = (_snap_timestamp(segs["outro"][1])
                       if "outro" in segs else "")
                f.write(f"{n},{is_},{ie_},{os_},{oe_}\n")
    elif format_type == "jellyfin":
        eps = []
        for fp, segs in sorted(results.items()):
            ep: Dict[str, Any] = {"filename": os.path.basename(fp)}
            if "intro" in segs:
                ep["intro"] = {
                    "start": _snap_timestamp(segs["intro"][0]),
                    "end": _snap_timestamp(segs["intro"][1])}
            if "outro" in segs:
                ep["outro"] = {
                    "start": _snap_timestamp(segs["outro"][0]),
                    "end": _snap_timestamp(segs["outro"][1])}
            eps.append(ep)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"episodes": eps}, f, indent=2)
    logger.info("Exported %s to %s", format_type, output_path)


# ============================================================
# CLI
# ============================================================


def validate_cli_args(args: argparse.Namespace, config: Config) -> bool:
    errors: List[str] = []
    if args.similarity_threshold is not None:
        if not 0.0 <= args.similarity_threshold <= 1.0:
            errors.append("threshold must be 0.0–1.0")
        else:
            config["similarity_threshold"] = args.similarity_threshold
    if args.min_intro_duration is not None:
        if args.min_intro_duration <= 0:
            errors.append("min duration must be > 0")
        else:
            config["min_segment_duration"] = args.min_intro_duration
    if args.max_intro_duration is not None:
        if args.max_intro_duration <= 0:
            errors.append("max duration must be > 0")
        else:
            config["max_segment_duration"] = args.max_intro_duration
    if args.intro_search_start is not None:
        if args.intro_search_start < 0:
            errors.append("search start must be ≥ 0")
        else:
            config["intro_search_start"] = args.intro_search_start
    if args.intro_search_end is not None:
        if args.intro_search_end <= 0:
            errors.append("search end must be > 0")
        else:
            config["intro_search_end"] = args.intro_search_end
    if args.outro_search_duration is not None:
        if args.outro_search_duration <= 0:
            errors.append("outro duration must be > 0")
        else:
            config["outro_search_duration"] = args.outro_search_duration
    if args.batch_size is not None and args.batch_size < 2:
        errors.append("batch must be ≥ 2")
    if (config.get("min_segment_duration", 0)
            >= config.get("max_segment_duration", float("inf"))):
        errors.append("min must be < max duration")
    if (config.get("intro_search_start", 0)
            >= config.get("intro_search_end", float("inf"))):
        errors.append("search start must be < end")
    for e in errors:
        logger.error(e)
    return len(errors) == 0


def main():
    _setup_logging()
    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    parser = argparse.ArgumentParser(
        description="Intro/Outro Skipper",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    parser.add_argument("--video-dir", type=str)
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument("--show-type", choices=list(SHOW_CONFIGS.keys()),
                        default="standard")
    parser.add_argument("--import-timestamps", type=str)
    parser.add_argument("--install-vlc-extension", action="store_true")
    parser.add_argument("--output", type=str, default=SKIP_DATA_FILE)
    parser.add_argument("--generate-conf", action="store_true")
    parser.add_argument(
        "--update-vlc-extension",
        action=_BooleanOptionalAction,
        default=True,
        help="Refresh VLC Lua scripts on every run "
             "(disable with --no-update-vlc-extension).")
    parser.add_argument("--similarity-threshold", type=float)
    parser.add_argument("--min-intro-duration", type=float)
    parser.add_argument("--max-intro-duration", type=float)
    parser.add_argument("--intro-search-start", type=float)
    parser.add_argument("--intro-search-end", type=float)
    parser.add_argument("--outro-search-duration", type=float)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--max-fingerprint-mb", type=float)
    parser.add_argument("--ffmpeg-timeout", type=int)
    parser.add_argument("--no-graph", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-parallel", action="store_true")
    parser.add_argument("--use-full-path", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--export-format", choices=["plex", "jellyfin"])
    parser.add_argument("--export-path", type=str)
    parser.add_argument("--clear-cache", action="store_true")
    parser.add_argument("--clear-cache-days", type=int)

    args = parser.parse_args()

    if args.clear_cache or args.clear_cache_days is not None:
        try:
            with FingerprintCache() as cache:
                cache.clear_cache(older_than_days=args.clear_cache_days)
            print("Cache cleared.")
        except Exception as exc:
            logger.error("Clear failed: %s", exc)
            sys.exit(1)
        return

    if not validate_ffmpeg(FFMPEG):
        logger.error("ffmpeg not found: %s", FFMPEG)
        sys.exit(1)

    if args.install_vlc_extension:
        install_vlc_extension(silent=False)
        print("VLC extension installed!")
        return

    if not args.video_dir:
        parser.print_help()
        print("\nError: --video-dir required")
        sys.exit(1)

    video_dir = Path(args.video_dir)
    if not video_dir.is_dir():
        logger.error("Not a directory: %s", video_dir)
        sys.exit(1)

    config = SHOW_CONFIGS[args.show_type].copy()
    if args.no_graph:
        config["use_graph_consensus"] = False
    if not validate_cli_args(args, config):
        sys.exit(1)

    exit_code = 0
    try:
        with FingerprintCache() as cache:
            try:
                if args.recursive:
                    videos = _collect_videos_recursive(video_dir)
                else:
                    videos = sorted(
                        str(v) for v in video_dir.iterdir()
                        if v.suffix.lower() in VIDEO_EXTENSIONS)
            except (PermissionError, OSError) as exc:
                logger.error("Error: %s", exc)
                sys.exit(1)
            if not videos:
                logger.error("No videos in %s", video_dir)
                sys.exit(1)
            logger.info("Found %d videos", len(videos))
            validate_episode_set(videos)
            duration_map: DurationMap = {}

            if args.import_timestamps:
                results = import_timestamps_file(
                    args.import_timestamps, str(video_dir), cache)
            elif len(videos) < 2:
                logger.error("Need ≥ 2 episodes.")
                sys.exit(1)
            else:
                bs = args.batch_size
                par = not args.no_parallel
                if len(videos) > (bs or CONSTANTS.BATCH_SIZE):
                    results, duration_map = detect_segments_batched(
                        videos, config, cache, bs, force=args.force,
                        parallel_extract=par,
                        max_fingerprint_mb=args.max_fingerprint_mb,
                        ffmpeg_timeout=args.ffmpeg_timeout)
                else:
                    results, duration_map = detect_segments(
                        videos, config, cache, force=args.force,
                        parallel_extract=par,
                        max_fingerprint_mb=args.max_fingerprint_mb,
                        ffmpeg_timeout=args.ffmpeg_timeout)

            if _shutdown_requested:
                sys.exit(130)
            if not results:
                logger.error("No segments detected.")
                sys.exit(1)
            results = {k: v for k, v in results.items() if v}
            if not results:
                sys.exit(1)
            for fp_ in results:
                if fp_ not in duration_map and os.path.isfile(fp_):
                    d = get_duration(fp_)
                    if d:
                        duration_map[fp_] = d

            if args.dry_run:
                print(f"\n{'='*60}\nDRY RUN\n{'='*60}")
            else:
                generate_skip_data(
                    results, args.output, duration_map,
                    use_full_path=args.use_full_path)
                if args.generate_conf:
                    generate_vlc_conf(results, durations=duration_map)
                if args.update_vlc_extension:
                    install_vlc_extension(silent=True)
                if args.export_format:
                    ext = ("csv" if args.export_format == "plex"
                           else "json")
                    ep = args.export_path or os.path.join(
                        DATA_DIR,
                        f"skip_data_{args.export_format}.{ext}")
                    export_for_media_server(
                        results, ep, args.export_format, duration_map)

            print(f"\n{'='*60}\nDONE\n{'='*60}")
            ni = sum(1 for r in results.values() if "intro" in r)
            no_ = sum(1 for r in results.values() if "outro" in r)
            print(f"Episodes: {len(results)}, "
                  f"Intros: {ni}, Outros: {no_}")
            print("\nSegments:")
            for fp_, segs in sorted(results.items()):
                name = os.path.basename(fp_)
                parts = []
                if "intro" in segs:
                    s, e = segs["intro"]
                    parts.append(
                        f"intro: {format_time(s)}–{format_time(e)}")
                if "outro" in segs:
                    s, e = segs["outro"]
                    parts.append(
                        f"outro: {format_time(s)}–{format_time(e)}")
                print(f"  {name}: {', '.join(parts) or 'none'}")

    except KeyboardInterrupt:
        exit_code = 130
    except Exception as exc:
        logger.error("Error: %s", exc)
        traceback.print_exc()
        exit_code = 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

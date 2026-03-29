"""
AniWorld.to Backup Tool v1.0
Read-only backup of watched/subscribed/watchlist data from aniworld.to
"""

import atexit
import json
import logging
import os
import queue
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Add parent directory to path to access config
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Config.Config3 import (
    SELECTORS_CONFIG, EMAIL, PASSWORD, HEADLESS, DATA_DIR,
    HTTP_REQUEST_TIMEOUT, PAGE_LOAD_TIMEOUT,
    DRIVER_QUIT_TIMEOUT, MAX_TOTAL_RETRIES
)
from src.index_manager3 import _is_pid_alive

logger = logging.getLogger(__name__)


# Adaptive backoff constants
MAX_HEAVY_BACKOFF = 5.0
HEAVY_BACKOFF_FACTOR = 0.5
MAX_MODERATE_BACKOFF = 3.0
MODERATE_BACKOFF_FACTOR = 0.3
LIGHT_BACKOFF_THRESHOLD = 1.0
BACKOFF_DECAY_STEP = 0.1


def _kill_pids_in_file(pids_dict):
    """Kill all geckodriver PIDs listed in a pids dict (skips _owner_pid)."""
    for key, pid in pids_dict.items():
        if key == '_owner_pid':
            continue
        try:
            pid = int(pid)
            if sys.platform == 'win32':
                subprocess.run(
                    ['taskkill', '/F', '/PID', str(pid), '/T'],
                    capture_output=True, check=False, timeout=2
                )
            else:
                try:
                    os.kill(pid, signal.SIGTERM)
                    time.sleep(0.5)
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        except (OSError, ValueError, subprocess.SubprocessError):
            pass


def cleanup_stale_worker_pids():
    """Scan data/ for stale worker PID files from previous runs and clean up."""
    try:
        files = [
            f for f in os.listdir(DATA_DIR)
            if f.startswith('.worker_pids_') and f.endswith('.json')
        ]
    except OSError:
        return
    for fname in files:
        fpath = os.path.join(DATA_DIR, fname)
        try:
            with open(fpath, 'r') as f:
                pids = json.load(f)
            if not isinstance(pids, dict):
                os.remove(fpath)
                continue
            owner_pid = pids.get('_owner_pid')
            if owner_pid and _is_pid_alive(owner_pid):
                continue
            _kill_pids_in_file(pids)
            os.remove(fpath)
        except (OSError, json.JSONDecodeError, ValueError):
            try:
                os.remove(fpath)
            except OSError:
                pass


# NOTE: cleanup_stale_worker_pids() is called once on first scraper
# instantiation via AniWorldScraper.__init__(), not at import time.


def cleanup_geckodriver_processes(timeout_sec=5):
    """Kill geckodriver processes we spawned."""
    if not os.path.exists(_MY_PID_FILE):
        return
    try:
        with open(_MY_PID_FILE, 'r') as f:
            pids = json.load(f)
        if isinstance(pids, dict):
            _kill_pids_in_file(pids)
    except (OSError, json.JSONDecodeError, ValueError):
        pass
    try:
        os.remove(_MY_PID_FILE)
    except OSError:
        pass


def _signal_handler(signum, frame):
    """Convert termination signals into clean exit so atexit handlers run"""
    sys.exit(0)


_MY_PID_FILE = os.path.join(DATA_DIR, f'.worker_pids_{os.getpid()}.json')

atexit.register(cleanup_geckodriver_processes, timeout_sec=5)
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)
if sys.platform == 'win32':
    signal.signal(signal.SIGBREAK, _signal_handler)

MAX_WORKERS = int(os.getenv("ANIWORLD_MAX_WORKERS", "24"))

# Pre-compiled regex patterns for aniworld.to
_DOMAIN_STRIP_RE = re.compile(r'^https?://[^/]+')
_ANIME_PATH_RE = re.compile(r'(/anime/stream/[^/]+)')
_ANIME_SLUG_RE = re.compile(r'^/anime/stream/([^/?#]+)/?$')
_EPISODE_LABEL_RE = re.compile(r'\s*\[Episode\s+\d+\]\s*$', re.IGNORECASE)
_STAFFEL_RE = re.compile(r'/staffel-(\d+)')
_FILME_RE = re.compile(r'/filme\b')


class SeasonDetectionError(Exception):
    """Raised when season detection fails after all retries."""
    pass


class ScrapingPaused(Exception):
    """Raised when scraping is paused via pause file."""
    pass


class ConfigurationError(Exception):
    """Raised when critical configuration is missing."""
    pass


# ==================== UTILITY FUNCTIONS ====================

def validate_episode_data(episodes):
    """Validate episode data for consistency."""
    if not isinstance(episodes, list):
        logger.warning("Episode data is not a list")
        return []

    validated = []
    for i, ep in enumerate(episodes):
        if not isinstance(ep, dict):
            logger.warning(f"Dropping non-dict episode entry at index {i}: {ep}")
            continue

        ep_num = ep.get('number')
        if ep_num is None:
            logger.warning(f"Dropping episode at index {i}: missing episode number")
            continue

        if 'watched' not in ep:
            logger.warning(f"Dropping episode at index {i}: missing 'watched' field")
            continue

        validated_ep = {
            'number': int(ep_num) if isinstance(ep_num, (int, float)) else ep_num,
            'watched': ep['watched']
        }
        if 'title_ger' in ep:
            validated_ep['title_ger'] = ep['title_ger']
        if 'title_eng' in ep:
            validated_ep['title_eng'] = ep['title_eng']
        if 'title' in ep:
            validated_ep['title'] = ep['title']
        if 'languages' in ep:
            validated_ep['languages'] = ep['languages']
        validated.append(validated_ep)

    return validated


def get_friendly_error(error):
    """Convert technical errors to user-friendly messages."""
    error_str = str(error).lower()

    if 'timeout' in error_str:
        return "Network timeout (server slow or connection unstable)"
    elif 'connection' in error_str:
        return "Connection error (check internet/firewall)"
    elif 'not found' in error_str or '404' in error_str:
        return "Page not found (series may have been removed)"
    elif 'unauthorized' in error_str or '401' in error_str or '403' in error_str:
        return "Access denied (login may have expired)"
    elif 'server error' in error_str or '500' in error_str or '502' in error_str:
        return "Server error (aniworld.to experiencing issues)"
    elif 'element not found' in error_str:
        return "Layout changed (site structure may have updated)"
    else:
        return error_str[:80]


class AniWorldScraper:
    """
    Read-only backup tool for aniworld.to user data.

    Scrapes series, seasons, episodes, watched status, and subscription info.
    Supports both parallel and sequential execution modes.
    """

    _stale_pids_cleaned = False

    def __init__(self):
        if not AniWorldScraper._stale_pids_cleaned:
            cleanup_stale_worker_pids()
            AniWorldScraper._stale_pids_cleaned = True

        self.driver = None
        self.config = SELECTORS_CONFIG
        self.email = EMAIL
        self.password = PASSWORD
        self.completed_links = set()
        self.failed_links = []
        self.series_data = []
        self.auth_cookies = []
        self._lock = threading.Lock()
        self._use_parallel = True
        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.pause_file = os.path.join(DATA_DIR, '.pause_scraping')
        self._checkpoint_mode = None
        self._config_validated = False
        self._last_pause_check = 0.0
        self._pause_cached = False
        self._series_retry_count = {}
        self.all_discovered_series = None

        # Global rate limiter
        self._request_lock = threading.Lock()
        self._last_request_time = 0.0
        self._min_request_interval = self.config.get('timing', {}).get('min_request_interval', 0.2)
        self._server_error_times = []
        self._global_backoff = 0.0

        # Periodic flush
        self._partial_data_file = os.path.join(DATA_DIR, '.series_data_partial.jsonl')
        self._flushed_count = 0
        self.timing_file = os.path.join(DATA_DIR, '.scrape_timing.json')
        self._historical_avg = None  # Loaded at scrape start from last run

        self._validate_selectors_config()

    def _throttle_request(self):
        """Enforce minimum interval between HTTP requests across all workers."""
        with self._request_lock:
            now = time.time()
            effective_interval = self._min_request_interval + self._global_backoff
            elapsed = now - self._last_request_time
            if elapsed < effective_interval:
                time.sleep(effective_interval - elapsed)
            self._last_request_time = time.time()

    def _record_server_error(self):
        """Record a server error and adjust global backoff adaptively."""
        with self._request_lock:
            now = time.time()
            self._server_error_times.append(now)
            self._server_error_times = [t for t in self._server_error_times if now - t < 60]
            error_count = len(self._server_error_times)
            if error_count >= 15:
                self._global_backoff = min(MAX_HEAVY_BACKOFF, HEAVY_BACKOFF_FACTOR * (error_count / 5))
            elif error_count >= 8:
                self._global_backoff = min(MAX_MODERATE_BACKOFF, MODERATE_BACKOFF_FACTOR * (error_count / 5))
            elif error_count >= 4:
                self._global_backoff = LIGHT_BACKOFF_THRESHOLD

    def _decay_global_backoff(self):
        """Gradually reduce global backoff after successful requests."""
        with self._request_lock:
            if self._global_backoff > 0:
                self._global_backoff = max(0.0, self._global_backoff - BACKOFF_DECAY_STEP)

    def _validate_selectors_config(self):
        """Validate that critical selectors are configured."""
        if self._config_validated:
            return

        critical_selectors = [
            'login.username_field',
            'login.password_field',
            'login.submit_button',
            'season_nav.pills',
        ]

        missing = []
        for selector_path in critical_selectors:
            if not self.get_selector(selector_path):
                missing.append(selector_path)

        if missing:
            error_msg = f"\n✗ CRITICAL: Missing selectors in config:\n"
            for m in missing:
                error_msg += f"  - {m}\n"
            error_msg += f"\nPlease check Config/selectors_config3.json\n"
            print(error_msg)
            logger.error(f"Missing critical selectors: {missing}")
            raise ConfigurationError(error_msg.strip())

        self._config_validated = True
        logger.debug("Selector config validated successfully")

    def _load_scrape_timing(self):
        """Load avg time per series from last completed scrape."""
        try:
            with open(self.timing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            avg = data.get('avg_per_series')
            if avg and avg > 0:
                return float(avg)
        except (OSError, json.JSONDecodeError, ValueError):
            pass
        return None

    def _save_scrape_timing(self, duration, series_count):
        """Save timing data from completed scrape for future ETA estimates."""
        if series_count <= 0:
            return
        data = {
            'last_scrape_duration': round(duration, 2),
            'series_count': series_count,
            'avg_per_series': round(duration / series_count, 4),
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S')
        }
        try:
            self._atomic_write_json(self.timing_file, data)
        except Exception as e:
            logger.warning(f"Could not save scrape timing: {e}")

    def _add_failed_link(self, item, series_url=None, display_title=None, error=None):
        """Thread-safe helper to add a failed link entry."""
        normalized = self._normalize_failed_item(item, series_url, display_title)
        if error:
            normalized['error'] = get_friendly_error(error)
            logger.debug(f"Failed: {normalized.get('title')} - {normalized['error']}")
        with self._lock:
            self.failed_links.append(normalized)

    def normalize_to_series_url(self, url):
        """Normalize a series URL/slug to full aniworld.to URL."""
        url = url.split('?')[0].split('#')[0]
        url = _DOMAIN_STRIP_RE.sub("", url)
        m = _ANIME_PATH_RE.match(url)
        if m:
            return f"https://aniworld.to{m.group(1)}"
        slug = url.strip().strip('/')
        if slug:
            return f"https://aniworld.to/anime/stream/{slug}"
        return url

    def _normalize_failed_item(self, item, series_url=None, display_title=None):
        """Normalize a failed item to a consistent dict format."""
        if isinstance(item, dict) and 'url' in item:
            return item
        if isinstance(item, dict):
            return {
                'url': item.get('url', item.get('link', series_url or '')),
                'title': item.get('title', display_title or '')
            }
        url = series_url or str(item)
        if not url.startswith('http'):
            url = f"https://aniworld.to/anime/stream/{url}"
        return {'url': url, 'title': display_title or ''}

    def _extract_item_info(self, item):
        """Extract URL, slug, and display title from a work item."""
        if isinstance(item, dict):
            series_url = item.get('url', item.get('link', ''))
            display_title = item.get('title', '')
        else:
            series_url = str(item)
            display_title = ''
        series_slug = self.get_series_slug_from_url(series_url)
        display_title = display_title or series_slug
        return series_url, series_slug, display_title

    def _aggregate_season_results(self, series_slug, season_results, missing_seasons, series_data,
                                  is_subscribed=None, is_watchlist=None):
        """Build series_data entry from season results."""
        series_had_error = len(missing_seasons) > 0
        series_watched = 0
        series_total_eps = 0
        total_malformed = 0

        for season_data in season_results:
            if series_slug not in series_data:
                series_data[series_slug] = {
                    'seasons': [],
                    'url': f"https://aniworld.to/anime/stream/{series_slug}",
                    'link': f"/anime/stream/{series_slug}",
                    'subscribed': is_subscribed,
                    'watchlist': is_watchlist,
                }
            # Track malformed episodes from this season
            malformed = season_data.pop('_malformed_episodes', 0)
            total_malformed += malformed
            series_data[series_slug]['seasons'].append(season_data)
            series_watched += season_data.get('watched_episodes', 0)
            series_total_eps += season_data.get('total_episodes', 0)

        if total_malformed > 0:
            series_had_error = True
            series_data[series_slug]['_has_malformed_episodes'] = True
            logger.warning(f"{series_slug}: {total_malformed} malformed episode row(s) across all seasons — marking for retry")

        sub_wl_ok = is_subscribed is not None and is_watchlist is not None
        if not series_had_error and sub_wl_ok and series_slug in series_data and series_total_eps > 0:
            self.completed_links.add(series_slug)

        return series_watched, series_total_eps, series_had_error, is_subscribed, is_watchlist

    def _format_progress_line(self, done, total, start_time, title, watched=None,
                              episode_total=None, empty=False, error=None,
                              worker_id=None, worker_count=None, season_labels=None,
                              subscribed=None, watchlist=None):
        """Build a single-line progress string with bar, ETA, and status.
        
        Uses historical avg from last scrape for better early ETA.
        """
        elapsed = time.time() - start_time
        current_avg = elapsed / max(1, done) if done > 0 else 0
        historical_avg = self._historical_avg
        # Blend with historical avg for better early ETA
        if done <= 0 and historical_avg:
            effective_avg = historical_avg
        elif historical_avg is not None and total > 0:
            blend = min(1.0, done / max(1, total * 0.15))
            effective_avg = (1 - blend) * historical_avg + blend * current_avg
        else:
            effective_avg = current_avg
        remaining = total - done
        eta_mins = int((effective_avg * remaining) / 60)
        pct = int((done / total) * 100) if total else 0
        bar_len = 30
        filled = int(bar_len * done / total) if total else 0
        bar = '█' * filled + '░' * (bar_len - filled)
        worker_info = f" | W{worker_id}/{worker_count}" if worker_id and worker_count else " | Fallback"
        season_info = f" [{','.join(str(s) for s in season_labels)}]" if season_labels else ""
        sub_parts = []
        if subscribed is not None:
            sub_parts.append(f"Sub:{'✓' if subscribed else '✗'}")
        if watchlist is not None:
            sub_parts.append(f"WL:{'✓' if watchlist else '✗'}")
        sub_info = f" ({' '.join(sub_parts)})" if sub_parts else ""
        if error:
            return f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✗ {title}: {error}"
        elif empty:
            return f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ⚠ {title}{season_info}: No episodes{sub_info}"
        else:
            return f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✓ {title}{season_info}: {watched}/{episode_total} watched{sub_info}"

    # ==================== FILE I/O HELPERS ====================

    @staticmethod
    def _atomic_write_json(filepath, data, timeout=HTTP_REQUEST_TIMEOUT):
        """Write JSON to file atomically via temp file + os.replace."""
        dirpath = os.path.dirname(filepath)
        os.makedirs(dirpath, exist_ok=True)

        try:
            stat = shutil.disk_usage(dirpath)
            if stat.free < 1024 * 1024:
                raise OSError(f"Insufficient disk space for {filepath} (< 1 MB free)")
        except Exception as e:
            logger.warning(f"Could not check disk space: {e}")

        fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix='.tmp')
        start_time = time.time()
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Write to {filepath} exceeded {timeout}s timeout")
            os.replace(tmp_path, filepath)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                logger.warning(f"Failed to clean up temp file: {tmp_path}")
            raise

    # ==================== CHECKPOINT SYSTEM ====================

    def set_checkpoint_paths(self, data_dir):
        """Set paths for checkpoint, failed series, and pause files"""
        self.checkpoint_file = os.path.join(data_dir, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(data_dir, '.failed_series.json')
        self.pause_file = os.path.join(data_dir, '.pause_scraping')

    def save_checkpoint(self, include_data=False):
        """Save scraping checkpoint to resume later (thread-safe)."""
        if not self.checkpoint_file:
            return
        with self._lock:
            try:
                checkpoint_data = {
                    'completed_links': list(self.completed_links),
                    'mode': self._checkpoint_mode,
                    'timestamp': time.time(),
                }
                if include_data and self.series_data:
                    checkpoint_data['series_data'] = self.series_data
                self._atomic_write_json(self.checkpoint_file, checkpoint_data)
            except Exception as e:
                logger.error(f"Failed to save checkpoint: {e}")
                print(f"  ⚠ Warning: checkpoint save failed: {e}")

    def load_checkpoint(self):
        """Load checkpoint to resume from previous run (thread-safe)."""
        with self._lock:
            if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
                return False
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict) and 'completed_links' in data:
                    completed_links = data.get('completed_links', [])
                    if not isinstance(completed_links, list):
                        completed_links = []

                    mode = data.get('mode')
                    if mode is not None and not isinstance(mode, str):
                        mode = None

                    series_data = data.get('series_data', [])
                    if not isinstance(series_data, list):
                        series_data = []

                    self.completed_links = set(completed_links)
                    self._checkpoint_mode = mode
                    if series_data:
                        self.series_data = series_data
                    return True
                elif isinstance(data, list):
                    self.completed_links = set(data) if isinstance(data, list) else set()
                    return True
                else:
                    print(f"✗ Checkpoint file is invalid or corrupted.")
                    return False
            except Exception as e:
                print(f"✗ Failed to load checkpoint: {e}")
                return False

    @staticmethod
    def get_checkpoint_mode(data_dir):
        """Read the mode from an existing checkpoint file without loading it."""
        checkpoint_file = os.path.join(data_dir, '.scrape_checkpoint.json')
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data.get('mode')
        except FileNotFoundError:
            logger.debug(f"No checkpoint file found at {checkpoint_file}")
        except (json.JSONDecodeError, OSError) as e:
            logger.debug(f"Could not read checkpoint mode: {e}")
        return None

    def clear_checkpoint(self):
        """Clear checkpoint after successful completion (thread-safe)."""
        with self._lock:
            if self.checkpoint_file and os.path.exists(self.checkpoint_file):
                try:
                    os.remove(self.checkpoint_file)
                except OSError as e:
                    logger.debug(f"Could not remove checkpoint file: {e}")
        self._cleanup_partial_file()

    def save_failed_series(self):
        """Save list of failed series for retry (thread-safe)."""
        if not self.failed_file or not self.failed_links:
            return
        with self._lock:
            try:
                existing = self._load_failed_series_unlocked()
                merged = {}
                merged_list = []

                for item in existing:
                    if isinstance(item, dict):
                        url = item.get('url', '')
                        if url:
                            merged[url] = item
                        else:
                            merged_list.append(item)
                    elif isinstance(item, str) and item:
                        merged[item] = {'url': item, 'title': ''}

                for item in self.failed_links:
                    if isinstance(item, dict):
                        url = item.get('url', '')
                        if url:
                            merged[url] = item
                        else:
                            logger.warning(f"Skipping failed item with empty URL: {item}")
                            continue
                    else:
                        key = str(item) if item else None
                        if key:
                            merged[key] = {'url': key, 'title': ''}

                result = list(merged.values()) + merged_list
                self._atomic_write_json(self.failed_file, result)
            except Exception as e:
                logger.error(f"Failed to save failed series list: {e}")
                print(f"  ⚠ Warning: could not save failed series list: {e}")

    def _load_failed_series_unlocked(self):
        """Internal: load failed series without locking."""
        if not self.failed_file:
            return []
        try:
            with open(self.failed_file, 'r', encoding='utf-8') as f:
                return json.load(f) or []
        except FileNotFoundError:
            return []
        except json.JSONDecodeError as e:
            logger.warning(f"Failed series file corrupted, ignoring: {e}")
            return []
        except Exception as e:
            logger.warning(f"Could not load failed series: {e}")
            return []

    def load_failed_series(self):
        """Load previously failed series for retry (thread-safe)."""
        with self._lock:
            return self._load_failed_series_unlocked()

    def clear_failed_series(self):
        """Clear failed series list after successful retry (thread-safe)."""
        with self._lock:
            if self.failed_file and os.path.exists(self.failed_file):
                try:
                    os.remove(self.failed_file)
                except OSError as e:
                    logger.debug(f"Could not remove failed series file: {e}")

    def is_pause_requested(self):
        """Check if pause was requested (cached: re-checks file at most every 2 seconds)."""
        try:
            now = time.time()
            if now - self._last_pause_check < 2.0:
                return self._pause_cached
            self._last_pause_check = now
            self._pause_cached = self.pause_file and os.path.exists(self.pause_file)
            return self._pause_cached
        except OSError:
            return False

    def clear_pause_request(self):
        """Clear pause request file."""
        self._pause_cached = False
        self._last_pause_check = 0.0
        if self.pause_file and os.path.exists(self.pause_file):
            try:
                os.remove(self.pause_file)
            except OSError as e:
                logger.debug(f"Could not remove pause file: {e}")

    # ==================== CONFIG HELPERS ====================

    def get_selector(self, path):
        """Get selector from config using dot notation"""
        keys = path.split('.')
        value = self.config.get('selectors', {})
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def get_login_page(self):
        """Get login page URL from config"""
        return self.config.get('login_page', 'https://aniworld.to/login')

    def get_site_url(self):
        """Get site URL from config"""
        return self.config.get('site_url', 'https://aniworld.to')

    def get_timing(self, key, default=1.0):
        """Get timing delay from config (in seconds)"""
        return self.config.get('timing', {}).get(key, default)

    def get_timing_float(self, key, default, min_val=0.0, max_val=None):
        """Read a timing value from config as float."""
        try:
            value = self.get_timing(key, default)
            if value is None or (isinstance(value, str) and value.lower() in ('null', 'none')):
                return float(default) if default is not None else 0.0
            result = float(value)
            if min_val is not None:
                result = max(result, min_val)
            if max_val is not None:
                result = min(result, max_val)
            return result
        except (ValueError, TypeError):
            logger.warning(f"Invalid timing value for {key}: {value}, using default {default}")
            return float(default) if default is not None else 0.0

    def get_timing_int(self, key, default, min_val=0, max_val=None):
        """Read a timing value from config as int."""
        try:
            value = self.get_timing(key, default)
            if value is None or (isinstance(value, str) and value.lower() in ('null', 'none')):
                return int(default) if default is not None else 0
            result = int(float(value))
            if min_val is not None:
                result = max(result, min_val)
            if max_val is not None:
                result = min(result, max_val)
            return result
        except (ValueError, TypeError):
            logger.warning(f"Invalid timing value for {key}: {value}, using default {default}")
            return int(default) if default is not None else 0

    # ==================== ELEMENT FINDING ====================

    def convert_selector_to_by(self, selector_type):
        """Convert config selector type to Selenium By"""
        by_map = {
            'id': By.ID,
            'name': By.NAME,
            'css': By.CSS_SELECTOR,
            'xpath': By.XPATH,
            'tag': By.TAG_NAME,
            'class': By.CLASS_NAME
        }
        return by_map.get(selector_type, By.CSS_SELECTOR)

    def find_element_from_config(self, driver, config_selectors, timeout=None):
        """Try to find element using list of selectors from config"""
        if not isinstance(config_selectors, list):
            config_selectors = [config_selectors]
        if timeout is None:
            timeout = self.get_timing_float('element_find_timeout', 2.0)

        for selector_config in config_selectors:
            selector_type = selector_config.get('type', 'css')
            selector_value = selector_config.get('value')
            by = self.convert_selector_to_by(selector_type)
            try:
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((by, selector_value))
                )
                return element
            except Exception:
                continue

        return None

    # ==================== BROWSER SETUP ====================

    def _build_firefox_options(self):
        """Build Firefox options with enhanced anti-detection and stealth"""
        firefox_options = Options()

        if HEADLESS:
            firefox_options.add_argument("-headless")

        # Ad & popup blocking
        firefox_options.set_preference("permissions.default.image", 1)
        firefox_options.set_preference("media.autoplay.default", 1)
        firefox_options.set_preference("media.autoplay.default.allowed", False)
        firefox_options.set_preference("dom.plugins.enabled", False)
        firefox_options.set_preference("security.enable_java", False)
        firefox_options.set_preference("dom.disable_beforeunload", True)
        firefox_options.set_preference("dom.popup_allowed_events", "click")
        firefox_options.set_preference("browser.contentblocking.category", "strict")
        firefox_options.set_preference("dom.ipc.processPrelaunch.enabled", False)
        firefox_options.set_preference("network.http.speculative-parallel-limit", 0)
        firefox_options.set_preference("network.cookie.lifetimePolicy", 2)
        firefox_options.set_preference("network.dns.disablePrefetch", True)
        firefox_options.set_preference("network.http.sendRefererHeader", 0)
        firefox_options.set_preference("network.IDN_show_punycode", True)
        firefox_options.set_preference("geo.enabled", False)
        firefox_options.set_preference("webgl.disabled", True)

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
        ]
        firefox_options.set_preference('general.useragent.override', random.choice(user_agents))
        firefox_options.set_preference('dom.webdriver.enabled', False)
        firefox_options.set_preference('useAutomationExtension', False)
        firefox_options.set_preference('dom.webdriver.chromium.enabled', False)
        firefox_options.set_preference('network.http.keep-alive.timeout', 300)
        firefox_options.set_preference('network.http.max-persistent-connections-per-server', 6)
        firefox_options.set_preference('network.http.pipelining', False)

        return firefox_options

    def _get_ublock_xpi(self):
        """Find uBlock Origin .xpi, using local copy or copying from Firefox profile."""
        ublock_id = 'uBlock0@raymondhill.net.xpi'
        addon_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'addons')
        local_xpi = os.path.join(addon_dir, 'ublock_origin.xpi')

        if os.path.isfile(local_xpi):
            return local_xpi

        profiles_dir = os.path.join(os.environ.get('APPDATA', ''), 'Mozilla', 'Firefox', 'Profiles')
        if os.path.isdir(profiles_dir):
            for profile in os.listdir(profiles_dir):
                xpi_path = os.path.join(profiles_dir, profile, 'extensions', ublock_id)
                if os.path.isfile(xpi_path):
                    os.makedirs(addon_dir, exist_ok=True)
                    shutil.copy2(xpi_path, local_xpi)
                    print(f'✓ Copied uBlock Origin from Firefox profile: {profile}')
                    return local_xpi

        url = 'https://addons.mozilla.org/firefox/downloads/latest/ublock-origin/latest.xpi'
        print('→ Downloading uBlock Origin from addons.mozilla.org...')
        try:
            os.makedirs(addon_dir, exist_ok=True)
            urllib.request.urlretrieve(url, local_xpi)
            print('✓ uBlock Origin downloaded')
            return local_xpi
        except Exception as e:
            print(f'⚠ Failed to download uBlock Origin: {e}')
            try:
                os.remove(local_xpi)
            except OSError:
                pass
            return None

    def setup_driver(self):
        """Initialize the Selenium WebDriver with anti-detection and ad-blocking"""
        firefox_options = self._build_firefox_options()
        service = FirefoxService()
        self.driver = webdriver.Firefox(service=service, options=firefox_options)
        self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

        try:
            if hasattr(service, 'process') and service.process:
                geckodriver_pid = service.process.pid
                if geckodriver_pid:
                    self.save_worker_pid(0, geckodriver_pid)
                    try:
                        result = subprocess.run(
                            ['wmic', 'process', 'where', f'ParentProcessId={geckodriver_pid}',
                             'get', 'ProcessId', '/value'],
                            capture_output=True, text=True,
                            encoding='utf-8', errors='replace',
                            timeout=self.get_timing_float('process_lookup_timeout', 2.0), check=False
                        )
                        for line in result.stdout.split('\n'):
                            if 'ProcessId' in line:
                                firefox_pid = line.split('=')[-1].strip()
                                if firefox_pid.isdigit():
                                    self.save_worker_pid('0_firefox', int(firefox_pid))
                    except Exception:
                        pass
        except Exception:
            pass

        # Stealth JS injections
        try:
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['de-DE', 'de', 'en-US', 'en']})")
            self.driver.execute_script("Object.defineProperty(window, 'chrome', {get: () => ({runtime: {}})})")
            self.driver.execute_script("""
                Object.defineProperty(screen, 'availWidth', {get: () => 1920});
                Object.defineProperty(screen, 'availHeight', {get: () => 1040});
                Object.defineProperty(screen, 'width', {get: () => 1920});
                Object.defineProperty(screen, 'height', {get: () => 1080});
                Object.defineProperty(screen, 'colorDepth', {get: () => 24});
            """)
            tz_offset = random.choice([-60, -60, -120, -120, -120, -180])
            self.driver.execute_script(f"Date.prototype.getTimezoneOffset = () => {tz_offset};")
        except Exception as e:
            print(f"⚠ Warning: Some anti-detection scripts failed: {e}")

        ublock_xpi = self._get_ublock_xpi()
        if ublock_xpi:
            try:
                self.driver.install_addon(ublock_xpi, temporary=True)
                print('✓ uBlock Origin installed')
                time.sleep(self.get_timing_float('addon_init_delay', 0.5))
            except Exception as e:
                print(f'⚠ Failed to install uBlock Origin: {e}')

        self.inject_aggressive_adblock()
        self.inject_popup_killer()

    # ==================== AD-BLOCKING & POPUP KILLER ====================

    def human_delay(self, min_sec=0.5, max_sec=2.0):
        """Add random human-like delay"""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)

    def _wait_for_page_ready(self, driver=None, timeout=None):
        """Wait for page to be fully loaded before parsing."""
        drv = driver or self.driver
        if timeout is None:
            timeout = self.get_timing_float('page_ready_timeout', 10.0)
        body_timeout = self.get_timing_float('page_ready_body_timeout', timeout)
        try:
            WebDriverWait(drv, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
        except Exception:
            logger.debug("Timeout waiting for document.readyState == 'complete'")
        try:
            WebDriverWait(drv, min(body_timeout, timeout)).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
        except Exception:
            pass

    def inject_aggressive_adblock(self, driver=None):
        """Inject aggressive CSS to hide ads, popups, and tracking elements"""
        drv = driver or self.driver
        try:
            adblock_css = """
                [class*="ad-"], [id*="ad-"],
                [class*="ads"], [id*="ads"],
                [class*="advertisement"], [id*="advertisement"],
                [class*="advert"], [id*="advert"],
                [class*="banner"], [id*="banner"],
                [class*="popup"], [id*="popup"],
                [class*="modal"], [id*="modal"],
                [class*="overlay"], [id*="overlay"],
                [data-type="ad"], [data-ad-slot],
                .adsense, #adsense,
                .doubleclick, #doubleclick,
                .google-ads, #google-ads,
                iframe[src*="ads"],
                iframe[src*="doubleclick"],
                iframe[src*="google"],
                iframe[src*="adsense"],
                iframe[src*="banner"],
                .sponsor, [class*="sponsor"],
                .commercial, [class*="commercial"],
                .promo, [class*="promo"]
                {
                    display: none !important;
                    visibility: hidden !important;
                    height: 0 !important;
                    margin: 0 !important;
                    padding: 0 !important;
                    border: 0 !important;
                }
                div.popup, div.modal, div.overlay,
                .dialogBox, .notification-box,
                .alert, .alert-box
                {
                    display: none !important;
                }
            """
            drv.execute_script(f"""
                var style = document.createElement('style');
                style.textContent = `{adblock_css}`;
                document.head.appendChild(style);
            """)
        except Exception as e:
            logger.debug(f"Ad-blocking CSS injection failed: {e}")

    def inject_popup_killer(self, driver=None):
        """Inject JavaScript to automatically close popups and modals"""
        drv = driver or self.driver
        try:
            popup_killer_script = """
                function killPopups() {
                    document.querySelectorAll('iframe').forEach(iframe => {
                        const classList = iframe.className || '';
                        const id = iframe.id || '';
                        if (classList.includes('container-') ||
                            classList.includes('ad') ||
                            id.includes('container-') ||
                            id.includes('ad')) {
                            iframe.style.display = 'none';
                            iframe.remove();
                        }
                    });
                    document.querySelectorAll('.modal').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    document.querySelectorAll('[class*="overlay"]').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    document.querySelectorAll('[class*="popup"]').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    window.open = function() { return null; };
                    window.alert = function() { return null; };
                    window.confirm = function() { return true; };
                    window.onbeforeunload = null;
                }
                killPopups();
                if (document.readyState !== 'loading') {
                    killPopups();
                } else {
                    document.addEventListener('DOMContentLoaded', killPopups);
                }
                const observer = new MutationObserver(killPopups);
                observer.observe(document.body, { childList: true, subtree: true });
            """
            drv.execute_script(popup_killer_script)
        except Exception as e:
            logger.debug(f"Popup killer injection failed: {e}")

    def _is_driver_alive(self, driver=None):
        """Check if a WebDriver session is still usable."""
        drv = driver or self.driver
        if drv is None:
            return False
        try:
            _ = drv.current_url
            return True
        except Exception:
            return False

    def close(self):
        """Close the browser with timeout to prevent hangs"""
        if self.driver:
            try:
                driver_ref = self.driver

                def _force_kill():
                    logger.warning("Driver quit timeout, forcing kill")
                    try:
                        if driver_ref.service and driver_ref.service.process:
                            driver_ref.service.process.kill()
                    except Exception:
                        pass

                timer = threading.Timer(DRIVER_QUIT_TIMEOUT, _force_kill)
                timer.start()
                try:
                    self.driver.quit()
                finally:
                    timer.cancel()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}, forcing close")
            print("✓ Browser closed")

    def _flush_series_data_to_disk(self):
        """Append current series_data to JSONL file and clear in-memory list."""
        if not self.series_data:
            return
        try:
            with open(self._partial_data_file, 'a', encoding='utf-8') as f:
                for entry in self.series_data:
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
            self._flushed_count += len(self.series_data)
            logger.debug(f"Flushed {len(self.series_data)} series to disk (total flushed: {self._flushed_count})")
            self.series_data = []
        except Exception as e:
            logger.error(f"Failed to flush series data to disk: {e}")

    def _load_flushed_series_data(self):
        """Read back all flushed entries from the JSONL file and merge with in-memory data."""
        merged = list(self.series_data)
        if os.path.exists(self._partial_data_file):
            try:
                with open(self._partial_data_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            merged.append(json.loads(line))
                logger.debug(f"Loaded {len(merged) - len(self.series_data)} flushed entries from disk")
            except Exception as e:
                logger.error(f"Failed to load flushed series data: {e}")
        self._cleanup_partial_file()
        return merged

    def _cleanup_partial_file(self):
        """Remove the partial JSONL flush file."""
        try:
            if os.path.exists(self._partial_data_file):
                os.remove(self._partial_data_file)
        except OSError:
            pass

    def _cleanup_memory(self):
        """Clear in-memory data to prevent memory leaks."""
        with self._lock:
            if len(self.completed_links) > 10000:
                self.completed_links = set(
                    s.get('url', '').rstrip('/').split('/')[-1]
                    for s in self.series_data if s.get('url')
                )
            if len(self.series_data) > 200:
                self._flush_series_data_to_disk()

    def _has_auth_cookies(self, driver):
        """Lightweight auth check: verify session cookies exist."""
        try:
            cookies = driver.get_cookies()
            cookie_names = {c['name'] for c in cookies}
            # aniworld.to uses Laravel — check for Laravel session cookies
            session_indicators = {'laravel_session', 'XSRF-TOKEN', 'remember_web'}
            if cookie_names & session_indicators:
                return True
            # Fallback: if we have 2+ cookies on the aniworld.to domain, session is likely alive
            site_domain = urlparse(self.get_site_url()).hostname
            domain_cookies = [c for c in cookies if site_domain in (c.get('domain', '') or '')]
            if len(domain_cookies) >= 2:
                return True
            logger.debug(f"_has_auth_cookies: no session indicators found. Cookies: {cookie_names}")
            return False
        except Exception:
            return False

    def is_logged_in(self, driver):
        """Check if authenticated by looking for the user profile avatar."""
        try:
            login_config = self.get_selector('login') or {}
            indicator = login_config.get('logged_in_indicator', "div.avatar a[href^='/user/profil/']")
            return len(driver.find_elements(By.CSS_SELECTOR, indicator)) > 0
        except Exception:
            return False

    def login(self, driver=None, retry_count=0, max_retries=2):
        """Login to aniworld.to using email/password via JS injection."""
        drv = driver or self.driver
        try:
            login_config = self.get_selector('login')
            if not login_config:
                raise Exception("Login config not found")

            login_page = self.get_login_page()
            self._throttle_request()
            drv.get(login_page)
            self._wait_for_page_ready(drv)

            page_source = drv.page_source
            server_error = self.check_server_error(page_source)
            if server_error:
                self._record_server_error()
                raise Exception(f"Login page returned {server_error}")

            self.inject_popup_killer(drv)
            self.inject_aggressive_adblock(drv)

            # Wait for submit button
            WebDriverWait(drv, self.get_timing_float('element_timeout', 15.0)).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='submit'], button[type='submit']"))
            )

            email_field = self.find_element_from_config(
                drv,
                login_config.get('username_field', []),
                timeout=self.get_timing_float('element_timeout', 15.0)
            )
            if not email_field:
                raise Exception("Email field not found")

            password_field = self.find_element_from_config(
                drv,
                login_config.get('password_field', []),
                timeout=self.get_timing_float('element_timeout', 15.0)
            )
            if not password_field:
                raise Exception("Password field not found")

            old_html = drv.find_element(By.TAG_NAME, 'html')

            form_submit_js = """
            arguments[0].value = arguments[2];
            arguments[0].dispatchEvent(new Event('input', {bubbles: true}));
            arguments[1].value = arguments[3];
            arguments[1].dispatchEvent(new Event('input', {bubbles: true}));
            arguments[4].click();
            """

            submit_button = self.find_element_from_config(
                drv,
                login_config.get('submit_button', []),
                timeout=self.get_timing_float('element_timeout', 15.0)
            )

            drv.execute_script(form_submit_js, email_field, password_field, self.email, self.password, submit_button)

            WebDriverWait(drv, self.get_timing_float('login_response_timeout', 10.0)).until(EC.staleness_of(old_html))
            self._wait_for_page_ready(drv, timeout=self.get_timing_float('login_response_timeout', 10.0))

            self.inject_popup_killer(drv)
            self.inject_aggressive_adblock(drv)

            self.auth_cookies = drv.get_cookies()

            if self.is_logged_in(drv):
                logger.info("Login successful")
                return True

            raise Exception("Login completed but verification failed")

        except Exception as e:
            if retry_count < max_retries:
                logger.warning(f"Login attempt {retry_count + 1}/{max_retries} failed: {e}")
                print(f"→ Retrying login ({retry_count + 1}/{max_retries})...")
                drv.delete_all_cookies()
                self.human_delay(2, 5)
                return self.login(drv, retry_count + 1, max_retries)
            else:
                logger.error(f"Login failed after {max_retries} retries: {e}")
                print("✗ Max login retries exceeded")
                raise

    # ==================== SERIES DISCOVERY ====================

    def get_all_series(self):
        """Get list of all anime from the /animes index page."""
        try:
            print("→ Fetching list of all anime...")
            site_url = self.get_site_url()
            idx_sel = self.get_selector('series_index') or {}
            idx_path = idx_sel.get('path', '/animes')
            all_series_url = f"{site_url}{idx_path}"

            self.driver.get(all_series_url)
            self._wait_for_page_ready()

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            series_list = []
            seen = set()
            link_selector = idx_sel.get('all_links', '#seriesContainer ul li a')
            alt_title_attr = idx_sel.get('alt_title_attr', 'data-alternative-title')

            for link in soup.select(link_selector):
                href = link.get('href', '')
                m = _ANIME_SLUG_RE.match(href)
                if not m:
                    continue
                slug = m.group(1)
                if slug in seen:
                    continue
                seen.add(slug)
                title = link.get_text(strip=True)
                if not title:
                    continue

                # Extract alternative titles from data attribute (primary source)
                alt_titles_raw = link.get(alt_title_attr, '')
                alt_titles = [t.strip() for t in alt_titles_raw.split(',') if t.strip()] if alt_titles_raw else []

                series_list.append({
                    'title': title,
                    'link': f"/anime/stream/{slug}",
                    'url': f"{site_url}/anime/stream/{slug}",
                    'slug': slug,
                    'alt_titles': alt_titles,
                })

            print(f"✓ Found {len(series_list)} unique anime")
            return series_list

        except Exception as e:
            print(f"✗ Failed to fetch anime index: {str(e)}")
            logger.error(f"Failed to fetch anime index: {e}")
            raise

    def get_account_series(self, source='both'):
        """Discover user's subscribed/watchlist series from account pages."""
        site_url = self.get_site_url()
        acct = self.get_selector('account_pages') or {}
        all_pages = [
            (acct.get('subscribed', '/account/subscribed'), 'Subscriptions'),
            (acct.get('watchlist', '/account/watchlist'), 'Watchlist'),
        ]
        if source == 'subscribed':
            account_pages = [all_pages[0]]
        elif source == 'watchlist':
            account_pages = [all_pages[1]]
        else:
            account_pages = [all_pages[0], all_pages[1]]

        if source == 'both' and len(account_pages) == 2:
            return self._get_account_series_parallel(account_pages, site_url)

        return self._get_account_series_sequential(account_pages, site_url, self.driver)

    def _fetch_account_page_series(self, page_path, label, site_url, driver):
        """Fetch all series from a single account page (no pagination on aniworld.to)."""
        series_list = []
        seen_slugs = set()
        url = f"{site_url}{page_path}"
        try:
            driver.get(url)
            self._wait_for_page_ready(driver)

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            acct_sel = self.get_selector('account_series') or {}
            entry_selector = acct_sel.get('entries', 'div.seriesListContainer > div a')
            title_selector = acct_sel.get('title', 'h3')

            for entry in soup.select(entry_selector):
                href = entry.get('href', '')
                m = _ANIME_SLUG_RE.match(href)
                if not m:
                    continue
                slug = m.group(1)
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                title_el = entry.select_one(title_selector)
                title = title_el.get_text(strip=True) if title_el else slug

                series_list.append({
                    'title': title,
                    'link': f"/anime/stream/{slug}",
                    'url': f"{site_url}/anime/stream/{slug}",
                    'slug': slug,
                })

        except Exception as e:
            logger.warning(f"Could not scan {url}: {e}")

        print(f"  ✓ {label}: {len(series_list)} series found")
        return series_list

    def _get_account_series_parallel(self, account_pages, site_url):
        """Fetch subscribed and watchlist pages in parallel using 2 worker browsers."""
        print("→ Fetching subscribed & watchlist in parallel (2 workers)...")

        results = [None, None]
        errors = [None, None]

        def worker(worker_id, page_path, label):
            driver = None
            try:
                driver = self._create_worker_driver(worker_id=f"discovery_{worker_id}")
                self.inject_aggressive_adblock(driver)
                authenticated = self._authenticate_driver(driver, label=f"Discovery worker {worker_id}")
                if not authenticated:
                    errors[worker_id] = f"Failed to authenticate discovery worker {worker_id}"
                    logger.error(errors[worker_id])
                    return
                results[worker_id] = self._fetch_account_page_series(page_path, label, site_url, driver)
            except Exception as e:
                errors[worker_id] = str(e)
                logger.error(f"Discovery worker {worker_id} failed: {e}")
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

        threads = []
        for idx, (page_path, label) in enumerate(account_pages):
            t = threading.Thread(target=worker, args=(idx, page_path, label))
            threads.append(t)
            t.start()
            time.sleep(self.get_timing_float('discovery_worker_stagger_delay', 1.0, max_val=30.0))

        for t in threads:
            t.join()

        for idx, (page_path, label) in enumerate(account_pages):
            if results[idx] is None:
                if errors[idx]:
                    print(f"  ⚠ {label} parallel fetch failed: {errors[idx]}")
                    print(f"  → Falling back to main browser for {label}...")
                results[idx] = self._fetch_account_page_series(page_path, label, site_url, self.driver)

        seen = set()
        series_list = []
        for source_results in results:
            if source_results:
                for item in source_results:
                    slug = item.get('slug', item['url'].rstrip('/').split('/')[-1])
                    if slug not in seen:
                        seen.add(slug)
                        item.pop('slug', None)
                        series_list.append(item)

        logger.info(f"Account series discovery (both, parallel): found {len(series_list)} unique series")
        print(f"\n  Total unique series discovered: {len(series_list)}")
        return series_list

    def _get_account_series_sequential(self, account_pages, site_url, driver):
        """Fetch account series sequentially using a single driver."""
        seen = set()
        series_list = []

        for page_path, label in account_pages:
            page_series = self._fetch_account_page_series(page_path, label, site_url, driver)
            for item in page_series:
                slug = item.get('slug', item['url'].rstrip('/').split('/')[-1])
                if slug not in seen:
                    seen.add(slug)
                    item.pop('slug', None)
                    series_list.append(item)

        logger.info(f"Account series discovery: found {len(series_list)} unique series")
        print(f"\n  Total unique series discovered: {len(series_list)}")
        return series_list

    # ==================== SERIES & EPISODE SCRAPING ====================

    _ERROR_TITLE_RE = re.compile(
        r'^(?:Error\s+)?(?P<code>\d{3})\b|\b(?:Error|Fehler)\s+(?P<code2>\d{3})\b',
        re.IGNORECASE,
    )

    def check_series_not_found_error(self, html):
        """Check if page contains error message for series not found."""
        soup = BeautifulSoup(html, 'html.parser')
        error_div = soup.find('div', class_='messageBox error')
        if error_div:
            error_text = error_div.get_text(strip=True)
            if 'nicht gefunden' in error_text.lower():
                return error_text
        # If the page has series content (season links), it's a real series page
        # — not an error page. This prevents false positives for series named
        # "Error 404", "Fehler 404", etc.
        if soup.select_one('#stream a[href*="/staffel-"]'):
            return None
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            m = self._ERROR_TITLE_RE.search(title_text)
            if m:
                code = m.group('code') or m.group('code2')
                if code == '404':
                    return title_text
        h2_tag = soup.find('h2')
        if h2_tag and h2_tag.get_text(strip=True) == '404':
            p_tag = soup.find('p')
            return p_tag.get_text(strip=True) if p_tag else '404 Nicht gefunden'
        return None

    _SERVER_ERROR_CODES = {
        '429': '429 Too Many Requests',
        '500': '500 Internal Server Error',
        '502': '502 Bad Gateway',
        '503': '503 Service Unavailable',
        '504': '504 Gateway Timeout',
    }

    def check_server_error(self, html):
        """Check if page contains a server error."""
        soup = BeautifulSoup(html, 'html.parser')
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            m = self._ERROR_TITLE_RE.search(title_text)
            if m:
                code = m.group('code') or m.group('code2')
                if code in self._SERVER_ERROR_CODES:
                    return self._SERVER_ERROR_CODES[code]
        body_text = soup.get_text(strip=True) if soup.body else ''
        for code, message in self._SERVER_ERROR_CODES.items():
            reason = message.split(' ', 1)[1]
            if code in body_text and reason in body_text:
                return message
        return None

    def scrape_series_detail(self, series_url, driver=None, max_retries=3, skip_subscription=False):
        """
        Scrape all episodes from a single season page with retry logic.

        Returns dict with season number, URL, episodes list, subscription status, and title.
        Adapted for aniworld.to HTML structure.
        """
        drv = driver or self.driver
        if not self._is_driver_alive(drv):
            logger.error(f"Driver is dead before scraping {series_url}")
            return None

        for attempt in range(max_retries):
            try:
                self._throttle_request()
                drv.get(series_url)
                self._wait_for_page_ready(drv)
                self.inject_popup_killer(drv)

                # Wait for subscription container if needed
                if not skip_subscription:
                    try:
                        sub_sel = self.get_selector('subscription') or {}
                        container_css = sub_sel.get('container', 'div.add-series')
                        WebDriverWait(drv, self.get_timing_float('element_find_timeout', 2.0)).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, container_css))
                        )
                    except Exception:
                        logger.debug(f"Subscription container not found within timeout for {series_url}")

                page_source = drv.page_source

                # Detect browser error pages
                try:
                    current_url = drv.current_url or ''
                except Exception:
                    current_url = ''
                if 'neterror' in current_url or 'dnsNotFound' in current_url or current_url.startswith('about:'):
                    raise Exception(f"Browser error page: {current_url}")
                if page_source and ('Die Verbindung mit dem Server' in page_source or 'dnsNotFound' in page_source):
                    raise Exception(f"Network error page for: {series_url}")

                server_error = self.check_server_error(page_source)
                if server_error:
                    self._record_server_error()
                    raise Exception(f"{server_error}: {series_url}")

                error_found = self.check_series_not_found_error(page_source)
                if error_found:
                    logger.warning(f"Series not found: {series_url} — {error_found}")
                    print(f"  ✗ Series not found: {series_url} - {error_found}")
                    return None

                soup = BeautifulSoup(page_source, 'html.parser')

                # Verify logged-in state
                login_config = self.get_selector('login') or {}
                logged_in_sel = login_config.get('logged_in_indicator', "div.avatar a[href^='/user/profil/']")
                if not soup.select_one(logged_in_sel):
                    logger.warning(f"Session expired (no profile avatar on page): {series_url}")
                    return None

                # Extract season number from URL
                staffel_match = _STAFFEL_RE.search(series_url)
                filme_match = _FILME_RE.search(series_url)
                if staffel_match:
                    season = staffel_match.group(1)
                elif filme_match:
                    season = 'Filme'
                else:
                    season = '1'

                # Parse episodes from table
                episodes = []
                sel = self.get_selector('series_detail') or {}
                episode_row_selector = sel.get('episode_rows', 'table.seasonEpisodesList tbody tr[data-episode-id]')
                episode_rows = soup.select(episode_row_selector)

                if not episode_rows:
                    # Fallback selectors
                    for fallback in ['table.seasonEpisodesList tr[data-episode-id]', 'tbody tr[data-episode-id]']:
                        episode_rows = soup.select(fallback)
                        if episode_rows:
                            break

                if not episode_rows:
                    logger.warning(f"No episode rows found for {series_url}")

                lang_map = self.config.get('language_map', {})
                malformed_count = 0

                for row_idx, row in enumerate(episode_rows, start=1):
                    try:
                        # Episode number from meta tag
                        ep_num_el = row.select_one(sel.get('episode_number', "meta[itemprop='episodeNumber']"))
                        ep_num_attr = sel.get('episode_number_attr', 'content')
                        if ep_num_el:
                            episode_num = ep_num_el.get(ep_num_attr, '') if ep_num_attr != 'text' else ep_num_el.get_text(strip=True)
                        else:
                            episode_num = ''
                        if not episode_num:
                            # Filme/movie pages lack meta episodeNumber — fall back to
                            # data-episode-season-id (the ordinal film/episode number
                            # within a season, e.g. 1, 2, 3), then 1-based row index.
                            episode_num = row.get('data-episode-season-id', '')
                        if not episode_num:
                            episode_num = str(row_idx)
                            logger.debug(f"Using row index {row_idx} as episode number for {series_url}")

                        # German title
                        ger_el = row.select_one(sel.get('title_ger', 'td.seasonEpisodeTitle a strong'))
                        title_ger = ger_el.get_text(strip=True) if ger_el else ''

                        # English title (strip [Episode NNN] suffix)
                        eng_el = row.select_one(sel.get('title_eng', 'td.seasonEpisodeTitle a span'))
                        title_eng = eng_el.get_text(strip=True) if eng_el else ''
                        title_eng = _EPISODE_LABEL_RE.sub('', title_eng).strip()

                        # Watched status from row class
                        watched_class = sel.get('watched_class', 'seen')
                        is_watched = watched_class in row.get('class', [])

                        # Language flags
                        flag_selector = sel.get('language_flags', 'td.editFunctions img.flag')
                        flag_imgs = row.select(flag_selector)
                        languages = []
                        for img in flag_imgs:
                            src = img.get('src', '')
                            title_attr = img.get('title', '')
                            # Extract filename from src path
                            flag_file = src.rsplit('/', 1)[-1] if '/' in src else src
                            lang = lang_map.get(flag_file, title_attr or flag_file)
                            if lang:
                                languages.append(lang)

                        ep_data = {
                            'number': episode_num,
                            'title_ger': title_ger,
                            'title_eng': title_eng,
                            'watched': is_watched,
                        }
                        if languages:
                            ep_data['languages'] = languages

                        episodes.append(ep_data)
                    except Exception as e:
                        malformed_count += 1
                        logger.warning(f"Malformed episode row {row_idx} in {series_url}: {e}")
                        continue

                # Detect subscription status
                if skip_subscription:
                    subscribed, watchlist = None, None
                else:
                    subscribed, watchlist = self.detect_subscription_status(soup)

                # Series title
                title_selector = sel.get('series_title', "h1[itemprop='name'] > span")
                title_element = soup.select_one(title_selector)
                series_title = title_element.get_text(strip=True) if title_element else None

                # Alternative titles from detail page (secondary source)
                alt_titles = []
                alt_el = soup.select_one(sel.get('alt_titles_element', "h1[itemprop='name']"))
                if alt_el:
                    alt_raw = alt_el.get(sel.get('alt_titles_attr', 'data-alternativetitles'), '')
                    if alt_raw:
                        alt_titles = [t.strip() for t in alt_raw.split(',') if t.strip()]

                # Description
                desc_el = soup.select_one(sel.get('description', 'p.seri_des'))
                description = ''
                if desc_el:
                    description = desc_el.get(sel.get('description_attr', 'data-full-description'), '')

                watched_count = sum(1 for ep in episodes if ep['watched'])

                logger.debug(f"Scraped {series_url}: {len(episodes)} episodes ({watched_count} watched), title={series_title}")

                if len(episodes) == 0:
                    logger.warning(f"0 episodes found for {series_url} — treating as failed")
                    return None

                # Cross-validate episode count: nav links vs table rows
                ep_nav_selector = sel.get('episode_nav_links', "#stream ul:nth-of-type(2) li a[data-episode-id]")
                episode_nav_links = soup.select(ep_nav_selector)
                expected_episode_count = len(episode_nav_links)
                if expected_episode_count > 0 and len(episodes) != expected_episode_count:
                    logger.warning(
                        f"Episode count mismatch for {series_url}: "
                        f"nav links={expected_episode_count}, table rows={len(episodes)} — "
                        f"treating as incomplete"
                    )
                    return None

                # Cross-validate watched count: nav seen vs table seen
                ep_nav_watched_selector = sel.get('episode_nav_watched', "#stream ul:nth-of-type(2) li a.seen")
                nav_watched_count = len(soup.select(ep_nav_watched_selector))
                if expected_episode_count > 0 and nav_watched_count != watched_count:
                    logger.warning(
                        f"Watched count mismatch for {series_url}: "
                        f"nav seen={nav_watched_count}, table seen={watched_count} — "
                        f"trusting table rows"
                    )

                validated_episodes = validate_episode_data(episodes)
                watched_count = sum(1 for ep in validated_episodes if ep['watched'])

                self._decay_global_backoff()
                result = {
                    'season': season,
                    'url': series_url,
                    'episodes': validated_episodes,
                    'watched_episodes': watched_count,
                    'total_episodes': len(validated_episodes),
                    'subscribed': subscribed,
                    'watchlist': watchlist,
                    'title': series_title,
                    'alt_titles': alt_titles,
                    'description': description,
                }
                if malformed_count > 0:
                    result['_malformed_episodes'] = malformed_count
                return result

            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    if '429' in error_msg or '502' in error_msg or '503' in error_msg:
                        self._record_server_error()
                        base = self.get_timing_float('error_backoff_502_base', 2.0)
                        cap = self.get_timing_float('error_backoff_502_max', 10.0)
                        backoff = min(cap, base * (2 ** attempt))
                        logger.warning(f"Server overload on {series_url}, backing off {backoff:.1f}s")
                    else:
                        backoff = min(self.get_timing_float('error_backoff_max', 15.0),
                                      self.get_timing_float('error_backoff_base', 2.0) * (2 ** attempt))
                    logger.warning(f"Retry {attempt + 2}/{max_retries} for {series_url}: {e}")
                    print(f"  ⚠ Retrying {series_url} (attempt {attempt + 2}/{max_retries}): {str(e)[:50]}")
                    time.sleep(backoff + random.uniform(0.5, 1.5))
                else:
                    logger.error(f"Failed to scrape {series_url} after {max_retries} attempts: {e}")
                    print(f"  ✗ Failed to scrape {series_url} after {max_retries} attempts: {str(e)[:80]}")
                    return None

    def detect_subscription_status(self, soup):
        """
        Detect if user has series subscribed and/or on watchlist.

        Uses aniworld.to's data attributes as primary source with CSS class cross-validation.

        Returns:
            tuple: (subscribed: bool|None, watchlist: bool|None)
        """
        try:
            subscribed = None
            watchlist = None

            # Verify logged-in state
            login_config = self.get_selector('login') or {}
            logged_in_sel = login_config.get('logged_in_indicator', "div.avatar a[href^='/user/profil/']")
            if not soup.select_one(logged_in_sel):
                logger.warning("No profile avatar — session likely expired, subscription status unreliable")
                return (None, None)

            sub_sel = self.get_selector('subscription') or {}
            container_selector = sub_sel.get('container', 'div.add-series')
            container = soup.select_one(container_selector)

            if not container:
                logger.warning("No subscription container found — cannot determine Sub/WL status")
                return (None, None)

            # Primary: data attributes (most reliable)
            fav_attr = sub_sel.get('favourite_attr', 'data-series-favourite')
            wl_attr = sub_sel.get('watchlist_attr', 'data-series-watchlist')
            fav_val = container.get(fav_attr)
            wl_val = container.get(wl_attr)

            if fav_val is not None:
                subscribed = fav_val == '1'
            if wl_val is not None:
                watchlist = wl_val == '1'

            # Cross-validate with CSS classes
            fav_active_sel = sub_sel.get('favourite_active', 'li.setFavourite.true')
            wl_active_sel = sub_sel.get('watchlist_active', 'li.setWatchlist.true')
            css_subscribed = soup.select_one(fav_active_sel) is not None
            css_watchlist = soup.select_one(wl_active_sel) is not None

            if subscribed is not None and css_subscribed != subscribed:
                logger.warning(f"Subscribe mismatch: data-attr={subscribed}, CSS class={css_subscribed} — trusting data attribute")
            if watchlist is not None and css_watchlist != watchlist:
                logger.warning(f"Watchlist mismatch: data-attr={watchlist}, CSS class={css_watchlist} — trusting data attribute")

            # Fallback to CSS if data attributes missing
            if subscribed is None:
                subscribed = css_subscribed
            if watchlist is None:
                watchlist = css_watchlist

            return (subscribed, watchlist)

        except Exception as e:
            print(f"    ⚠ Error detecting subscription status: {str(e)[:50]}")
            return (None, None)

    # ==================== SEASON DETECTION ====================

    def get_series_slug_from_url(self, url):
        """Extract series slug from full URL or relative path."""
        try:
            if url.startswith('http'):
                path = urlparse(url).path
            else:
                path = url

            parts = path.split('/')
            # /anime/stream/{slug}
            if 'stream' in parts:
                idx = parts.index('stream')
                if idx + 1 < len(parts):
                    return parts[idx + 1]

            logger.error(f"Could not extract slug from URL: {url}")
            raise ValueError(f"URL does not contain '/anime/stream/{{slug}}': {url}")
        except Exception as e:
            logger.error(f"Error extracting slug from URL '{url}': {e}")
            raise ValueError(f"Invalid series URL: {url}")

    def _extract_seasons_from_soup(self, soup, series_slug):
        """Extract season numbers from a parsed page's season navigation."""
        nav_sel = self.get_selector('season_nav') or {}
        pill_selector = nav_sel.get('pills', "#stream ul:first-of-type li a[href*='/staffel-']")
        movies_selector = nav_sel.get('movies_pill', "#stream ul:first-of-type li a[href*='/filme']")

        seasons = []
        seen = set()

        # Extract season numbers from href
        season_links = soup.select(pill_selector)
        for link in season_links:
            href = link.get('href', '')
            m = _STAFFEL_RE.search(href)
            if m:
                season_num = m.group(1)
                if season_num not in seen:
                    seen.add(season_num)
                    seasons.append(season_num)

        # Check for movies/filme
        movies_links = soup.select(movies_selector)
        if movies_links and 'Filme' not in seen:
            seen.add('Filme')
            seasons.append('Filme')

        return seasons

    def get_all_seasons_for_series(self, series_slug, driver=None, max_retries=None):
        """
        Detect all available seasons for a series from the season navigation.

        Returns list of season identifiers (e.g., ['1', '2', 'Filme']).
        """
        drv = driver or self.driver
        if max_retries is None:
            val = self.get_timing('max_retries_season', default=None)
            max_retries = int(val) if val is not None else 3

        last_error = None
        for attempt in range(max_retries):
            try:
                base_url = f"https://aniworld.to/anime/stream/{series_slug}"
                self._throttle_request()
                drv.get(base_url)
                self._wait_for_page_ready(drv)
                self.inject_popup_killer(drv)

                try:
                    current_url = drv.current_url or ''
                except Exception:
                    current_url = ''
                if 'neterror' in current_url or 'dnsNotFound' in current_url or current_url.startswith('about:'):
                    last_error = f"Browser error page for {series_slug}: {current_url}"
                    logger.warning(f"Attempt {attempt + 1}/{max_retries}: {last_error}")
                    if attempt < max_retries - 1:
                        time.sleep(self.get_timing_float('season_detection_browser_error_delay', 2.0, max_val=15.0))
                        continue
                    raise SeasonDetectionError(last_error)

                # Wait for season nav
                nav_wait = self.get_timing_float('season_nav_wait', 10.0)
                nav_found = False
                try:
                    WebDriverWait(drv, nav_wait).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '#stream'))
                    )
                    nav_found = True
                except Exception:
                    pass

                page_source = drv.page_source

                if not nav_found:
                    server_error = self.check_server_error(page_source)
                    if server_error:
                        self._record_server_error()
                        last_error = f"{server_error} on season detection for {series_slug}"
                        logger.warning(f"Attempt {attempt + 1}/{max_retries}: {last_error}")
                        if attempt < max_retries - 1:
                            base = self.get_timing_float('error_backoff_502_base', 2.0)
                            cap = self.get_timing_float('error_backoff_502_max', 10.0)
                            backoff = min(cap, base * (2 ** attempt))
                            time.sleep(backoff + random.uniform(0.5, 1.5))
                            continue
                        raise SeasonDetectionError(last_error)

                    error_found = self.check_series_not_found_error(page_source)
                    if error_found:
                        logger.warning(f"Series not found during season detection: {series_slug} — {error_found}")
                        raise SeasonDetectionError(f"Series not found: {series_slug} — {error_found}")

                soup = BeautifulSoup(page_source, 'html.parser')
                seasons = self._extract_seasons_from_soup(soup, series_slug)

                if seasons:
                    logger.debug(f"Detected {len(seasons)} seasons for {series_slug}: {seasons}")
                    self._decay_global_backoff()
                    return seasons

                last_error = f"No season links found for {series_slug}"
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: {last_error}")
                if attempt < max_retries - 1:
                    retry_delay = self.get_timing_float('season_detection_retry_delay', 1.0, max_val=30.0)
                    retry_jitter = self.get_timing_float('season_detection_retry_jitter', 0.5, max_val=5.0)
                    time.sleep(retry_delay + random.uniform(0, retry_jitter))
                    continue

                raise SeasonDetectionError(
                    f"Season detection failed for {series_slug} after {max_retries} attempts: {last_error}"
                )

            except SeasonDetectionError:
                raise
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Attempt {attempt + 1}/{max_retries} season detection for {series_slug}: {e}")
                if attempt < max_retries - 1:
                    backoff = min(self.get_timing_float('error_backoff_max', 8.0),
                                  self.get_timing_float('error_backoff_base', 1.0) * (2 ** attempt))
                    time.sleep(backoff + random.uniform(0, 0.5))
                    continue
                raise SeasonDetectionError(
                    f"Season detection failed for {series_slug} after {max_retries} attempts: {last_error}"
                )

    # ==================== MAIN SCRAPING ORCHESTRATION ====================

    def _create_worker_driver(self, worker_id=None):
        """Create a new WebDriver for a worker thread and track its PID."""
        firefox_options = self._build_firefox_options()
        service = FirefoxService()
        driver = None
        try:
            driver = webdriver.Firefox(service=service, options=firefox_options)
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

            ublock_xpi = self._get_ublock_xpi()
            if ublock_xpi:
                try:
                    driver.install_addon(ublock_xpi, temporary=True)
                    time.sleep(self.get_timing_float('worker_addon_init_delay', 0.3))
                except Exception:
                    pass

            if worker_id is not None:
                for attempt in range(5):
                    try:
                        if hasattr(service, 'process') and service.process:
                            geckodriver_pid = service.process.pid
                            if geckodriver_pid:
                                self.save_worker_pid(worker_id, geckodriver_pid)
                                try:
                                    result = subprocess.run(
                                        ['wmic', 'process', 'where', f'ParentProcessId={geckodriver_pid}',
                                         'get', 'ProcessId', '/value'],
                                        capture_output=True, text=True,
                                        encoding='utf-8', errors='replace',
                                        timeout=self.get_timing_float('process_lookup_timeout', 2.0), check=False
                                    )
                                    for line in result.stdout.split('\n'):
                                        if 'ProcessId' in line:
                                            firefox_pid = line.split('=')[-1].strip()
                                            if firefox_pid.isdigit():
                                                self.save_worker_pid(f"{worker_id}_firefox", int(firefox_pid))
                                except Exception:
                                    pass
                                break
                    except Exception:
                        pass
                    time.sleep(self.get_timing_float('worker_service_init_delay', 0.1))

            return driver
        except Exception as e:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
            elif hasattr(service, 'process') and service.process:
                try:
                    service.stop()
                except Exception:
                    pass
            raise Exception(f"Failed to create worker driver: {str(e)}")

    def _restart_worker_driver(self, worker_id, old_driver):
        """Restart a worker's browser and re-authenticate.

        Args:
            worker_id: Worker number for logging.
            old_driver: The crashed/stale driver to quit.

        Returns:
            tuple: (new_driver, success) — new_driver is None on failure.
        """
        try:
            old_driver.quit()
        except Exception:
            pass
        try:
            driver = self._create_worker_driver(worker_id)
            self.inject_aggressive_adblock(driver)
            if not self._authenticate_driver(driver, label=f"W{worker_id}", max_attempts=2):
                logger.error(f"W{worker_id}: Re-auth failed after driver restart")
                try:
                    driver.quit()
                except Exception:
                    pass
                return None, False
            logger.info(f"W{worker_id}: Driver restarted and re-authenticated")
            return driver, True
        except Exception as e:
            logger.error(f"W{worker_id}: Failed to restart driver: {e}", exc_info=True)
            return None, False

    def _worker_health_check(self, worker_id, driver, error_streak):
        """Perform a health check on a worker driver and re-authenticate if needed.

        Args:
            worker_id: Worker number for logging.
            driver: The worker's WebDriver.
            error_streak: Current consecutive error count.

        Returns:
            tuple: (error_streak, driver_alive) — driver_alive is False if driver is dead.
        """
        if not self._is_driver_alive(driver):
            return error_streak, False
        if not self._has_auth_cookies(driver):
            logger.warning(f"W{worker_id}: Health check failed — re-authenticating")
            if self._authenticate_driver(driver, label=f"W{worker_id}", max_attempts=2):
                error_streak = 0
        return error_streak, True

    def save_worker_pid(self, worker_id, pid):
        """Save worker geckodriver PID for cleanup (atomic write)."""
        with self._lock:
            try:
                pids = {'_owner_pid': os.getpid()}
                if os.path.exists(_MY_PID_FILE):
                    try:
                        with open(_MY_PID_FILE, 'r') as f:
                            existing = json.load(f)
                        if isinstance(existing, dict):
                            pids.update(existing)
                            pids['_owner_pid'] = os.getpid()
                    except (json.JSONDecodeError, OSError):
                        pass
                pids[str(worker_id)] = pid
                self._atomic_write_json(_MY_PID_FILE, pids)
            except Exception as e:
                logger.debug(f"Failed to save worker PID {worker_id}: {e}")

    def clear_worker_pids(self):
        """Clear tracked worker PIDs after scraping completes."""
        with self._lock:
            try:
                if os.path.exists(_MY_PID_FILE):
                    os.remove(_MY_PID_FILE)
            except OSError as e:
                logger.debug(f"Could not remove worker PIDs file: {e}")

    def _authenticate_driver(self, driver, label=None, max_attempts=3):
        """Authenticate a worker driver via cookies or full login."""
        label = label or 'driver'
        for attempt in range(max_attempts):
            retry_delay = self.get_timing_float('worker_auth_retry_delay', 1.0)
            try:
                if self._apply_cookies_to_driver(driver) and self.is_logged_in(driver):
                    logger.debug(f"{label}: authenticated via cookies")
                    return True
                else:
                    self.login(driver)
                    if self.is_logged_in(driver):
                        logger.debug(f"{label}: authenticated via full login")
                        return True
                    else:
                        logger.warning(f"{label}: login verification failed (attempt {attempt + 1}/{max_attempts})")
                        print(f"  ⚠ {label}: Login verification failed (try {attempt + 1}/{max_attempts})")
                        time.sleep(retry_delay)
            except Exception as e:
                logger.warning(f"{label}: auth exception (attempt {attempt + 1}/{max_attempts}): {e}")
                print(f"  ⚠ {label}: Auth failed - {str(e)[:80]}")
                time.sleep(retry_delay)

        logger.error(f"{label}: failed to authenticate after {max_attempts} attempts")
        return False

    def _apply_cookies_to_driver(self, driver):
        """Apply auth cookies from main driver to a worker driver (thread-safe snapshot)."""
        with self._lock:
            cookies_snapshot = list(self.auth_cookies)
        if not cookies_snapshot:
            return False
        try:
            driver.get(self.get_site_url())
            self._wait_for_page_ready(driver, timeout=self.get_timing_float('cookie_apply_page_ready_timeout', 5.0))
            for cookie in cookies_snapshot:
                try:
                    driver.add_cookie({
                        'name': cookie.get('name'),
                        'value': cookie.get('value'),
                        'domain': cookie.get('domain'),
                        'path': cookie.get('path', '/'),
                        'secure': cookie.get('secure', False),
                        'httpOnly': cookie.get('httpOnly', False)
                    })
                except Exception:
                    continue
            driver.refresh()
            self._wait_for_page_ready(driver)
            return True
        except Exception:
            return False

    def _finish_scrape(self, start_time, failed_count, series_count=0):
        """Save failed series, save timing data, and print timing summary."""
        if self.failed_links:
            self.save_failed_series()
            print(f"\n⚠ {len(self.failed_links)} series failed. Saved for retry.")
        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        if series_count > 0:
            self._save_scrape_timing(total_time, series_count)
        if failed_count:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s ({failed_count} failed)")
        else:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s")

    def _scrape_all_seasons_first_pass(self, drv, series_slug, seasons, max_retries):
        """First pass: scrape every season in order, tracking subscription state."""
        season_results = []
        title = None
        consecutive_failures = 0
        _sub_readings = []
        _sub_confirmed = False

        for season in seasons:
            if season == 'Filme':
                season_url = f"https://aniworld.to/anime/stream/{series_slug}/filme"
            else:
                season_url = f"https://aniworld.to/anime/stream/{series_slug}/staffel-{season}"
            try:
                if not self._is_driver_alive(drv):
                    logger.error(f"{series_slug}: Driver died before season {season}")
                    break

                if not self._has_auth_cookies(drv) or not self.is_logged_in(drv):
                    logger.warning(f"{series_slug}: Session expired before season {season} — re-authenticating")
                    self._authenticate_driver(drv, label=f"season-{season}", max_attempts=2)

                data = self.scrape_series_detail(season_url, drv, max_retries=max_retries,
                                                 skip_subscription=_sub_confirmed)
                if data is not None:
                    season_results.append(data)
                    if data.get('title'):
                        title = data['title']
                    if not _sub_confirmed:
                        sub_val, wl_val = data.get('subscribed'), data.get('watchlist')
                        if sub_val is None and wl_val is None:
                            if not self.is_logged_in(drv):
                                logger.warning(f"{series_slug}: Session expired during season {season} — re-authenticating")
                                self._authenticate_driver(drv, label=f"sub-recovery-s{season}", max_attempts=2)
                                retry_data = self.scrape_series_detail(season_url, drv, max_retries=2)
                                if retry_data is not None:
                                    season_results[-1] = retry_data
                                    sub_val = retry_data.get('subscribed')
                                    wl_val = retry_data.get('watchlist')
                        _sub_readings.append((sub_val, wl_val))
                        if len(_sub_readings) >= 2 and _sub_readings[-1] == _sub_readings[-2]:
                            _sub_confirmed = True
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    logger.warning(f"{series_slug}: Season {season} returned no data (fail #{consecutive_failures})")
                    if not self.is_logged_in(drv):
                        self._authenticate_driver(drv, label=f"season-{season}-recovery", max_attempts=2)
                    if consecutive_failures >= 2:
                        backoff_step = self.get_timing_float('season_failure_backoff_step', 0.5)
                        backoff_max = self.get_timing_float('season_failure_backoff_max', 2.0)
                        backoff_jitter = self.get_timing_float('season_failure_backoff_jitter', 0.1)
                        backoff = min(backoff_max, backoff_step * (consecutive_failures - 1))
                        time.sleep(backoff + random.uniform(0, backoff_jitter))
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"Error scraping {series_slug}/season-{season}: {e}")
                if consecutive_failures >= 2:
                    backoff_step = self.get_timing_float('season_failure_backoff_step', 0.5)
                    backoff_max = self.get_timing_float('season_failure_backoff_max', 2.0)
                    backoff_jitter = self.get_timing_float('season_failure_backoff_jitter', 0.1)
                    backoff = min(backoff_max, backoff_step * (consecutive_failures - 1))
                    time.sleep(backoff + random.uniform(0, backoff_jitter))

        return season_results, title, _sub_readings, _sub_confirmed

    def _retry_missing_seasons(self, drv, series_slug, missing, season_results, title):
        """Retry pass for seasons that failed during the first pass."""
        logger.warning(f"{series_slug}: Missing {len(missing)} season(s) after first pass: {sorted(missing)}")
        print(f"  ⚠ {series_slug}: Retrying {len(missing)} missing season(s): {','.join(sorted(missing))}")

        if not self._is_driver_alive(drv):
            return title

        for season in sorted(missing):
            if not self._is_driver_alive(drv):
                logger.error(f"{series_slug}: Driver died during retry pass, aborting remaining seasons")
                break

            if season == 'Filme':
                season_url = f"https://aniworld.to/anime/stream/{series_slug}/filme"
            else:
                season_url = f"https://aniworld.to/anime/stream/{series_slug}/staffel-{season}"
            val = self.get_timing('max_retries_retry', default=None)
            retry_max = int(val) if val is not None else 5
            for attempt in range(retry_max):
                if not self._is_driver_alive(drv):
                    logger.error(f"{series_slug}: Driver died during retry for season {season}")
                    break
                try:
                    if not self.is_logged_in(drv):
                        self._authenticate_driver(drv, label=f"retry-s{season}", max_attempts=2)
                    data = self.scrape_series_detail(season_url, drv, max_retries=2)
                    if data is not None:
                        season_results.append(data)
                        if data.get('title'):
                            title = data['title']
                        logger.info(f"{series_slug}: Recovered season {season} on retry attempt {attempt + 1}")
                        print(f"    ✓ {series_slug}: Recovered season {season} (retry {attempt + 1}/{retry_max})")
                        break
                except Exception as e:
                    logger.warning(f"{series_slug}: Retry {attempt + 1}/{retry_max} for season {season}: {e}")

                if attempt < retry_max - 1:
                    retry_delay = self.get_timing_float('missing_season_retry_delay', 0.2, max_val=10.0)
                    retry_jitter = self.get_timing_float('missing_season_retry_jitter', 0.1, max_val=2.0)
                    time.sleep(retry_delay + random.uniform(0, retry_jitter))

        return title

    def _scrape_all_seasons_verified(self, series_slug, seasons, driver=None, max_retries=None):
        """Scrape all seasons with verification and retry for missing ones."""
        drv = driver or self.driver
        if max_retries is None:
            val = self.get_timing('max_retries_season', default=None)
            max_retries = int(val) if val is not None else 3

        expected = set(seasons)

        season_results, title, _sub_readings, _sub_confirmed = \
            self._scrape_all_seasons_first_pass(drv, series_slug, seasons, max_retries)

        scraped = {r['season'] for r in season_results}
        missing = expected - scraped
        if missing:
            title = self._retry_missing_seasons(drv, series_slug, missing, season_results, title)

        scraped_final = {r['season'] for r in season_results}
        still_missing = expected - scraped_final

        if still_missing:
            logger.error(f"{series_slug}: Still missing {len(still_missing)} season(s) after retries: {sorted(still_missing)}")

        # Sort: numeric seasons first, then 'Filme'
        season_results.sort(key=lambda r: int(r['season']) if r['season'].isdigit() else 9999)

        if _sub_confirmed and _sub_readings:
            confirmed_sub, confirmed_wl = _sub_readings[-1]
        else:
            confirmed_sub = next(
                (r['subscribed'] for r in season_results if r.get('subscribed') is not None),
                None
            )
            confirmed_wl = next(
                (r['watchlist'] for r in season_results if r.get('watchlist') is not None),
                None
            )

        for r in season_results:
            r.pop('subscribed', None)
            r.pop('watchlist', None)

        return {
            'season_results': season_results,
            'missing_seasons': sorted(still_missing) if still_missing else [],
            'title': title,
            'subscribed': confirmed_sub,
            'watchlist': confirmed_wl,
        }

    def _scrape_series_parallel(self, series_urls, max_workers):
        """True parallel scraping with shared work queue."""
        self.clear_pause_request()

        series_data = {}
        total_series = len(series_urls)
        self._historical_avg = self._load_scrape_timing()
        start_time = time.time()
        completed = 0
        failed = 0
        stop_event = threading.Event()

        # Scale workers: ~1 per 15 series, minimum 1, capped at max_workers
        worker_count = min(max_workers, max(1, total_series // 15))
        work_queue = queue.Queue()
        for item in series_urls:
            work_queue.put(item)

        print(f"→ {total_series} series queued for {worker_count} workers (shared work queue)")

        def worker_loop(worker_id):
            nonlocal completed, failed

            success_delay = self.get_timing_float('success_delay', 0.3)
            backoff_base = self.get_timing_float('error_backoff_base', 1.0)
            backoff_max = self.get_timing_float('error_backoff_max', 8.0)
            health_every = self.get_timing_int('health_check_every', 15)
            restart_threshold = self.get_timing_int('error_restart_threshold', 8)

            driver = None
            try:
                driver = self._create_worker_driver(worker_id)
            except Exception as e:
                logger.error(f"Worker #{worker_id}: failed to create driver: {e}", exc_info=True)
                print(f"  ✗ Worker #{worker_id}: Failed to create browser: {str(e)[:80]}")
                return
            self.inject_aggressive_adblock(driver)

            authenticated = self._authenticate_driver(driver, label=f"Worker #{worker_id}")
            if not authenticated:
                logger.error(f"Worker #{worker_id}: failed to authenticate — items stay in queue")
                print(f"  ✗ Worker #{worker_id}: Failed to authenticate. Items remain in queue.")
                try:
                    driver.quit()
                except Exception:
                    pass
                return

            error_streak = 0
            tasks_since_check = 0

            while not stop_event.is_set():
                try:
                    item = work_queue.get_nowait()
                except queue.Empty:
                    break

                if stop_event.is_set():
                    break

                if self.is_pause_requested():
                    print(f"\n⏸ Worker #{worker_id} pausing (pause file detected)")
                    break

                series_url, series_slug, display_title = self._extract_item_info(item)
                if series_slug == 'unknown':
                    continue

                try:
                    if not self._is_driver_alive(driver):
                        logger.warning(f"W{worker_id}: Driver dead — restarting")
                        print(f"  ⚠ W{worker_id}: Browser crashed — restarting...")
                        driver, ok = self._restart_worker_driver(worker_id, driver)
                        if not ok:
                            work_queue.put(item)
                            break
                        error_streak = 0

                    try:
                        seasons = self.get_all_seasons_for_series(series_slug, driver)
                    except SeasonDetectionError as e:
                        if not self._is_driver_alive(driver):
                            work_queue.put(item)
                            driver, ok = self._restart_worker_driver(worker_id, driver)
                            if not ok:
                                break
                            error_streak = 0
                            continue
                        with self._lock:
                            completed += 1
                            failed += 1
                            self.failed_links.append(self._normalize_failed_item(item, series_url, display_title))
                            print(self._format_progress_line(completed, total_series, start_time, display_title,
                                                             error=f"season detection: {str(e)[:60]}",
                                                             worker_id=worker_id, worker_count=worker_count))
                        error_streak += 1
                        continue

                    result = self._scrape_all_seasons_verified(series_slug, seasons, driver)
                    season_results = result['season_results']
                    missing_seasons = result['missing_seasons']
                    series_title = result['title'] or display_title
                    result_sub = result.get('subscribed')
                    result_wl = result.get('watchlist')

                    if missing_seasons and not self._is_driver_alive(driver):
                        work_queue.put(item)
                        driver, ok = self._restart_worker_driver(worker_id, driver)
                        if not ok:
                            break
                        error_streak = 0
                        continue

                    with self._lock:
                        completed += 1
                        if season_results:
                            season_labels = [r['season'] for r in season_results]
                            series_watched, series_total_eps, series_had_error, _, _ = \
                                self._aggregate_season_results(
                                    series_slug, season_results, missing_seasons, series_data,
                                    is_subscribed=result_sub, is_watchlist=result_wl)

                            if series_slug in series_data:
                                series_data[series_slug]['title'] = series_title
                                # Merge alt_titles from first season result
                                for sr in season_results:
                                    if sr.get('alt_titles'):
                                        existing = series_data[series_slug].get('alt_titles', [])
                                        combined = list(dict.fromkeys(existing + sr['alt_titles']))
                                        series_data[series_slug]['alt_titles'] = combined
                                        break
                                # Store description from first result
                                for sr in season_results:
                                    if sr.get('description'):
                                        series_data[series_slug]['description'] = sr['description']
                                        break

                            if series_had_error:
                                failed += 1
                                error_parts = []
                                if missing_seasons:
                                    error_parts.append(f"Missing seasons: {missing_seasons}")
                                if series_data.get(series_slug, {}).get('_has_malformed_episodes'):
                                    error_parts.append("malformed episode rows")
                                self._add_failed_link(item, series_url, display_title,
                                                      error=' + '.join(error_parts) or 'data error')
                            elif result_sub is None or result_wl is None:
                                failed += 1
                                missing_fields = []
                                if result_sub is None:
                                    missing_fields.append('Sub')
                                if result_wl is None:
                                    missing_fields.append('WL')
                                self._add_failed_link(item, series_url, display_title,
                                                      error=f"missing {'+'.join(missing_fields)} status")
                                logger.warning(f"W{worker_id}: {series_slug} missing Sub/WL status (Sub={result_sub}, WL={result_wl}) — marking as failed for retry")
                            elif series_total_eps == 0:
                                failed += 1
                                self._add_failed_link(item, series_url, display_title,
                                                      error="0 episodes across all seasons")
                                logger.warning(f"W{worker_id}: {series_slug} returned 0 episodes — marking as failed for retry")

                            print(self._format_progress_line(
                                completed, total_series, start_time, series_title,
                                watched=series_watched, episode_total=series_total_eps,
                                empty=series_total_eps == 0,
                                worker_id=worker_id, worker_count=worker_count,
                                season_labels=season_labels,
                                subscribed=result_sub, watchlist=result_wl))
                        else:
                            failed += 1
                            self._add_failed_link(item, series_url, display_title, error="No season data")
                            print(self._format_progress_line(
                                completed, total_series, start_time, display_title,
                                error="No season data",
                                worker_id=worker_id, worker_count=worker_count))

                    if season_results:
                        error_streak = 0
                        time.sleep(success_delay)
                    else:
                        error_streak += 1

                    tasks_since_check += 1

                    # Periodic health check
                    if tasks_since_check >= health_every:
                        tasks_since_check = 0
                        error_streak, alive = self._worker_health_check(worker_id, driver, error_streak)
                        if not alive:
                            break

                    # Restart on too many consecutive errors
                    if error_streak >= restart_threshold:
                        logger.warning(f"W{worker_id}: {error_streak} consecutive errors — restarting browser")
                        print(f"  ⚠ W{worker_id}: Too many errors — restarting browser...")
                        driver, ok = self._restart_worker_driver(worker_id, driver)
                        if not ok:
                            break
                        error_streak = 0

                    # Periodic checkpoint
                    if completed % 10 == 0:
                        self.save_checkpoint()

                    # Periodic memory cleanup
                    if completed % 50 == 0:
                        self._cleanup_memory()

                except Exception as e:
                    logger.error(f"W{worker_id}: Unhandled error processing {series_slug}: {e}", exc_info=True)
                    with self._lock:
                        completed += 1
                        failed += 1
                        self._add_failed_link(item, series_url, display_title, error=e)
                        print(self._format_progress_line(
                            completed, total_series, start_time, display_title,
                            error=str(e)[:60],
                            worker_id=worker_id, worker_count=worker_count))
                    error_streak += 1
                    backoff = min(backoff_max, backoff_base * (2 ** min(error_streak, 5)))
                    time.sleep(backoff)

            # Worker cleanup
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        # Launch workers
        threads = []
        for wid in range(worker_count):
            t = threading.Thread(target=worker_loop, args=(wid,), name=f"worker-{wid}")
            threads.append(t)
            t.start()
            time.sleep(self.get_timing_float('parallel_worker_stagger_delay', 2.0, max_val=30.0))

        for t in threads:
            t.join()

        return series_data

    def _scrape_series_sequential(self, series_urls):
        """Sequential scraping using main driver."""
        series_data = {}
        total = len(series_urls)
        self._historical_avg = self._load_scrape_timing()
        start_time = time.time()
        completed = 0
        failed = 0

        for item in series_urls:
            if self.is_pause_requested():
                print("\n⏸ Scraping paused (pause file detected)")
                self.save_checkpoint(include_data=True)
                raise ScrapingPaused()

            series_url, series_slug, display_title = self._extract_item_info(item)
            if series_slug == 'unknown':
                completed += 1
                continue

            if series_slug in self.completed_links:
                completed += 1
                continue

            # Recover dead driver before attempting the next series
            if not self._is_driver_alive():
                logger.warning("Main driver died — restarting for sequential scrape")
                print("  ⚠ Browser crashed — restarting...")
                try:
                    self.close()
                except Exception:
                    pass
                try:
                    self.setup_driver()
                    self.login()
                    logger.info("Main driver restarted successfully")
                    print("  ✓ Browser restarted")
                except Exception as restart_err:
                    logger.error(f"Failed to restart main driver: {restart_err}")
                    print(f"  ✗ Failed to restart browser: {restart_err}")
                    break

            try:
                seasons = self.get_all_seasons_for_series(series_slug)
            except SeasonDetectionError as e:
                completed += 1
                failed += 1
                self._add_failed_link(item, series_url, display_title, error=e)
                print(self._format_progress_line(completed, total, start_time, display_title,
                                                 error=f"season detection: {str(e)[:60]}"))
                continue

            result = self._scrape_all_seasons_verified(series_slug, seasons)
            season_results = result['season_results']
            missing_seasons = result['missing_seasons']
            series_title = result['title'] or display_title
            result_sub = result.get('subscribed')
            result_wl = result.get('watchlist')

            completed += 1
            if season_results:
                season_labels = [r['season'] for r in season_results]
                series_watched, series_total_eps, series_had_error, _, _ = \
                    self._aggregate_season_results(
                        series_slug, season_results, missing_seasons, series_data,
                        is_subscribed=result_sub, is_watchlist=result_wl)

                if series_slug in series_data:
                    series_data[series_slug]['title'] = series_title
                    for sr in season_results:
                        if sr.get('alt_titles'):
                            existing = series_data[series_slug].get('alt_titles', [])
                            combined = list(dict.fromkeys(existing + sr['alt_titles']))
                            series_data[series_slug]['alt_titles'] = combined
                            break
                    for sr in season_results:
                        if sr.get('description'):
                            series_data[series_slug]['description'] = sr['description']
                            break

                if series_had_error:
                    failed += 1
                    error_parts = []
                    if missing_seasons:
                        error_parts.append(f"Missing seasons: {missing_seasons}")
                    if series_data.get(series_slug, {}).get('_has_malformed_episodes'):
                        error_parts.append("malformed episode rows")
                    self._add_failed_link(item, series_url, display_title,
                                          error=' + '.join(error_parts) or 'data error')
                elif result_sub is None or result_wl is None:
                    failed += 1
                    missing_fields = []
                    if result_sub is None:
                        missing_fields.append('Sub')
                    if result_wl is None:
                        missing_fields.append('WL')
                    self._add_failed_link(item, series_url, display_title,
                                          error=f"missing {'+'.join(missing_fields)} status")
                    logger.warning(f"{series_slug} missing Sub/WL status (Sub={result_sub}, WL={result_wl}) — marking as failed for retry")
                elif series_total_eps == 0:
                    failed += 1
                    self._add_failed_link(item, series_url, display_title,
                                          error="0 episodes across all seasons")
                    logger.warning(f"{series_slug} returned 0 episodes — marking as failed for retry")

                print(self._format_progress_line(
                    completed, total, start_time, series_title,
                    watched=series_watched, episode_total=series_total_eps,
                    empty=series_total_eps == 0,
                    season_labels=season_labels,
                    subscribed=result_sub, watchlist=result_wl))
            else:
                failed += 1
                self._add_failed_link(item, series_url, display_title, error="No season data")
                print(self._format_progress_line(
                    completed, total, start_time, display_title,
                    error="No season data"))

            if completed % 10 == 0:
                self.save_checkpoint()

            time.sleep(self.get_timing_float('success_delay', 0.8))

        return series_data

    def _finalize_series_data(self, raw_series_data):
        """Convert raw series_data dict to list format for saving."""
        if isinstance(raw_series_data, list):
            return raw_series_data

        result = []
        for slug, data in raw_series_data.items():
            entry = dict(data)
            entry.setdefault('title', slug)
            entry.setdefault('url', f"https://aniworld.to/anime/stream/{slug}")
            entry.setdefault('link', f"/anime/stream/{slug}")
            result.append(entry)

        # Merge with any flushed data
        if self._flushed_count > 0:
            result = self._load_flushed_series_data() + result

        return result

    def run(self, output_file=None, single_url=None, url_list=None,
            new_only=False, parallel=True, resume_only=False,
            retry_failed=False):
        """
        Main entry point for scraping.

        Args:
            output_file: Path to output JSON file
            single_url: Single series URL to scrape
            url_list: List of URLs to scrape
            new_only: Only scrape series not in existing index
            parallel: Use parallel mode (default True)
            resume_only: Resume from checkpoint only
            retry_failed: Retry previously failed series
        """
        start_time = time.time()

        try:
            self.setup_driver()
            self.inject_aggressive_adblock()
            self.login()

            if output_file:
                data_dir = os.path.dirname(output_file)
                self.set_checkpoint_paths(data_dir)

            # Determine work list
            if retry_failed:
                self._checkpoint_mode = 'retry'
                work_list = self.load_failed_series()
                if not work_list:
                    print("✓ No failed series to retry")
                    return
                print(f"→ Retrying {len(work_list)} failed series...")
                self.clear_failed_series()

            elif single_url:
                work_list = [{'url': self.normalize_to_series_url(single_url), 'title': ''}]
                parallel = False

            elif url_list:
                self._checkpoint_mode = 'batch'
                work_list = [{'url': self.normalize_to_series_url(u), 'title': ''} for u in url_list]

            else:
                # Full catalogue scrape
                self._checkpoint_mode = 'new_only' if new_only else 'all_series'
                all_series = self.get_all_series()
                self.all_discovered_series = all_series

                if new_only and output_file and os.path.exists(output_file):
                    try:
                        with open(output_file, 'r', encoding='utf-8') as f:
                            existing = json.load(f)
                        if isinstance(existing, list):
                            existing_slugs = set()
                            for s in existing:
                                url = s.get('url', s.get('link', ''))
                                slug = url.rstrip('/').split('/')[-1]
                                if slug:
                                    existing_slugs.add(slug)
                        elif isinstance(existing, dict):
                            existing_slugs = set()
                            for s in existing.values():
                                url = s.get('url', s.get('link', ''))
                                slug = url.rstrip('/').split('/')[-1]
                                if slug:
                                    existing_slugs.add(slug)
                        else:
                            existing_slugs = set()

                        work_list = [s for s in all_series if s.get('slug', '') not in existing_slugs]
                        print(f"→ {len(work_list)} new series to scrape (skipping {len(existing_slugs)} existing)")
                    except Exception:
                        work_list = all_series
                else:
                    work_list = all_series

            if not work_list:
                print("✓ Nothing to scrape")
                return

            # Resume from checkpoint
            if resume_only and self.load_checkpoint():
                remaining = [item for item in work_list
                             if self._get_item_slug(item) not in self.completed_links]
                done = len(work_list) - len(remaining)
                print(f"✓ Resuming from checkpoint: {done}/{len(work_list)} already done")
                if not remaining:
                    print("✓ All series already scraped")
                    self.clear_checkpoint()
                    return
                work_list = remaining

            # Execute scraping
            if parallel and len(work_list) > 1:
                raw_data = self._scrape_series_parallel(work_list, MAX_WORKERS)
            else:
                raw_data = self._scrape_series_sequential(work_list)

            self.series_data = self._finalize_series_data(raw_data)

            self._finish_scrape(start_time, len(self.failed_links), len(work_list))

        except ScrapingPaused:
            print("\n⏸ Scraping paused. Use resume to continue.")
        except (KeyboardInterrupt, SystemExit):
            print("\n⚠ Scraping interrupted")
            self.save_checkpoint(include_data=True)
        except Exception as e:
            logger.error(f"Scraping error: {e}", exc_info=True)
            print(f"\n✗ Scraping error: {str(e)[:80]}")
            self.save_checkpoint(include_data=True)
        finally:
            self.clear_worker_pids()
            self.close()

    def _get_item_slug(self, item):
        """Extract slug from a work item for checkpoint comparison."""
        if isinstance(item, dict):
            url = item.get('url', item.get('link', ''))
        else:
            url = str(item)
        return url.rstrip('/').split('/')[-1]

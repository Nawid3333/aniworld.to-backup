"""
AniWorld.to Backup Tool Configuration
Load credentials from .env file and site-specific selectors
"""

import os
import json
import warnings
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ==================== CREDENTIALS ====================
EMAIL = os.getenv("ANIWORLD_EMAIL", "")
PASSWORD = os.getenv("ANIWORLD_PASSWORD", "")

# ==================== DIRECTORIES ====================
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# Series index file
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# ==================== SELECTORS CONFIGURATION ====================
CONFIG_DIR = os.path.dirname(__file__)
SELECTORS_CONFIG_FILE = os.path.join(CONFIG_DIR, "selectors_config3.json")

def load_selectors_config():
    """Load site-specific selectors configuration from JSON file"""
    try:
        with open(SELECTORS_CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        warnings.warn(f"Selectors config not found: {SELECTORS_CONFIG_FILE}")
        return {}
    except json.JSONDecodeError as e:
        warnings.warn(f"Error parsing selectors config: {e}")
        return {}
    except Exception as e:
        warnings.warn(f"Could not load selectors config: {str(e)}")
        return {}

SELECTORS_CONFIG = load_selectors_config()

# ==================== DISPLAY SETTINGS ====================
VERBOSE_CHANGES = False

# ==================== SCRAPING SETTINGS ====================
HEADLESS = True

# ==================== LOGGING ====================
LOG_FILE = os.path.join(LOGS_DIR, "aniworld_backup.log")

# ==================== TIMEOUTS & RETRIES ====================
HTTP_REQUEST_TIMEOUT = 15.0
PAGE_LOAD_TIMEOUT = 20.0
DRIVER_QUIT_TIMEOUT = 5.0

MAX_TOTAL_RETRIES = 5

print(f"✓ Config loaded (DATA_DIR: {DATA_DIR})")

import json
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# All tunable logic lives here. The /logic page in the dashboard renders and
# edits these values. config_overrides.json (if present) overrides any key.
# ---------------------------------------------------------------------------

ALERT_RULES = {
    "big_buy_threshold": 1_000_000,
    "insider_buy_threshold": 250_000,
    "insider_title_keywords": ["CEO", "Chief Executive", "CFO", "Chief Financial"],
    "cluster_window_days": 10,
    "cluster_min_insiders": 3,
}

# Tiered bonuses: within each tier only the highest matching threshold applies.
# Each entry is (threshold, points, label). Must stay in descending order.
CONVICTION_TIERS = {
    "value": [
        (5_000_000, 3, "value_over_5m"),
        (1_000_000, 2, "value_over_1m"),
        (250_000,   1, "value_over_250k"),
    ],
    "pct_holdings": [
        (50, 3, "pct_over_50"),
        (20, 2, "pct_over_20"),
    ],
}

# Non-tiered flags — each fires independently and is additive.
CONVICTION_FLAGS = {
    "base_open_market_buy":    3,   # P code floor — non-P transactions always score 0
    "ceo_cfo_bonus":           2,
    "director_bonus":          1,
    "ten_percent_owner_bonus": 1,
    "cluster_bonus":           2,   # 3+ distinct insiders at same issuer within window
    "non_10b5_1_buy":          1,
}

CONVICTION_MAX = 10
CONVICTION_THRESHOLDS = {"high": 8, "medium": 5}  # tier labels for color coding

FILTER_DEFAULTS = {
    "min_value": 100_000,
    "transaction_codes": ["P", "S"],
    "hide_10b5_1": True,
    "roles": [],
}

TRANSACTION_CODES = {
    "P": {"label": "Open market purchase",   "show_by_default": True,  "buy": True},
    "S": {"label": "Open market sale",        "show_by_default": True,  "buy": False},
    "A": {"label": "Award / grant",           "show_by_default": False, "buy": None},
    "M": {"label": "Option exercise",         "show_by_default": False, "buy": None},
    "F": {"label": "Tax withholding sale",    "show_by_default": False, "buy": False},
    "G": {"label": "Gift",                    "show_by_default": False, "buy": None},
    "D": {"label": "Sale back to issuer",     "show_by_default": False, "buy": False},
    "J": {"label": "Other acquisition/disp.", "show_by_default": False, "buy": None},
    "X": {"label": "Exercise of derivative", "show_by_default": False, "buy": None},
    "C": {"label": "Conversion of derivative","show_by_default": False, "buy": None},
    "E": {"label": "Expiration of short deriv","show_by_default": False,"buy": None},
    "H": {"label": "Expiration of long deriv", "show_by_default": False, "buy": None},
    "I": {"label": "Discretionary transaction","show_by_default": False,"buy": None},
    "K": {"label": "Equity swap settled",     "show_by_default": False, "buy": None},
    "L": {"label": "Small acquisition (<$10k)","show_by_default": False,"buy": None},
    "O": {"label": "Exercise out-of-money",   "show_by_default": False, "buy": None},
    "U": {"label": "Tender of shares",        "show_by_default": False, "buy": False},
    "W": {"label": "Will / inheritance",      "show_by_default": False, "buy": None},
    "Z": {"label": "Deposit into voting trust","show_by_default": False,"buy": None},
}

ALERT_BASE_URL = "https://opi-insider.duckdns.org"

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

SEC_USER_AGENT = "Option Pit Research charlie@optionpit.com"
SEC_RATE_LIMIT = 8  # requests per second — SEC cap is 10; stay safely under

TICKER_CACHE_DAYS = 7

BASE_DIR = Path(__file__).parent
DB_PATH = str(BASE_DIR / "data" / "insider_tracker.db")
TICKER_CACHE_PATH = str(BASE_DIR / "data" / "company_tickers.json")
OVERRIDES_PATH = str(BASE_DIR / "config_overrides.json")


def load_config() -> dict:
    """Return merged config: defaults overridden by config_overrides.json values."""
    cfg = {
        "alert_rules": dict(ALERT_RULES),
        "alert_base_url": ALERT_BASE_URL,
        "polygon_api_key": POLYGON_API_KEY,
        "filter_defaults": dict(FILTER_DEFAULTS),
        "transaction_codes": dict(TRANSACTION_CODES),
        "conviction_tiers": {k: list(v) for k, v in CONVICTION_TIERS.items()},
        "conviction_flags": dict(CONVICTION_FLAGS),
        "conviction_max": CONVICTION_MAX,
        "conviction_thresholds": dict(CONVICTION_THRESHOLDS),
        "sec_user_agent": SEC_USER_AGENT,
        "sec_rate_limit": SEC_RATE_LIMIT,
        "ticker_cache_days": TICKER_CACHE_DAYS,
        "db_path": DB_PATH,
    }
    if os.path.exists(OVERRIDES_PATH):
        with open(OVERRIDES_PATH) as f:
            overrides = json.load(f)
        for section in ("alert_rules", "filter_defaults", "conviction_flags"):
            if section in overrides:
                cfg[section].update(overrides[section])
    return cfg


def save_overrides(
    alert_rules: dict,
    filter_defaults: dict,
    conviction_flags: dict | None = None,
) -> None:
    """Persist edits made via the /logic page."""
    # Merge with existing overrides so we don't clobber sections not being saved
    existing: dict = {}
    if os.path.exists(OVERRIDES_PATH):
        with open(OVERRIDES_PATH) as f:
            existing = json.load(f)

    existing["alert_rules"] = alert_rules
    existing["filter_defaults"] = filter_defaults
    if conviction_flags is not None:
        existing["conviction_flags"] = conviction_flags

    with open(OVERRIDES_PATH, "w") as f:
        json.dump(existing, f, indent=2)

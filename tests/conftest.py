"""Shared pytest setup.

This runs before any test module imports app.py, which matters: app.py builds the
slowapi Limiter at import time from RATE_LIMIT_STORAGE_URI, and that default
points at Redis. On a developer machine with no Redis running, every
rate-limited request then pays a connection timeout, which took the suite from
about 2 seconds to about 14. Pinning the limiter to in-memory storage for tests
keeps it fast and keeps the counters isolated per run.

Production behaviour is unaffected and still covered: tests/test_deploy_manifest.py
asserts the variable is documented in .env.example, and the real Redis backing is
what the deployed service uses.
"""
from __future__ import annotations

import os

os.environ.setdefault("RATE_LIMIT_STORAGE_URI", "memory://")

"""Paths and shared constants. Single source of truth for all I/O locations."""
import os
from datetime import timezone

# Repo layout: scripts/core/paths.py -> repo root is two levels up
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(REPO_ROOT, 'data')
TMP_DIR = os.path.join(DATA_DIR, '.tmp')
STATE_FILE = os.path.join(DATA_DIR, '_state.json')
VALIDATION_LOG = os.path.join(DATA_DIR, '_validation.json')

UTC = timezone.utc

USER_AGENT = 'EnergyDashboard/5.0 (+https://github.com/owner/energy-dashboard)'

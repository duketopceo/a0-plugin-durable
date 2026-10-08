"""Plugin config: framework get_plugin_config merged over shipped defaults.

a0's `helpers.plugins.get_plugin_config` already falls back to the plugin's
`default_config.yaml` then merges user settings — `DEFAULTS` here is the
code-level fallback for non-a0 runtimes (tests, a future Hermes port).
Keep DEFAULTS, `default_config.yaml`, and the README settings table in
sync — adding a key means touching all three.
"""

from __future__ import annotations

import math
import os
from typing import Any

DEFAULTS: dict[str, Any] = {
    "enabled": False,
    "engine": "local",
    "journal_path": "",
    "step_timeout_s": 300,
    "max_iterations": 100,
    "tasks": [],
    "restate_ingress": "http://127.0.0.1:8080",
    "restate_admin": "http://127.0.0.1:9070",
    "listen_port": 9080,
}


# env var -> config key (documented in default_config.yaml)
_ENV_MAP = {
    "DURABLE_ENABLED": "enabled",
    "DURABLE_ENGINE": "engine",
    "DURABLE_JOURNAL_PATH": "journal_path",
    "DURABLE_RESTATE_INGRESS": "restate_ingress",
    "DURABLE_RESTATE_ADMIN": "restate_admin",
    "DURABLE_LISTEN_PORT": "listen_port",
}


def get_config() -> dict[str, Any]:
    """DEFAULTS < user plugin config (which itself sits over
    default_config.yaml via a0's own fallback) < env vars. Never raises."""
    cfg = dict(DEFAULTS)
    try:
        from helpers.plugins import get_plugin_config  # type: ignore

        user = get_plugin_config("durable")
        if isinstance(user, dict):
            cfg.update(user)
    except Exception:
        pass
    for env, key in _ENV_MAP.items():
        val = os.environ.get(env)
        if val not in (None, ""):
            cfg[key] = val
    return cfg


def truthy(v: Any) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "on"}
    return bool(v)


def num(v: Any, default: float) -> float:
    """Safe numeric coercion — a bad setting must not break plugin init.
    Non-finite results (nan/inf would raise again on int()) fall back."""
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError, OverflowError):
        return default

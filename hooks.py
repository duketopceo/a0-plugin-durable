"""Plugin lifecycle hooks — a0 calls install()/uninstall() on load/unload.

install() deliberately does NOT configure the engine: a0's plugin loader
calls hooks before the runtime config layer is guaranteed ready, so the
sync `startup_migration` extension owns configure(). uninstall() stops all
plugin-owned services (restate ASGI endpoint thread, live local runners);
journaled state survives on disk — tasks resume on next install+tick.
"""

from __future__ import annotations

import logging


def install() -> None:
    logging.getLogger("a0.durable").info(
        "durable plugin installed — engine configures at startup_migration"
    )


def uninstall() -> None:
    """Stop engine-owned services; leave the journal on disk."""
    try:
        from usr.plugins.durable.helpers import runtime

        runtime._reset()
    except Exception:
        pass

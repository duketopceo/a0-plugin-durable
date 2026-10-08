"""Plugin lifecycle hooks — a0 calls install()/uninstall() on load/unload.

uninstall() must stop plugin-owned services cleanly (issue acceptance):
the restate ASGI endpoint thread and any live local runners. Journaled
state persists — tasks resume on the next install+tick.
"""

from __future__ import annotations


def install() -> None:
    """No-op — the sync startup_migration extension performs configure()
    once a0's runtime (and its config layer) is up."""
    try:
        from usr.plugins.durable.helpers import runtime

        runtime.configure()
    except Exception:
        pass


def uninstall() -> None:
    """Stop engine-owned services; leave the journal on disk."""
    try:
        from usr.plugins.durable.helpers import runtime

        runtime._reset()
    except Exception:
        pass

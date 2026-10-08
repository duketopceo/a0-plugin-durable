"""startup_migration extension — configure the durable engine.

Sync `execute` (startup_migration runs under call_extensions_sync — an
awaitable return would raise). Engine construction is synchronous; the
Restate endpoint starts lazily on the first async touch because there is
no running loop here.
"""

from typing import Any

from helpers.extension import Extension

from usr.plugins.durable.helpers import runtime


class DurableInit(Extension):
    def execute(self, data: dict[str, Any] | None = None, **kwargs) -> None:
        try:
            runtime.configure()
        except Exception:
            pass

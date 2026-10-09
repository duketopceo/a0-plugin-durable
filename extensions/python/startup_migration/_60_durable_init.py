"""startup_migration extension — configure the durable engine.

Sync `execute` (startup_migration runs under call_extensions_sync — an
awaitable return would raise). Engine construction is synchronous; the
engine's serving side starts lazily on the first async touch because there
is no running loop here.

The `usr.plugins.durable` import stays inside execute() — upstream's
`import_module` sweep is unguarded, so a module-level plugin import that
ever fails would abort the whole extension sweep (and a0 boot for
startup_migration). `_60_` intentionally lands after other plugins' startup
migrations so host code can register steps (llm_call/tool_call) first.
"""

from helpers.extension import Extension


class DurableInit(Extension):
    def execute(self, **kwargs) -> None:
        del kwargs  # a0 passes no payload at this extension point
        try:
            from usr.plugins.durable.helpers import runtime

            runtime.configure()
        except Exception:
            pass

"""Make `usr.plugins.durable.*` imports resolvable under pytest — the same
qualified path the a0 runtime uses inside usr/plugins/durable/ — and stub
the framework modules the plugin touches (helpers.extension,
helpers.plugins, helpers.api) so everything runs standalone, offline.
"""

import asyncio
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _pkg(name, path=None):
    mod = types.ModuleType(name)
    mod.__path__ = [str(path)] if path else []
    return mod


_usr = _pkg("usr")
_plugins = _pkg("usr.plugins")
_durable = _pkg("usr.plugins.durable", ROOT)
_usr.plugins = _plugins
_plugins.durable = _durable
sys.modules.setdefault("usr", _usr)
sys.modules.setdefault("usr.plugins", _plugins)
sys.modules["usr.plugins.durable"] = _durable


# --- minimal a0 framework stubs ---------------------------------------------


class Extension:
    def __init__(self, agent=None, **kw):
        self.agent = agent


class ApiHandler:
    def __init__(self, app=None, thread_lock=None):
        self.app = app
        self.thread_lock = thread_lock

    @classmethod
    def requires_auth(cls) -> bool:
        return True

    @classmethod
    def requires_csrf(cls) -> bool:
        return cls.requires_auth()

    @classmethod
    def get_methods(cls):
        return ["POST"]

    async def process(self, input, request):
        raise NotImplementedError


class FakeRequest:
    def __init__(self, path="/api/x", method="POST", headers=None):
        self.path = path
        self.method = method
        self.headers = dict(headers or {})


_helpers = _pkg("helpers")

_ext = types.ModuleType("helpers.extension")
_ext.Extension = Extension
_helpers.extension = _ext

_plugins_mod = types.ModuleType("helpers.plugins")
_plugins_mod.get_plugin_config = lambda name: {}
_helpers.plugins = _plugins_mod

_api_mod = types.ModuleType("helpers.api")
_api_mod.ApiHandler = ApiHandler
_api_mod.Request = FakeRequest
_helpers.api = _api_mod

sys.modules.setdefault("helpers", _helpers)
sys.modules["helpers.extension"] = _ext
sys.modules["helpers.plugins"] = _plugins_mod
sys.modules["helpers.api"] = _api_mod


def run(coro):
    return asyncio.run(coro)


import pytest  # noqa: E402


@pytest.fixture()
def journal_path(tmp_path):
    return str(tmp_path / "journal.db")


@pytest.fixture()
def local_cfg(journal_path):
    return {
        "enabled": True,
        "engine": "local",
        "journal_path": journal_path,
        "step_timeout_s": 5,
        "max_iterations": 10,
        "tasks": [],
    }


@pytest.fixture(autouse=True)
def _clean_runtime(journal_path):
    """Every test starts with the plugin unconfigured, no engine, and an
    empty step registry (built-in api_call re-registers per configure)."""
    from usr.plugins.durable.helpers import runtime

    runtime._reset()
    yield
    runtime._reset()

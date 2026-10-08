"""Restate engine: clean degradation without the optional SDK, ingress
client failure containment. No restate server exists in tests — every path
must fail safe."""

import socket

from usr.plugins.durable.helpers.engines import restate as re_mod
from usr.plugins.durable.helpers.engines.restate import RestateEngine

from conftest import run


def _dead_port() -> int:
    """A loopback port that is guaranteed closed."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_module_imports_without_sdk():
    # restate_sdk is not a test dep — the module must still import and the
    # engine must report itself unavailable
    assert re_mod.restate is None
    eng = RestateEngine(ingress="http://127.0.0.1:1", admin="http://127.0.0.1:1",
                        listen_port=19999)
    assert eng.available is False


def test_start_without_sdk_returns_false():
    eng = RestateEngine(ingress="http://127.0.0.1:1", admin="http://127.0.0.1:1",
                        listen_port=19999)
    assert run(eng.start()) is False


def test_ingress_client_failures_are_none():
    port = _dead_port()
    eng = RestateEngine(ingress=f"http://127.0.0.1:{port}",
                        admin=f"http://127.0.0.1:{port}", listen_port=19999)

    async def _main():
        assert await eng.submit({"id": "t"}) is None
        assert await eng.signal("t", "pause") is False
        assert await eng.status("t") is None
        assert await eng.signal("t", "bogus") is False

    run(_main())


def test_stop_without_start_is_clean():
    eng = RestateEngine(ingress="http://127.0.0.1:1", admin="http://127.0.0.1:1",
                        listen_port=19999)
    run(eng.stop())  # must not raise


def test_workflow_definition_only_with_sdk():
    """Without the SDK there is no workflow object — documented contract."""
    assert re_mod.agent_task is None

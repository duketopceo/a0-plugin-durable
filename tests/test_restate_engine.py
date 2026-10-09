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


def test_submit_url_shape_and_process_dedupe(monkeypatch):
    """Submit POSTs to the keyed run/send endpoint; the process-local set
    dedupes re-submits (config tasks re-submit every tick)."""
    eng = RestateEngine(ingress="http://ing", admin="http://adm",
                        listen_port=19999)
    posts = []

    def fake_post(url, payload, timeout=10.0):
        posts.append((url, payload))
        return {"invocationId": "x"}

    monkeypatch.setattr(re_mod, "_post", fake_post)

    async def _main():
        tid = await eng.submit({"id": "task-1", "prompt_messages": []})
        assert tid == "task-1"
        url, payload = posts[0]
        assert url == "http://ing/AgentTask/task-1/run/send"
        assert payload["id"] == "task-1"
        # second submit of the same id — no second POST
        assert await eng.submit({"id": "task-1"}) == "task-1"
        assert len(posts) == 1
        # a failed POST does not poison the dedupe set (retry works later)
        monkeypatch.setattr(re_mod, "_post", lambda *a, **k: None)
        assert await eng.submit({"id": "task-2"}) is None
        monkeypatch.setattr(re_mod, "_post", fake_post)
        assert await eng.submit({"id": "task-2"}) == "task-2"

    run(_main())


def test_submit_rejects_invalid_id_without_posting(monkeypatch):
    eng = RestateEngine(ingress="http://ing", admin="http://adm",
                        listen_port=19999)
    posts = []
    monkeypatch.setattr(
        re_mod, "_post", lambda *a, **k: posts.append(a) or {})

    async def _main():
        assert await eng.submit({"id": "has space"}) is None
        assert not posts  # invalid id never reached the wire

    run(_main())


def test_signal_gates_on_state_and_urls(monkeypatch):
    """Signals gate on a live known state — unknown/terminal tasks return
    False without POSTing a signal (local-engine parity)."""
    eng = RestateEngine(ingress="http://ing", admin="http://adm",
                        listen_port=19999)
    posts = []

    def fake_post(url, payload, timeout=10.0):
        posts.append(url)
        if url.endswith("/get_state"):
            if "/gone/" in url:
                return None                     # unknown task
            if "/done/" in url:
                return {"status": "completed"}  # terminal
            return {"status": "executing"}
        return {"ok": True}

    monkeypatch.setattr(re_mod, "_post", fake_post)

    async def _main():
        assert await eng.signal("gone", "pause") is False
        assert await eng.signal("done", "pause") is False
        assert await eng.signal("t", "bogus") is False
        assert all(u.endswith("/get_state") for u in posts)  # gated — no signal POSTs
        assert await eng.signal("t", "pause") is True
        assert posts[-1] == "http://ing/AgentTask/t/pause"
        assert await eng.signal("t", "cancel") is True
        assert posts[-1] == "http://ing/AgentTask/t/cancel"

    run(_main())


def test_status_empty_body_maps_to_unknown(monkeypatch):
    eng = RestateEngine(ingress="http://ing", admin="http://adm",
                        listen_port=19999)
    monkeypatch.setattr(re_mod, "_post", lambda *a, **k: {})

    async def _main():
        assert await eng.status("t") is None
        assert await eng.meta("t") is None

    run(_main())

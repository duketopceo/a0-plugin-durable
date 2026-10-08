"""Step registry + the built-in api_call step."""

import pytest

from usr.plugins.durable.helpers import registry

from conftest import run


def test_register_get_reset():
    async def fn(**kw):
        return {}

    registry.register_step("x", fn)
    assert registry.get_step("x") is fn
    assert "x" in registry.registered_steps()
    registry.reset()
    assert registry.get_step("x") is None


def test_register_rejects_bad_input():
    registry.register_step("", None)
    registry.register_step("ok", "not-callable")
    assert registry.get_step("ok") is None


def test_register_defaults_installs_api_call():
    registry.reset()
    registry.register_defaults()
    assert registry.get_step("api_call") is not None


def test_api_call_rejects_non_http_scheme():
    async def _main():
        with pytest.raises(ValueError, match="scheme"):
            await registry.api_call_step("GET", "file:///etc/passwd")
        with pytest.raises(ValueError, match="scheme"):
            await registry.api_call_step("GET", "gopher://x")

    run(_main())


def test_api_call_inbound_headers_filtered():
    """Underscore-prefixed header names must never reach the wire."""
    sent = {}

    import http.server
    import threading

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            sent.update(dict(self.headers))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok": true}')

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        async def _main():
            return await registry.api_call_step(
                "GET",
                f"http://127.0.0.1:{srv.server_port}/",
                headers={"X-Ok": "1", "_secret": "leak"},
            )

        result = run(_main())
        assert result["status"] == 200
        assert result["body"] == {"ok": True}
        assert "X-Ok" in sent or "x-ok" in {k.lower(): v for k, v in sent.items()}
        assert not any(k.startswith("_") for k in sent)
    finally:
        srv.shutdown()
        srv.server_close()


def test_api_call_strips_sensitive_response_headers():
    import http.server
    import threading

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Set-Cookie", "session=abc")  # must not be journaled
            self.send_header("X-Keep", "1")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        async def _main():
            return await registry.api_call_step(
                "GET", f"http://127.0.0.1:{srv.server_port}/"
            )

        result = run(_main())
        lowered = {k.lower() for k in result["headers"]}
        assert "set-cookie" not in lowered
        assert "x-keep" in lowered
    finally:
        srv.shutdown()
        srv.server_close()

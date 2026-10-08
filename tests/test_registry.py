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


def test_api_call_refuses_redirects():
    """urllib forwards the caller's headers cross-host on redirect — a 3xx
    must surface as a result, never a follow."""
    import http.server
    import threading

    hits = []

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            if self.path == "/":
                self.send_response(302)
                self.send_header("Location", "/secret")
                self.end_headers()
            else:  # would only run if the redirect were followed
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"{}")

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        async def _main():
            return await registry.api_call_step(
                "GET",
                f"http://127.0.0.1:{srv.server_port}/",
                headers={"Authorization": "Bearer leak-me"},
            )

        result = run(_main())
        assert result["status"] == 302
        assert hits == ["/"]  # the redirect target was never hit
    finally:
        srv.shutdown()
        srv.server_close()


def test_api_call_caps_response_body():
    """Journaled results can't exceed 1 MiB — the read is capped."""
    import http.server
    import threading

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"x" * (2 << 20))  # 2 MiB

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
        assert result["status"] == 200
        assert len(str(result["body"])) <= (1 << 20)  # hard cap
    finally:
        srv.shutdown()
        srv.server_close()

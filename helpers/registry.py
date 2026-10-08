"""Step registry — the host-to-engine bridge.

Engines execute *named* steps; callables are registered here by the host at
configure time. This replaces Khan's ``_call``/``_result``/``_transport``
arg injection, which cannot survive a real engine boundary (callables don't
serialize over Temporal gRPC or Restate HTTP). With the registry, the
Restate endpoint and the local runner resolve work identically: look the
step name up, get a callable that closes over the live a0 runtime.

Built-in default: ``api_call`` ships a stdlib-urllib implementation so
journaled HTTP steps work with zero host wiring. ``llm_call``/``tool_call``
MUST be registered by the host — without them, tasks needing those steps
fail cleanly with a "no step registered" error. ``api_call`` is a utility
for host-registered steps (or for tasks whose host maps tool names onto it);
the engine loop only ever dispatches ``llm_call`` and ``tool_call``.

Registration lifecycle: ``register_defaults`` installs ``api_call`` with
setdefault semantics, so a host may register its own before OR after
``runtime.configure()``. Entries survive ``runtime.shutdown()``; the full
``runtime._reset()``/``hooks.uninstall()`` path clears the registry.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from usr.plugins.durable.helpers import LOG_NAME

log = logging.getLogger(LOG_NAME)

StepFn = Callable[..., Awaitable[dict[str, Any]]]

_steps: dict[str, StepFn] = {}

_HTTP_TIMEOUT_SECONDS = 30.0
# Never trust a journaled HTTP result bigger than this — it lands in sqlite.
_MAX_BODY_BYTES = 1 << 20  # 1 MiB
# Response headers dropped before journaled results — auth/cookie material
# must not persist into the journal.
_SENSITIVE_RESPONSE_HEADERS = frozenset({
    "set-cookie",
    "authorization",
    "proxy-authorization",
    "proxy-authenticate",
    "www-authenticate",
    "x-api-key",
    "api-key",
    "apikey",
    "x-auth-token",
    "x-access-token",
    "x-session-id",
    "cookie",
    "x-csrf-token",
    "x-xsrf-token",
})


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects — following one re-issues the request with the
    caller's headers (Authorization included) to a host the step author never
    named (urllib forwards headers cross-host — CPython gh-77842). A 3xx
    surfaces as its own response instead."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect())


def register_step(name: str, fn: StepFn) -> None:
    """Register/replace a step callable. Never raises."""
    try:
        if not name or not callable(fn):
            return
        _steps[str(name)] = fn
    except Exception:
        pass


def get_step(name: str) -> StepFn | None:
    return _steps.get(name)


def registered_steps() -> list[str]:
    return sorted(_steps)


def reset() -> None:
    """Clear all registrations (test/reload hook)."""
    _steps.clear()


async def api_call_step(
    method: str, url: str, headers: dict | None = None, body: dict | None = None
) -> dict[str, Any]:
    """Default ``api_call`` step: stdlib HTTP fetch in a worker thread.

    Only http/https schemes; redirects are refused (they would forward the
    caller's headers to an unintended host); a finite timeout bounds every
    call; the response body is capped at 1 MiB; sensitive response headers
    are stripped before the result is journaled.

    This is intentionally unrestricted HTTP — steps are SSRF-capable by
    design (reaching internal services is a feature). The host decides which
    task inputs are trusted.
    """
    parsed = urlparse(str(url))
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(
            f"Unsupported URL scheme: {parsed.scheme!r}; only http and https allowed"
        )
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(str(url), data=data, method=str(method).upper())
    for key, value in (headers or {}).items():
        if not str(key).startswith("_"):
            req.add_header(str(key), str(value))

    def _fetch() -> tuple[int, dict, bytes]:
        try:
            with _OPENER.open(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
                return resp.status, dict(resp.headers), resp.read(_MAX_BODY_BYTES)
        except urllib.error.HTTPError as e:
            # refused redirect / error status still produces a step result —
            # the caller sees the status code instead of an exception
            return e.code, dict(e.headers or {}), e.read(_MAX_BODY_BYTES)

    status, resp_headers, raw = await asyncio.to_thread(_fetch)
    filtered = {
        k: v for k, v in resp_headers.items() if k.lower() not in _SENSITIVE_RESPONSE_HEADERS
    }
    try:
        parsed_body: Any = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        parsed_body = raw.decode("utf-8", errors="replace")
    return {"status": status, "headers": filtered, "body": parsed_body}


def register_defaults() -> None:
    """Install the built-in steps. Called by runtime.configure — setdefault
    so a host's own api_call (registered either before or after configure)
    always wins."""
    _steps.setdefault("api_call", api_call_step)

"""Process-wide reuse of client SSL contexts.

Loading a CA bundle into a new context costs ~10-15 ms with OpenSSL 3 and does
not scale across threads, so building one per connection caps how fast a
process can open connections. A cached context is reused only by callers with
identical TLS options and unchanged CA files. Configurations with a client
certificate are never cached. Contexts are rebuilt after ``_TTL_SECONDS`` so
trust-store changes that no file signature covers (e.g. the Windows
certificate store) still apply without a restart.
"""

import os
import ssl
import threading
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple

from urllib3.util.ssl_ import ALPN_PROTOCOLS

if TYPE_CHECKING:
    from databricks.sql.types import SSLOptions

_MAX_ENTRIES = 32
_TTL_SECONDS = 600

_lock = threading.Lock()
_contexts: "OrderedDict[tuple, Tuple[ssl.SSLContext, float]]" = OrderedDict()
_build_locks: Dict[tuple, Any] = {}

_BASE_CONTEXT_CLASS = ssl.SSLContext


class _SharedSSLContext(_BASE_CONTEXT_CLASS):  # type: ignore[valid-type,misc]
    """A cached context that ignores same-value writes.

    urllib3 re-applies ALPN and verify_mode to its context on every connection.
    SSL_new() copies the context's ALPN buffer without the GIL, so replacing
    that buffer while another thread connects can corrupt the ClientHello.
    """

    def set_alpn_protocols(self, alpn_protocols):
        protocols = list(alpn_protocols)
        if protocols != getattr(self, "_shared_alpn_protocols", None):
            super().set_alpn_protocols(protocols)
            self._shared_alpn_protocols = protocols

    @property
    def verify_mode(self):
        return super().verify_mode

    @verify_mode.setter
    def verify_mode(self, value):
        if super().verify_mode != value:
            _BASE_CONTEXT_CLASS.verify_mode.__set__(self, value)


def _make_shareable(context: ssl.SSLContext) -> bool:
    # Only plain stdlib contexts are shared; others (e.g. pyOpenSSL) stay per caller.
    if type(context) is not _BASE_CONTEXT_CLASS:
        return False
    context.__class__ = _SharedSSLContext
    context.set_alpn_protocols(ALPN_PROTOCOLS)
    return True


def _file_signature(path) -> Optional[Tuple]:
    # Rewriting or replacing a CA file yields a new key, so rotation is picked up.
    if not path:
        return None
    try:
        st = os.stat(path)
    except (OSError, TypeError, ValueError):
        return (path, None)
    # ctime too: in-place rewrites that preserve mtime (cp -p, rsync -t) still change it.
    return (path, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def _cache_key(kind: str, ssl_options: "SSLOptions") -> tuple:
    defaults = ssl.get_default_verify_paths()  # honors SSL_CERT_FILE / SSL_CERT_DIR
    return (
        kind,
        bool(ssl_options.tls_verify),
        bool(ssl_options.tls_verify_hostname),
        _file_signature(ssl_options.tls_trusted_ca_file),
        _file_signature(defaults.cafile),
        defaults.capath,
        os.environ.get("SSLKEYLOGFILE"),
    )


def _fresh_context(key: tuple) -> Optional[ssl.SSLContext]:
    # Caller holds _lock.
    entry = _contexts.get(key)
    if entry is None or time.monotonic() - entry[1] > _TTL_SECONDS:
        return None
    _contexts.move_to_end(key)
    return entry[0]


def is_cacheable(ssl_options: "SSLOptions") -> bool:
    return not (
        ssl_options.tls_client_cert_file or ssl_options.tls_client_cert_key_file
    )


def get_ssl_context(
    kind: str, ssl_options: "SSLOptions", build: Callable[[], ssl.SSLContext]
) -> ssl.SSLContext:
    """Return the shared context for ``kind`` and ``ssl_options``, building it once.

    ``kind`` separates builders that configure contexts differently. urllib3
    writes a pool's ``cert_reqs`` and ALPN protocols back onto its context on
    every connection; the shared context skips those writes only when nothing
    changes, so every pool sharing a context must use TLS pool kwargs derived
    only from ``ssl_options`` (and no ``assert_hostname``). Do not otherwise
    modify the returned context. Contexts other than plain ``ssl.SSLContext``
    are returned without caching.
    """
    if not is_cacheable(ssl_options):
        return build()

    key = _cache_key(kind, ssl_options)
    with _lock:
        context = _fresh_context(key)
        if context is not None:
            return context
        key_lock = _build_locks.setdefault(key, threading.Lock())

    # Build outside the shared lock so a slow CA file only delays its own key.
    with key_lock:
        with _lock:
            context = _fresh_context(key)
        if context is not None:
            return context
        try:
            context = build()
            shareable = _make_shareable(context)
        except BaseException:
            _forget_build_lock(key, key_lock)
            raise
        if not shareable:
            _forget_build_lock(key, key_lock)
            return context
        with _lock:
            _contexts[key] = (context, time.monotonic())
            _contexts.move_to_end(key)
            if len(_contexts) > _MAX_ENTRIES:
                _contexts.popitem(last=False)
        _forget_build_lock(key, key_lock)
        return context


def _forget_build_lock(key: tuple, key_lock: Any) -> None:
    with _lock:
        if _build_locks.get(key) is key_lock:
            del _build_locks[key]


def clear() -> None:
    with _lock:
        _contexts.clear()


def _reset_after_fork() -> None:
    # A fork during a build would leave the child's copies of the locks held.
    global _lock
    _lock = threading.Lock()
    _build_locks.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)

"""Process-wide reuse of client SSL contexts.

Loading a CA bundle into a new context costs ~10-15 ms with OpenSSL 3 and does
not scale across threads, so building one per connection caps how fast a
process can open connections. A cached context is reused only by callers with
identical TLS options and unchanged CA files. Configurations with a client
certificate are never cached.
"""

import os
import ssl
import threading
from collections import OrderedDict
from typing import TYPE_CHECKING, Callable, Optional, Tuple

if TYPE_CHECKING:
    from databricks.sql.types import SSLOptions

_MAX_ENTRIES = 32

_lock = threading.Lock()
_contexts: "OrderedDict[tuple, ssl.SSLContext]" = OrderedDict()


def _file_signature(path) -> Optional[Tuple]:
    # Rewriting or replacing a CA file yields a new key, so rotation is picked up.
    if not path:
        return None
    try:
        st = os.stat(path)
    except (OSError, TypeError, ValueError):
        return (path, None)
    return (path, st.st_ino, st.st_size, st.st_mtime_ns)


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


def is_cacheable(ssl_options: "SSLOptions") -> bool:
    return not (
        ssl_options.tls_client_cert_file or ssl_options.tls_client_cert_key_file
    )


def get_ssl_context(
    kind: str, ssl_options: "SSLOptions", build: Callable[[], ssl.SSLContext]
) -> ssl.SSLContext:
    """Return the shared context for ``kind`` and ``ssl_options``, building it once.

    ``kind`` separates builders that configure contexts differently. Callers
    must not modify the returned context.
    """
    if not is_cacheable(ssl_options):
        return build()

    key = _cache_key(kind, ssl_options)
    with _lock:
        context = _contexts.get(key)
        if context is None:
            context = build()
            _contexts[key] = context
            if len(_contexts) > _MAX_ENTRIES:
                _contexts.popitem(last=False)
        else:
            _contexts.move_to_end(key)
        return context


def clear() -> None:
    with _lock:
        _contexts.clear()


def _reset_after_fork() -> None:
    # A fork during a build would leave the child's copy of the lock held.
    global _lock
    _lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)

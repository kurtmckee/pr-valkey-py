from collections.abc import Callable, Mapping
from types import MappingProxyType
from urllib.parse import ParseResult, parse_qs, unquote, urlparse

from valkey.asyncio.connection import (
    ConnectKwargs,
)
from valkey.asyncio.connection import SSLConnection as SSLConnectionAsync
from valkey.asyncio.connection import (
    UnixDomainSocketConnection as UnixDomainSocketConnectionAsync,
)
from valkey.connection import SSLConnection, UnixDomainSocketConnection


def _to_bool(value: str) -> bool | None:
    if not value:
        return None
    upper_value = value.upper()
    if upper_value in FALSE_STRINGS:
        return False
    if upper_value in TRUE_STRINGS:
        return True
    raise ValueError(f"'{value}' is not a valid boolean value.")


FALSE_STRINGS = ("0", "F", "FALSE", "N", "NO")
TRUE_STRINGS = ("1", "T", "TRUE", "Y", "YES")


def _to_int(value: str) -> int:
    """Convert a string to an integer value greater than, or equal to, zero.

    This is stricter than simply calling `int()`.

    * Unicode integer values like `४` are rejected.
    * Negative integer values are rejected.

    :raises ValueError: If the string is not an ASCII integer value >= 0.
    """

    number = int(value)
    if value.isascii() and number >= 0:
        return number
    raise ValueError(f"'{value} is not an ASCII integer value >= 0.")


URL_QUERY_ARGUMENT_PARSERS: Mapping[str, Callable[..., object]] = MappingProxyType(
    {
        "db": _to_int,
        "socket_timeout": float,
        "socket_connect_timeout": float,
        "socket_keepalive": _to_bool,
        "retry_on_timeout": _to_bool,
        "max_connections": _to_int,
        "health_check_interval": _to_int,
        "client_capa_redirect": _to_bool,
        "ssl_check_hostname": _to_bool,
        "timeout": float,
    }
)


supported_schemes = ["valkey", "valkeys", "redis", "rediss", "unix"]
# "unix:" is an undocumented special case.
supported_prefixes = tuple([f"{scheme}://" for scheme in supported_schemes] + ["unix:"])


def parse_url(url: str, async_connection: bool) -> ConnectKwargs:
    # Reject unknown schemes.
    lower_url = url[:10].lower()
    if not lower_url.startswith(supported_prefixes):
        raise ValueError(
            f"Valkey URL must specify one of the following schemes {supported_schemes}"
        )

    parsed: ParseResult = urlparse(url)
    kwargs: ConnectKwargs = {}

    for name, value_list in parse_qs(parsed.query).items():
        if value_list and len(value_list) > 0:
            value = unquote(value_list[0])
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    kwargs[name] = parser(value)
                except (TypeError, ValueError):
                    raise ValueError(f"Invalid value for `{name}` in connection URL.")
            else:
                kwargs[name] = value

    if parsed.username:
        kwargs["username"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)

    if parsed.scheme == "unix":
        if parsed.path:
            kwargs["path"] = unquote(parsed.path)
        kwargs["connection_class"] = (
            UnixDomainSocketConnectionAsync
            if async_connection
            else UnixDomainSocketConnection
        )

    else:
        if parsed.hostname:
            kwargs["host"] = unquote(parsed.hostname)
        if parsed.port:
            kwargs["port"] = int(parsed.port)

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if parsed.path and "db" not in kwargs:
            try:
                path = unquote(parsed.path).strip("/")
                if path:
                    kwargs["db"] = _to_int(path)
            except ValueError:
                raise ValueError("Invalid value for DB in connection URL path.")

        if parsed.scheme in ("valkeys", "rediss"):
            kwargs["connection_class"] = (
                SSLConnectionAsync if async_connection else SSLConnection
            )

    return kwargs

import urllib.parse

import pytest
from valkey._parsers.url_parser import parse_url
from valkey.asyncio.connection import SSLConnection as AsyncSSLConnection
from valkey.asyncio.connection import (
    UnixDomainSocketConnection as AsyncUnixDomainSocketConnection,
)
from valkey.connection import SSLConnection, UnixDomainSocketConnection


@pytest.mark.parametrize(
    "url, async_connection, expected_connection_class",
    (
        ("valkey://localhost", False, None),
        ("valkey://localhost", True, None),
        ("valkeys://localhost", False, SSLConnection),
        ("valkeys://localhost", True, AsyncSSLConnection),
        ("redis://localhost", False, None),
        ("redis://localhost", True, None),
        ("rediss://localhost", False, SSLConnection),
        ("rediss://localhost", True, AsyncSSLConnection),
        ("unix:///var/run/valkey.sock", False, UnixDomainSocketConnection),
        ("unix:///var/run/valkey.sock", True, AsyncUnixDomainSocketConnection),
        # Undocumented special cases
        ("unix:/var/run/valkey.sock", False, UnixDomainSocketConnection),
        ("unix:/var/run/valkey.sock", True, AsyncUnixDomainSocketConnection),
    ),
)
@pytest.mark.parametrize("morph", (str.upper, str.lower), ids=("upper", "lower"))
def test_url_parser_schema_and_connection_class_positive(
    morph, url, async_connection, expected_connection_class
):
    url = morph(url)
    connect_args = parse_url(url, async_connection=async_connection)
    if expected_connection_class is None:
        assert "connection_class" not in connect_args
    else:
        assert connect_args["connection_class"] is expected_connection_class


@pytest.mark.parametrize(
    "url",
    (
        pytest.param("redisx://localhost", id="redis*://-prefix"),
        pytest.param("redis:localhost", id="redis*-prefix"),
        pytest.param("valkeyx://localhost", id="valkey*://-prefix"),
        pytest.param("valkeyx:localhost", id="valkey*:-prefix"),
        pytest.param("unix/var/run/valkey.sock", id="unix-prefix"),
        pytest.param("bogus://localhost", id="bogus-scheme"),
    ),
)
def test_url_parser_schema_negative(url):
    with pytest.raises(ValueError, match="must specify one of the following schemes"):
        parse_url(url, async_connection=False)


@pytest.mark.parametrize(
    "username, password",
    (
        ("", ""),
        ("user", ""),
        ("", "pass"),
        ("user", "pass"),
        ("valkey://a1:a2@a3/a4?a5=🧪", "valkey://b1:b2@b3/b4?b5=📦"),
    ),
)
def test_url_parser_username_and_password(username, password):
    encoded_username = urllib.parse.quote_plus(username)
    encoded_password = urllib.parse.quote_plus(password)
    url = f"valkey://{encoded_username}:{encoded_password}@localhost"

    connect_args = parse_url(url, async_connection=False)
    assert connect_args["host"] == "localhost"

    if username:
        assert connect_args["username"] == username
    else:
        assert "username" not in connect_args

    if password:
        assert connect_args["password"] == password
    else:
        assert "password" not in connect_args


@pytest.mark.parametrize(
    "trailing, expected_db",
    (
        pytest.param("", None, id="unspecified"),
        pytest.param("/", None, id="nothing-in-path"),
        pytest.param("/11", 11, id="db-in-path"),
        pytest.param("/11/", 11, id="db-with-trailing-slash-in-path"),
        pytest.param("/11?db=", 11, id="db-in-path-overrides-empty-db-query-param"),
        pytest.param("?db=", None, id="nothing-in-query-param"),
        pytest.param("?db=11", 11, id="db-in-query-param"),
        pytest.param("?db=11&db=22", 11, id="select-first-db-in-query-param"),
        pytest.param("/11?db=22", 22, id="prefer-db-in-query-param"),
    ),
)
def test_url_parser_db_positive(trailing, expected_db):
    parsed = parse_url(f"valkey://localhost{trailing}", False)
    assert parsed["host"] == "localhost"

    if expected_db is not None:
        assert parsed["db"] == expected_db
    else:
        assert "db" not in parsed


@pytest.mark.parametrize(
    "trailing",
    (
        pytest.param("/%GG", id="bad-quote-in-path"),
        pytest.param("/%FF%FE", id="invalid-unicode-quote-in-path"),
        pytest.param("/11.11", id="float-in-path"),
        pytest.param("/11/22", id="slash-in-path"),
        pytest.param("/४४", id="unicode-int-in-path"),
        pytest.param("/-1", id="negative-int-in-path"),
        pytest.param("/abc", id="text-in-path"),
        pytest.param("?db=%GG", id="bad-quote-in-query-param"),
        pytest.param("?db=%FF%FE", id="invalid-unicode-quote-in-query-param"),
        pytest.param("?db=11.11", id="float-in-query-param"),
        pytest.param("?db=४४", id="unicode-int-in-query-param"),
        pytest.param("?db=-1", id="negative-int-in-query-param"),
        pytest.param("?db=abc", id="text-in-query-param"),
    ),
)
def test_url_parser_db_negative(trailing):
    with pytest.raises(ValueError):
        parse_url(f"valkey://localhost{trailing}", False)

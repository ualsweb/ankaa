from typing import Any

__version__: str
HAS_TEST_SERVER: bool

class Dataset:
    """Native in-process dataset. Results implement ``__arrow_c_stream__``."""

    path: str
    def sql(self, query: str) -> Any: ...
    def schema(self, query: str) -> Any: ...
    def tables(self) -> list[str]: ...
    def stats(self) -> dict[str, int]: ...

class Client:
    """Native Arrow Flight client."""

    addr: str
    def sql(self, query: str) -> Any: ...
    def schema(self, query: str) -> Any: ...
    def tables(self) -> list[str]: ...

def open_capture(path: str, expect_snapshots: bool = False) -> Dataset: ...
def connect(
    addr: str,
    token: str | None = None,
    tls_ca: str | None = None,
    tls_domain: str | None = None,
    tls_cert: str | None = None,
    tls_key: str | None = None,
    connect_timeout_s: float = 10.0,
) -> Client: ...

class TestServer:
    """Only with the ``test-server`` cargo feature."""

    port: int
    addr: str
    def stop(self) -> None: ...
    def __enter__(self) -> "TestServer": ...
    def __exit__(self, *args: object) -> None: ...

def _test_server(
    capture_path: str, token: str | None = None, expect_snapshots: bool = False
) -> tuple[int, TestServer]: ...

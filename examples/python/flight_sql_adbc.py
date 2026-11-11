"""ANKAA over Flight SQL with ADBC: an optional smoke test, not part of CI.

    pip install adbc-driver-flightsql pyarrow

Start a server, for example offline from the capture in the repository:

    ANKAA_TOKEN=devtoken cargo run --release -p ankaad -- serve \
      --streams btcusdt@trade,btcusdt@depth@100ms \
      --url file://bench/data/binance-btcusdt-trade-depth-20s.ankc --no-depth-snapshots

then run this script against it:

    ANKAA_TOKEN=devtoken python examples/python/flight_sql_adbc.py [grpc://127.0.0.1:50051]

With TLS use `grpc+tls://host:50051` and set ANKAA_TLS_CA to the CA file.
The script exits non-zero at the first check that fails.
"""

from __future__ import annotations

import os
import sys

import adbc_driver_flightsql.dbapi as flight_sql
import adbc_driver_manager
from adbc_driver_flightsql import DatabaseOptions


def tls_options() -> dict[str, str]:
    ca = os.environ.get("ANKAA_TLS_CA")
    if not ca:
        return {}
    with open(ca, encoding="ascii") as f:
        return {DatabaseOptions.TLS_ROOT_CERTS.value: f.read()}


def connect(uri: str, **options: str):
    # autocommit: the server has no transactions to open
    return flight_sql.connect(uri, db_kwargs={**tls_options(), **options}, autocommit=True)


def main() -> int:
    uri = sys.argv[1] if len(sys.argv) > 1 else "grpc://127.0.0.1:50051"
    token = os.environ.get("ANKAA_TOKEN", "")
    bearer = {DatabaseOptions.AUTHORIZATION_HEADER.value: f"Bearer {token}"} if token else {}

    # 1. the token as a bearer header on every call
    with connect(uri, **bearer) as conn:
        info = conn.adbc_get_info()
        print("server:", info.get("vendor_name"), info.get("vendor_version"))
        assert info.get("vendor_name") == "ANKAA", info

        with conn.cursor() as cur:
            # the DB-API layer prepares every statement before it runs it
            cur.execute("SELECT count(*) AS n FROM trades")
            table = cur.fetch_arrow_table()
            print("trades:", table.column("n")[0].as_py())
            assert table.num_rows == 1

            cur.execute("SELECT ts, price, size FROM trades ORDER BY ts LIMIT 5")
            head = cur.fetch_arrow_table()
            assert head.schema.names == ["ts", "price", "size"], head.schema
            print(head.to_pydict())

            # the schema of a query, nothing executed
            schema = cur.adbc_execute_schema("SELECT ts, price FROM trades")
            assert schema.names == ["ts", "price"], schema

        types = conn.adbc_get_table_types()
        assert "TABLE" in types, types
        objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
        tables = sorted(
            t["table_name"]
            for catalog in objects
            for schema in catalog["catalog_db_schemas"] or []
            for t in schema["db_schema_tables"] or []
        )
        print("tables:", tables)
        assert "trades" in tables, tables
        trades = conn.adbc_get_table_schema("trades")
        assert "price" in trades.names, trades

        # read-only: a write is refused, whichever way it is sent
        with conn.cursor() as cur:
            for write in ("DROP TABLE trades", "CREATE TABLE t AS SELECT 1"):
                try:
                    cur.execute(write)
                except adbc_driver_manager.Error as e:
                    print("refused:", write, "->", type(e).__name__)
                else:
                    raise AssertionError(f"{write!r} was accepted")
            try:
                cur.adbc_statement.set_sql_query("DELETE FROM trades")
                cur.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                print("refused: update ->", type(e).__name__)
            else:
                raise AssertionError("an update was accepted")
            cur.execute("SELECT count(*) FROM trades")
            assert cur.fetchone()[0] == table.column("n")[0].as_py()

    if token:
        # 2. the token as the password of a user (the Handshake call with Basic authentication)
        with connect(uri, username="ankaa", password=token) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS x")
            assert cur.fetchone() == (1,)
            print("user name and password: ok")

        # 3. a wrong token gets nowhere
        for wrong in (
            {DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer not-the-token"},
            {"username": "ankaa", "password": "not-the-token"},
            {},
        ):
            try:
                with connect(uri, **wrong) as conn, conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchall()
            except adbc_driver_manager.Error as e:
                print("refused:", sorted(wrong) or "no credentials", "->", type(e).__name__)
            else:
                raise AssertionError(f"accepted without the token: {sorted(wrong)}")

    print("ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())

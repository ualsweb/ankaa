# RFC-0002: Dictionary columns are names in SQL

- **Status:** Accepted (implemented 2026-11-11)
- **Date:** 2026-11-11
- **Affects:** `ankaa-sql` (`names`, `provider`, `arrow_conv`, `dense_agg`, `eager_agg`, `functions`), `bins/ankaad`
  (`dict_names`, `serve`), config `[server] dict_columns`; PRD F-08 (SQL surface). **Breaking** for queries that
  join `dictionaries` or `symbols` on ids (one setting restores the old behaviour).

## Summary

A `dict` column stores a dense `u32` id per row and the names once. Until now SQL showed the id: every query
that filtered by name or returned one had to join `dictionaries` (or `symbols`). SQL now shows the column as a
dictionary-encoded string, Arrow `Dictionary(UInt32, Utf8)`: users write `WHERE host = 'web-1'` and
`GROUP BY region` as on any other engine, and the engine keeps working on the ids. Nothing changes in storage,
the log, segments or replication.

## Motivation

- **Every competing engine takes names.** In the TSBS-shaped comparison (`bench/telemetry`) the ANKAA dialect
  needed `hostname IN (SELECT id FROM dictionaries …)` in 13 of 15 queries and ten joins to return a full row
  (`high-cpu-1`: about 6 ms of the query was joining ten dictionaries for a handful of rows). The other four
  dialects are plain SQL.
- **Tools.** Grafana, BI tools over the Postgres protocol and notebooks generate filters on the column they
  see. They cannot be taught to join a side table.
- **The id is an internal number.** It depends on arrival order; showing it invites dashboards that hard-code it.

## Design

**Names, published.** `ankaa_sql::Names` holds one Arrow string array per dictionary (id = position) and a
name → id map. The ingest loop, the only writer of a dictionary, publishes its names before it shows new rows
to queries (before every snapshot swap), so a query never sees an id without its name. Publishing appends: the
array is rebuilt only when names were added (the map grows by the new names only); ids never change, and a
batch built earlier keeps its own reference to the array it was built with.

**Batches.** A chunk's id buffer is used in place as the dictionary keys; the values are the published array,
shared by reference by every batch of every table that uses that dictionary. No string is copied per row, and
since all batches point at the same values Arrow and DataFusion never merge or re-encode dictionaries when they
concatenate, repartition or hash them.

**Predicates on ids.** `col = 'x'`, `col <> 'x'`, `col IN (…)`, `col NOT IN (…)` and `OR`s of them against string
literals are taken by the table (`Exact` pushdown) and translated once per query into a set of ids. Per chunk
the zone map of the id column gives one of three verdicts: *skip* (no id of the set in range, or the name was
never seen), *all* (the chunk holds one id and it matches: a partition-key chunk), *rows*. Only when some chunk
needs *rows* the scan is wrapped in a filter over the ids. Any other predicate that involves one name column
and nothing else that varies by row (`LIKE`, `<`, a function of the column, a regular expression) is evaluated
once per query over the dictionary's names, typed as the column is, and the accepted ids go through the same
three verdicts (`ChunkTable::pred_over_names`): measured on Axion, `LIKE` on a 1,000-name column over 26 M rows
went from compiling the pattern for every batch (490 ms) to once. What needs the names of a row together with
another column runs in DataFusion on the dictionary arrays, row by row.

**Aggregates and table functions.** The dense aggregate path and the aggregate-before-join rewrite read the
keys of a dictionary column and group on them as before; their output key is a dictionary over the same values.
`ohlcv`, `rolling`, `percentiles`, `gapfill` and `asof_join` take and return the key as names; `asof_join`
between tables that do not share a dictionary stays refused. Continuous-query tables carry the names of their
input's key column.

**Clients receive strings.** The Flight encoder expands a dictionary column into its value type while it writes
a batch (arrow-flight's default, `DictionaryHandling::Hydrate`), and the Postgres listener reports `text`. Every
schema the Flight service announces (`GetFlightInfo`, `GetSchema`, `ListFlights`, Flight SQL `GetTables`) is
mapped the same way (`ankaa_flight::wire_schema`), so drivers that allocate result columns from the announced
schema (JDBC, ADBC) meet the stream they were told about. Sending dictionaries was considered: it saves bytes
on wide results, but the names array would travel again with every batch, and not every Flight SQL driver reads
dictionary batches or a dictionary replaced mid-stream.

**`server.dict_columns`** = `"names"` (default) or `"ids"` (also `ankaad serve --dict-ids`): `"ids"` keeps the
previous surface exactly. It is a view setting, free to change at any restart. In `"names"` mode an old
`JOIN dictionaries d ON d.id = t.col` stops with a cast error (`Cannot cast string 'web-1' to value of UInt32
type`) rather than returning a different answer. The one exception is a column whose names are themselves
numbers: there the old join runs and compares ids with those numbers, which the upgrade note calls out.

## Costs considered

- Building a dictionary array validates that every key is below the number of values (one pass over the ids
  of a batch, vectorised). The crate forbids `unsafe`, and the check is also what makes a
  corrupted segment an error instead of an out-of-bounds read.
- A result that returns a name column pays one string copy per output row and column at the transport, as
  every engine that returns strings does. A later option for clients that ask for it: dictionary batches with
  the values compacted per batch (4 bytes per cell on the wire).
- Statistics for a dictionary column are not given to the planner as min/max (they are ids, not strings).

## Not changed

`ankaad replay` and the in-process Python capture session show ids (the comparison harness for market data
exports and checks ids); `dictionaries` and `symbols` remain as tables.

## Alternatives considered

- **Plain `Utf8` columns.** Simplest for clients, but every scan would materialise strings per row and every
  `GROUP BY` would hash them.
- **A SQL rewrite that inserts the joins.** Keeps numeric columns, but breaks on anything the rewriter does not
  understand and the result types would still be surprising in tools.
- **`Utf8View` with shared buffers.** 16 bytes per row instead of 4 and no grouping on ids.

## Verification

- `crates/ankaa-sql/tests/names.rs`: queries written for ids are refused (`id_queries_fail_loudly`); 26 statements (filters of every shape, unknown names, `NOT IN`, mixed with
  time ranges and limits, `GROUP BY`, `ORDER BY`, joins, `DISTINCT`, `LIKE`) over three chunk layouts, each
  compared with the same data as plain strings in a DataFusion `MemTable`; a plan check that pushed predicates
  leave no residual filter when no chunk needs one; every table function.
- `bins/ankaad/tests/e2e.rs::dict_columns_are_names_in_sql`: live ingest, new names while querying, restart,
  continuous-query tables, `gapfill`, and the `"ids"` mode. Postgres: `dictionary_columns_are_text_for_postgres_clients`.
- `bench/telemetry`: the ANKAA dialect is now the same SQL as the reference engine's and is diffed against it.

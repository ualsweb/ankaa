# ADR-0001: Product identity: engine + server + Python, not a latency kernel

- **Status:** Accepted 2026-11-09
- **Context:** Review of the previous product definition.

## Decision
ANKAA is an open-source, Arm-native real-time tick-data engine with SQL (Apache DataFusion), Arrow Flight and Python as first-class surfaces, plus native time-series operators. The bare-metal low-latency ingest path (`ankaa-wire`) is a Phase 3 showcase module, not a v1.0 requirement.

## Consequences
Scope moves from v3.2's 132-core AF_XDP/TX topology to ingest, hot store, operators and interfaces. Latency claims are published only as measured on named hardware with NIC timestamps.

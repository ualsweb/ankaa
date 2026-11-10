# Measurement protocol

Every performance figure published by this project ships with enough to reproduce it:

1. **Hardware**: cloud instance type or exact SKU, CPU model, `lscpu`, memory configuration, NUMA topology (`numactl -H`), NIC model where relevant.
2. **Software**: kernel version and boot parameters, Rust toolchain, exact git commit, build profile and features.
3. **Script**: the command that ran the benchmark, committed under `bench/`, plus the input data or how to regenerate it.
4. **Raw results**: histograms (fixed-point buckets, no sketch on the measurement path), not only percentiles.
5. **Clock chain** for latency figures: which clock stamped the start and the end. Wire-to-queryable latency uses NIC hardware timestamps in the PTP hardware clock (PHC) domain and converts the CPU counter to PHC with an offset calibrated at start and re-verified periodically; the calibration **residual is published with the number**. Software receive timestamps are reported separately and labelled informative.
6. **Comparisons**: only against open-source systems, on the same instance, same data, same query set, with their configuration committed. No comparative claims against commercial systems without an independent audit.
7. **Emulation**: results from QEMU or other emulators are correctness evidence only and are never reported as performance.

Until the first Graviton4 measurements land, this repository publishes **no** throughput or latency numbers.

# Selective compaction benchmark

Standalone release benchmark: `cargo run --release --example compaction_selectivity`.

Each of four fresh fixtures writes 2,048 sorted fixed-width keys with 256-byte values, compacts them into 18 L1 partitions, then overwrites 24 keys and deletes 8 keys in the single range 960–991. The measured call compacts the resulting L0 file; fixture construction and verification are outside the timer. One run is a warmup and three are measured. Fixture fingerprinting reads every L0/L1 candidate before timing, so these are warm-cache measurements, not cold-storage latency.

All four runs removed the L0 input and exactly one previous L1 file, left 17 L1 files untouched, and wrote one new L1 output. Every untouched file retained the same path, byte length, and full-file FNV-1a fingerprint. Decoding all manifest-registered SSTables after compaction reconstructed the exact expected live key/value map, including replacement values and deleted-key absence. The resulting L1 ranges were nonoverlapping.

For each sample, selected inputs were 41,034 bytes, outputs were 31,214 bytes, untouched L1 files were 570,058 bytes, and all pre-compaction L0+L1 candidates were 611,092 bytes. Selected input bytes were 6.7148645376% of candidate bytes, leaving 93.2851354624% unselected. This is a whole-level byte-fraction counterfactual, not a measured speedup. No matched whole-level baseline was run, and no 10k/100k/1M growth benchmark was performed. Median of three measured compaction calls: 10.871812 ms (samples: 13.014773, 10.783875, and 10.871812 ms). The four fixtures retained 2,040 live unique records after 24 replacements and 8 deletes (including deletes of absent keys); each compaction removed one selected L1 file and left 17 byte-identical L1 files untouched. No lifetime write-amplification claim is made.

The raw JSON, command, environment, build/run logs, source hashes before and after execution, and release-binary hash are archived alongside this summary. The source hashes match before and after; the measured binary hash is recorded in `binary-hash.txt`.

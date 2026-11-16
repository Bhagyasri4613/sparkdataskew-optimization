# Design and Trade-offs

- **Synthetic data:** A fixed seed and deterministic transaction IDs make
  generated CSV reproducible. The generator assigns the configured fraction
  of transactions to a small hot-customer set.
- **Duplicate policy:** When transaction IDs collide, one row is retained in
  a deterministic order and the rest are quarantined. This is predictable but
  does not infer which conflicting business record is authoritative.
- **Salting:** A stable hash of `transaction_id` assigns each row to a salt
  bucket. The first aggregation groups by customer and salt; a second combines
  bucket totals by customer. This reduces the first-stage hot-key workload but
  adds a second aggregation and shuffle.
- **Benchmarking:** Both methods use the same cached accepted dataset, collect
  ordered results for correctness, and run repeatedly. The report shows the
  median; local timings are sensitive to JVM startup, cache state, shuffle
  partitions, and machine load. Salting is not expected to win on every small
  dataset.
- **Idempotency:** The pipeline overwrites `output/current/` on every run.
  Retries are full reruns, not incremental resumes. Keep historical/backfill
  outputs by pointing a copied config at a distinct output directory.
- **Local output:** Spark processes the data, then Python streams rows to CSV
  and JSON Lines files. This avoids Windows Hadoop `winutils.exe` setup and
  works for the project's 100,000-row learning workload. For larger cloud
  workloads, write Parquet directly from Spark on Linux instead.
- **Scope:** This is a local learning workload, not a production scheduler or
  transactional lakehouse. It does not provide atomic multi-file commits.

# Data Dictionary

## Entities and grain

- **Transaction input:** one row per transaction event. Its primary key is
  `transaction_id`; it belongs to a `customer_id`. There is no separate
  customer table in this starter project.
- **Customer aggregate:** one row per customer, derived from accepted
  transactions.
- `currency` is checked against the configured allowed-code list. In this
  small project that list acts as the currency reference data.

## Transaction input

| Field | Type in storage | Meaning and rule |
|---|---|---|
| `transaction_id` | String | Required unique transaction key. Duplicate IDs keep one deterministic row; additional rows are rejected. |
| `customer_id` | String | Required aggregation key. Generator concentrates most rows on a small hot-customer set. |
| `transaction_ts` | ISO-8601 string | Required parseable transaction timestamp. |
| `amount` | Decimal string | Required decimal greater than zero; stored as `decimal(18,2)`. |
| `currency` | String | Required supported code; configured values default to USD and EUR. |
| `merchant_id` | String | Required merchant identifier. |
| `source_file` | String | Input file name, added by the pipeline for traceability. |
| `run_id` | String | Identifier for the pipeline execution. |

Timestamps are ISO-8601 strings and are interpreted as UTC for this exercise.
IDs are stable strings; generated transaction IDs are deterministic for the
same seed and record count.

## Accepted transaction output

The accepted CSV contains the six transaction fields plus `source_file` and
`run_id`. Timestamps are ISO-8601 text and amounts are decimal text.

## Rejected transaction output

The JSON Lines quarantine contains the original six values, `source_file`, `run_id`,
`source_record` when Spark could not parse the input row, and
`rejection_reason`. Multiple validation failures are separated with `|`.
Duplicate rows use the reason `duplicate_transaction_id`.

## Customer aggregate output

| Field | Type | Meaning |
|---|---|---|
| `customer_id` | String | Customer grouped by the aggregation. |
| `transaction_count` | Long | Number of accepted transactions. |
| `total_amount` | Decimal | Sum of accepted transaction amounts. |

Normal and salted aggregation outputs are CSV files with the same schema and
are reconciled before the run succeeds.

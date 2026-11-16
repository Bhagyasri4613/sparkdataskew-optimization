# Spark Data Skew Optimization

This beginner project creates fake shopping transactions. Most belong to a few
customers, which gives Spark an intentionally uneven workload. It compares a
regular customer total with a salted customer total.

## How it works

```text
CSV file -> check rows -> Spark customer totals -> compare results and time
```

The project has one source table: transactions. One row means one transaction.
Invalid rows go to a reject folder. See [the data dictionary](docs/DATA_DICTIONARY.md)
for the fields and rules, and [the design notes](docs/DESIGN_NOTES.md) for the
main trade-off.

## Run it

You need Python 3.9 or 3.10, Java 17, and internet access to install the Python
packages. Open PowerShell in this folder:

```powershell
py -3.9 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m src.main --input data/input/sample_transactions.csv --run-id sample
```

If Java is installed but `java -version` says it is not recognized, add it to
the current PowerShell session before running Spark:

```powershell
$java = Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Filter java.exe -Recurse |
    Select-Object -First 1 -ExpandProperty FullName
$env:JAVA_HOME = Split-Path (Split-Path $java)
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
```

Keep that terminal open while installing packages and running the project.

The sample is quick to try. To generate the default 100,000 fake transactions
instead, run:

```powershell
python -m src.main
```

Change the size or seed with `--records 100000` and `--seed 42`. Use
`--help` to see the available options. Results are saved as CSV and JSON Lines
files in `output/current/`; logs are saved to `logs/`. Python writes these
local files so the project can run on Windows without extra Hadoop utilities.

## Check it

```powershell
python -m pytest
```

The tests compare totals to a separate expected-answer file and cover empty
input, malformed values, missing fields, duplicates, invalid amounts,
unsupported currencies, and late timestamps.

## Reruns and limits

Each run replaces `output/current/`, so rerunning the same input does not append
duplicate results. A retry starts the whole batch again; it does not resume a
half-finished Spark job. For a backfill, use a separate output path in a copied
config file.

Salting spreads a hot customer's transactions across temporary buckets, then
combines the bucket totals. This extra step may be slower for a small local
sample. The report measures both methods; correct totals matter even if
salting is not faster on your computer. For larger cloud workloads, a useful
next step is to write Parquet directly with Spark on Linux.

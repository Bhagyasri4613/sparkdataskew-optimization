from pathlib import Path
import os
import shutil
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Spark tests require a Java runtime.")

    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark-skew-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

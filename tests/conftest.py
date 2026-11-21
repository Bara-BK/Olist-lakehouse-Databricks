"""Shared pytest setup.

The Spark session is created at import time on purpose: the shared transformation module builds
Spark Column objects at import, and in local (non-Connect) PySpark that needs an active session.
"""
import os
import pathlib
import sys

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

# The shared logic lives in notebooks/olist_transforms.py (also valid plain Python)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "notebooks"))

_spark = (
    SparkSession.builder.master("local[2]")
    .appName("olist-tests")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
_spark.sparkContext.setLogLevel("ERROR")


@pytest.fixture(scope="session")
def spark():
    return _spark

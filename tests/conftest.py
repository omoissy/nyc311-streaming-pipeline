from __future__ import annotations

import copy
import json
import os
import sys
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # Python workers must run the same interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def api_response() -> dict:
    return json.loads((FIXTURES / "randomuser_response.json").read_text(encoding="utf-8"))


@pytest.fixture
def raw_users(api_response) -> list[dict]:
    # Deep copy so tests can mutate records freely.
    return copy.deepcopy(api_response["results"])


@pytest.fixture
def raw_user(raw_users) -> dict:
    return raw_users[0]

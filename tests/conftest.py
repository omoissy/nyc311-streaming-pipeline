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

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nyc311-unit-tests")
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
    return json.loads((FIXTURES / "nyc311_response.json").read_text(encoding="utf-8"))


@pytest.fixture
def raw_requests(api_response) -> list[dict]:
    return copy.deepcopy(api_response)


@pytest.fixture
def raw_request(raw_requests) -> dict:
    return raw_requests[0]

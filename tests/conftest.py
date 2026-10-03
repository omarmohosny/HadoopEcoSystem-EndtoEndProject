import glob
import importlib.machinery
import os
import sys

import pytest

SCRIPTS = os.environ.get("SCRIPTS_DIR", "/home/hadoop/kafka/scripts")
SPARK_HOME = os.environ.get("SPARK_HOME", "/home/hadoop/spark")

sys.path.insert(0, SCRIPTS)
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
sys.path.extend(glob.glob(os.path.join(SPARK_HOME, "python/lib/py4j-*-src.zip")))


@pytest.fixture(scope="session")
def classifier():
    # CLASSIFIER_PATH lets the same tests run against another version (e.g. a .bak file)
    path = os.environ.get("CLASSIFIER_PATH", os.path.join(SCRIPTS, "system_log_classifier.py"))
    return importlib.machinery.SourceFileLoader("classifier_under_test", path).load_module()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("system-logs-tests")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

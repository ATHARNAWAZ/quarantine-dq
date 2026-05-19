import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from quarantine.adapters.spark import quarantine_rdd


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.master("local[1]").appName("quarantine-test").getOrCreate()


def test_spark_rdd_split(spark):
    data = [
        {"order_id": 1, "revenue": 100.0},
        {"order_id": None, "revenue": 200.0},
        {"order_id": 3, "revenue": 300.0},
    ]
    rdd = spark.sparkContext.parallelize(data)
    clean_rdd, q_count = quarantine_rdd(
        rdd,
        pipeline="spark_test",
        stage="validate",
        not_null=["order_id"],
    )
    clean_records = clean_rdd.collect()
    assert len(clean_records) == 2
    assert q_count == 1

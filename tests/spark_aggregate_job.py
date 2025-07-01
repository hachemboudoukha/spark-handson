import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.spark_aggregate_job import calculate_population_by_dept

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_calculate_population_by_dept(spark):
    data = [
        Row(name="Alice", departement="75"),
        Row(name="Bob", departement="75"),
        Row(name="Charlie", departement="13")
    ]
    df = spark.createDataFrame(data)
    result_df = calculate_population_by_dept(df)
    result_data = result_df.collect()

    assert len(result_data) == 2
    assert result_data[0]["departement"] == "75"
    assert result_data[0]["nombre_population"] == 2
    assert result_data[1]["departement"] == "13"
    assert result_data[1]["nombre_population"] == 1

import pytest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import first_job
from src.fr.hymaia.exo2.spark_aggregate_job import second_job

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_integration_first_job(spark, tmp_path):
    clients_path = "src/resources/exo2/clients_bdd.csv"
    villes_path = "src/resources/exo2/city_zipcode.csv"
    output_path = tmp_path / "clean_output"

    first_job(spark, clients_path, villes_path).write.parquet(str(output_path))

    result_df = spark.read.parquet(str(output_path))
    assert result_df.count() > 0
    assert "departement" in result_df.columns

def test_integration_second_job(spark, tmp_path):
    clean_path = "data/exo2/clean"
    output_path = tmp_path / "aggregate_output"

    second_job(spark, clean_path).write.csv(str(output_path), header=True)

    result_df = spark.read.csv(str(output_path), header=True)
    assert result_df.count() > 0
    assert "departement" in result_df.columns
    assert "nombre_population" in result_df.columns

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.spark_clean_job import filter_majeur_clients, join_clients_with_city, add_department_column

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_filter_majeur_clients(spark):
    data = [
        Row(name="Alice", age=25, zip="75000"),
        Row(name="Bob", age=17, zip="75001"),
        Row(name="Charlie", age=30, zip="75002")
    ]
    df = spark.createDataFrame(data)
    result_df = filter_majeur_clients(df)
    result_data = result_df.collect()

    assert len(result_data) == 2
    assert result_data[0]["name"] == "Alice"
    assert result_data[1]["name"] == "Charlie"

def test_join_clients_with_city(spark):
    clients_data = [
        Row(name="Alice", age=25, zip="75000"),
        Row(name="Charlie", age=30, zip="75002")
    ]
    villes_data = [
        Row(zip="75000", city="Paris"),
        Row(zip="75002", city="Paris"),
        Row(zip="13000", city="Marseille")
    ]
    clients_df = spark.createDataFrame(clients_data)
    villes_df = spark.createDataFrame(villes_data)
    result_df = join_clients_with_city(clients_df, villes_df)
    result_data = result_df.collect()

    assert len(result_data) == 2
    assert result_data[0]["city"] == "Paris"
    assert result_data[1]["city"] == "Paris"

def test_add_department_column(spark):
    data = [
        Row(name="Alice", age=25, zip="75000"),
        Row(name="Charlie", age=30, zip="20000")
    ]
    df = spark.createDataFrame(data)
    result_df = add_department_column(df)
    result_data = result_df.collect()

    assert result_data[0]["departement"] == "75"
    assert result_data[1]["departement"] == "2A"

def test_add_department_column_error(spark):
    data = [
        Row(name="Alice", age=25, zip="ABCDE")  # Invalid zip code
    ]
    df = spark.createDataFrame(data)
    with pytest.raises(Exception):
        add_department_column(df)

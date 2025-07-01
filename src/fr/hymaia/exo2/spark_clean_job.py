import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Spark Clean Job") \
        .getOrCreate()

    clients_with_dept_df = first_job(spark)
    write_output(clients_with_dept_df, "data/exo2/clean")

def first_job(spark,
              clients_path="src/resources/exo2/clients_bdd.csv",
              villes_path="src/resources/exo2/city_zipcode.csv"):

    clients_df, villes_df = load_data(spark, clients_path, villes_path)
    majeur_clients_df = filter_majeur_clients(clients_df)
    clients_with_city_df = join_clients_with_city(majeur_clients_df, villes_df)
    clients_with_dept_df = add_department_column(clients_with_city_df)

    return clients_with_dept_df

def load_data(spark, clients_path, villes_path):
    clients_df = spark.read.option("header", True).csv(clients_path)
    villes_df = spark.read.option("header", True).csv(villes_path)
    return clients_df, villes_df

def filter_majeur_clients(clients_df):
    return clients_df.filter(F.col("age") >= 18)

def join_clients_with_city(major_clients_df, villes_df):
    return major_clients_df.join(villes_df, "zip", "left").dropDuplicates()

def add_department_column(df):
    zip_int = F.col("zip").cast("int")
    df = df.withColumn(
        "departement",
        F.when((zip_int >= 20000) & (zip_int <= 20190), F.lit("2A"))
         .when((zip_int > 20190) & (zip_int <= 20999), F.lit("2B"))
         .otherwise(F.substring(F.col("zip"), 1, 2))
    )
    return df

def write_output(df, output_path, output_format="parquet"):
    valid_formats = ["parquet", "csv"]
    if output_format not in valid_formats:
        raise ValueError(f"Invalid output format: {output_format}. Valid formats are: {valid_formats}")

    if output_format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif output_format == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

if __name__ == "__main__":
    main()

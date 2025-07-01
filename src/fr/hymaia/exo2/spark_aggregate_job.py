import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Spark Aggregate Job") \
        .getOrCreate()

    dept_population_df = second_job(spark)
    write_output(dept_population_df, "data/exo2/aggregate", output_format="csv")

def second_job(spark, clean_path="data/exo2/clean"):
    cleaned_data_df = load_cleaned_data(spark, clean_path)
    dept_population_df = calculate_population_by_dept(cleaned_data_df)
    return dept_population_df

def load_cleaned_data(spark, clean_path):
    return spark.read.parquet(clean_path)

def calculate_population_by_dept(df):
    dept_population_df = df.groupBy("departement").agg(F.count("name").alias("nombre_population"))
    dept_population_df = dept_population_df.orderBy(F.col("nombre_population").desc(), F.col("departement"))
    return dept_population_df

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

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq 
import time

spark = SparkSession.builder.config('spark.jars','src/resources/exo4/udf.jar').appName("exo4").master("local[*]").getOrCreate()

def main():
    start = time.time()
    df = spark.read.option('header', True).csv("src/resources/exo4/sell.csv")
    resultat_scala = df.withColumn("category_name", addCategoryName(f.col("category")))
    resultat_scala.show()
    print(resultat_scala.count())
    resultat_scala.write.mode("overwrite").parquet("output/exo4/scala_udf")
    end = time.time()
    print(f"Temps d'exécution : {end - start:.2f} secondes")

def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf  = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

if __name__ == "__main__":
    main()
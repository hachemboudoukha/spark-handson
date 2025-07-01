import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import time

spark = SparkSession.builder.appName("exo4_python_udf").master("local[*]").getOrCreate()

def main():
    start = time.time()
    df = spark.read.option('header', True).csv("src/resources/exo4/sell.csv")
    resultat_python = df.withColumn("category_name", add_catogery_name(f.col("category")))
    resultat_python.show()
    print(resultat_python.count())
    resultat_python.write.mode("overwrite").parquet("output/exo4/python_udf")
    end = time.time()
    print(f"Temps d'exécution : {end - start:.2f} secondes")

@f.udf(returnType=StringType())
def add_catogery_name(category):
    if category < "6":  
        return "food"
    else:
        return "fourniture"

if __name__ == "__main__":
    main()
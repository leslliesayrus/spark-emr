from pyspark.sql import SparkSession
from pyspark import SparkConf
from os.path import abspath

spark = SparkSession.builder.getOrCreate()
print(SparkConf().getAll())

print(SparkConf().getAll())
print(spark.sparkContext.setLogLevel("INFO"))


country_path = "s3://ingestion-bucket/country/*.csv"
population_path = "s3://ingestion-bucket/population/*.csv"

df_country = spark.read \
                          .format('csv') \
                          .option("inferSchema", "True") \
                          .option('header', 'True') \
                          .csv(country_path)

df_population = spark.read \
                          .format('csv') \
                          .option("inferSchema", "True") \
                          .option('header', 'True') \
                          .csv(population_path)

df_country.createOrReplaceTempView("country")
df_population.createOrReplaceTempView("population")

df_join = spark.sql('select c.Country, c.Capital, c.Continent, p.* from country c, population p where c.CCA3 = p.CCA3')

df_join = df_join.withColumnRenamed('Area_(km²)', 'area')
df_join = df_join.withColumnRenamed('Density_(per km²)', 'density_per_km')

df_join.write.format('parquet').mode('overwrite').save('s3://processed')




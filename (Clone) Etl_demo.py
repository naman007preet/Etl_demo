# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://landing@projectsaaugust.dfs.core.windows.net/",
  mount_point = "/mnt/landing",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://raw@projectsaaugust.dfs.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://euh@projectsaaugust.dfs.core.windows.net/",
  mount_point = "/mnt/euh",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://eh-layer@projectsaaugust.dfs.core.windows.net/",
  mount_point = "/mnt/eh-layer",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

# Source and destination paths
source_path = "/mnt/landing/imdb_top_1000.csv"
destination_path = "/mnt/raw/imdb_top_1000.csv"

# Copy the file
dbutils.fs.cp(source_path, destination_path)

print(f"File copied from {source_path} to {destination_path}")

# COMMAND ----------

# Read the CSV file from the raw layer
df = spark.read.csv("/mnt/raw/imdb_top_1000.csv", header=True, inferSchema=True)

display(df)

# COMMAND ----------

# Read the CSV file into a DataFrame
df = spark.read.csv("/mnt/raw/imdb_top_1000.csv", header=True, inferSchema=True)

# Table 1: Movies_Info
movies_info = df.select(
    "Poster_Link",
    "Series_Title",
    "Released_Year",
    "Certificate",
    "Runtime",
    "Genre",
    "IMDB_Rating",
    "Overview",
    "Meta_score"
)

# Table 2: Movies_Cast
movies_cast = df.select(
    "Series_Title",
    "Director",
    "Star1",
    "Star2",
    "Star3",
    "Star4",
    "No_of_Votes",
    "Gross"
)

# Write Movies_Info to /mnt/euh/movies_info
movies_info.write.format("csv").mode("overwrite").option("header", True).save("/mnt/euh/movies_info")

# Write Movies_Cast to /mnt/euh/movies_cast
movies_cast.write.format("csv").mode("overwrite").option("header", True).save("/mnt/euh/movies_cast")

print("Data successfully written to /mnt/euh")


# COMMAND ----------

from pyspark.sql.functions import regexp_replace, when, col

# Load Movies_Info and Movies_Cast data
movies_info_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/euh/movies_info")
movies_cast_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/euh/movies_cast")

# Normalize column names
movies_info_df = movies_info_df.toDF(*[col_name.strip() for col_name in movies_info_df.columns])

# Check if Gross column exists
if "Gross" in movies_info_df.columns:
    movies_info_df = movies_info_df.withColumn("Gross", regexp_replace(col("Gross"), ",", "").cast("double"))
else:
    print("Column 'Gross' does not exist in the data.")

# Transform Runtime and add Year_Bucket
movies_info_df = movies_info_df.withColumn("Runtime", regexp_replace(col("Runtime"), " min", "").cast("int")) \
                               .withColumn(
                                   "Year_Bucket",
                                   when((col("Released_Year") >= 1990) & (col("Released_Year") < 2000), "1990-1999")
                                   .when((col("Released_Year") >= 2000) & (col("Released_Year") < 2010), "2000-2009")
                                   .when((col("Released_Year") >= 2010) & (col("Released_Year") < 2020), "2010-2019")
                                   .otherwise("Before 1990")
                               )

# Join Movies_Info and Movies_Cast on a common key (if applicable)
transformed_df = movies_info_df.join(movies_cast_df, on="Series_Title", how="inner")

# Write transformed data to /mnt/eh-landing/transformed_movies
transformed_df.write.format("csv").mode("overwrite").option("header", True).save("/mnt/eh-layer/transformed_movies")
print("Transformed data successfully written to /mnt/eh-layer/transformed_movies")


# COMMAND ----------

transformed_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/mnt/eh-layer/transformed_movies")


# COMMAND ----------

transformed_df.createOrReplaceTempView("transformed_movies_view")


# COMMAND ----------

# Example Query: Get top 10 movies by IMDB rating
result = spark.sql("""
    SELECT Series_Title, IMDB_Rating, Released_Year
    FROM transformed_movies_view
    ORDER BY IMDB_Rating DESC
    LIMIT 10
""")
result.show()


# COMMAND ----------



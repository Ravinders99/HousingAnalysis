# Databricks notebook source
storage_account_name = ""
container_name = "bronze1"
storage_account_access_key = ""

# Mount Azure Data Lake Storage Gen2
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = f"/mnt/{container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)


# COMMAND ----------

from pyspark.sql import functions as F

# Step 1: Define your container name and file path
container_name = "bronze1"  # Replace with your container name
file_name = ""  # Your file name in the Data Lake

# Step 2: Load the JSON data from Azure Data Lake Storage into a DataFrame
df = spark.read.json(f"/mnt/{container_name}/{file_name}")

# Step 3: Display the DataFrame to verify it is loaded correctly
display(df)

# Step 4: Define valid ranges for latitude and longitude
valid_latitude_range = (42.0, 83.0)
valid_longitude_range = (-141.0, -53.0)

# Step 5: Mark the rows with invalid coordinates
df = df.withColumn(
    "invalid_coordinates",
    F.when(
        (df.latitude < valid_latitude_range[0]) | 
        (df.latitude > valid_latitude_range[1]) | 
        (df.longitude < valid_longitude_range[0]) | 
        (df.longitude > valid_longitude_range[1]), 
        True
    ).otherwise(False)
)

# Step 6: Display the DataFrame after marking invalid coordinates
display(df)

# Step 7: Fix the invalid coordinates by replacing them with average latitude and longitude
average_latitude = df.select(F.avg("latitude")).first()[0]
average_longitude = df.select(F.avg("longitude")).first()[0]

df = df.withColumn(
    "latitude",
    F.when(df.invalid_coordinates, F.lit(average_latitude)).otherwise(df.latitude)
).withColumn(
    "longitude",
    F.when(df.invalid_coordinates, F.lit(average_longitude)).otherwise(df.longitude)
)

# Step 8: Display the DataFrame after correcting coordinates
display(df)

# Step 9: Save the cleaned data back to Azure Data Lake Storage
df.write.json(f"/mnt/{container_name}/cleaned_data.json")


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Step 1: Load the JSON data into a DataFrame
file_path = "/mnt/{bronze1}/.json"
df = spark.read.json(file_path)

# Step 1: Print the schema and understand where 'housing_data' is nested
df.printSchema()
df.select("areas").printSchema()


# Step 1: Explode the `areas` and `housing_data` fields to access the necessary columns
df_flattened = df.select(
    "province", 
    "population",  # Province level population
    "tax_rate", 
    F.explode("areas").alias("area_data")
)

# Step 2: Now explode the nested `housing_data` from the `area_data`
df_flattened = df_flattened.select(
    "province", 
    "population",  # Province population
    "tax_rate", 
    "area_data.name",  # Area name
    "area_data.latitude", 
    "area_data.longitude", 
    "area_data.area_size_km2", 
    F.col("area_data.population").alias("area_population"),  # Rename area population to avoid ambiguity
    F.explode("area_data.housing_data").alias("housing_data")
)

# Step 3: Extract the fields from `housing_data`
df_flattened = df_flattened.select(
    "province", 
    "population",  # Province population
    "tax_rate", 
    "name",  # Area name
    "area_population",  # Area population
    "latitude", 
    "longitude", 
    "area_size_km2", 
    "housing_data.year", 
    "housing_data.average_price", 
    "housing_data.rooms", 
    "housing_data.area_size_m2", 
    "housing_data.facilities"
)

# Step 4: Handle invalid coordinates (latitude and longitude)
valid_latitude_range = (42.0, 83.0)  # Latitude from 42°N to 83°N
valid_longitude_range = (-141.0, -53.0)  # Longitude from -141°W to -53°W

df_flattened = df_flattened.withColumn(
    "invalid_coordinates", 
    F.when(
        (df_flattened.latitude < valid_latitude_range[0]) |
        (df_flattened.latitude > valid_latitude_range[1]) |
        (df_flattened.longitude < valid_longitude_range[0]) |
        (df_flattened.longitude > valid_longitude_range[1]), 
        True
    ).otherwise(False)
)

# Step 5: Replace invalid coordinates with averages (or handle as per requirement)
average_latitude = df_flattened.select(F.avg("latitude")).first()[0]
average_longitude = df_flattened.select(F.avg("longitude")).first()[0]

df_cleaned = df_flattened.withColumn(
    "latitude", 
    F.when(df_flattened.invalid_coordinates, average_latitude).otherwise(df_flattened.latitude)
).withColumn(
    "longitude", 
    F.when(df_flattened.invalid_coordinates, average_longitude).otherwise(df_flattened.longitude)
)

# Display the cleaned DataFrame
display(df_cleaned)

# Step 6: Save the cleaned data back to Azure Data Lake or any other storage
output_path = f"/mnt/{container_name}/cleaned_canadian_housing_data.json"
# df_cleaned.write.json(output_path, mode="overwrite")

# Define the new container name
new_container_name = "silver1"  # Replace with your new container name

# Define the path where the cleaned data will be saved in the new container
output_path = f"/mnt/silver1/cleaned_canadian_housing_data.json"

# Save the cleaned data to the new container
df_cleaned.write.json(output_path, mode="overwrite")

# Optional: Verify that the file is saved in the new container
display(dbutils.fs.ls(f"/mnt/{new_container_name}/"))


# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Step 1: Load the JSON data into a DataFrame from bronze1 container
file_path = "/mnt/bronze1/.json"
df = spark.read.json(file_path)

# Step 2: Print the schema to understand the structure of 'areas' and 'housing_data'
df.printSchema()
df.select("areas").printSchema()

# Step 3: Explode the `areas` field to access nested `housing_data`
df_flattened = df.select(
    "province", 
    "population",  # Province level population
    "tax_rate", 
    F.explode("areas").alias("area_data")
)

# Step 4: Explode the nested `housing_data` from `area_data`
df_flattened = df_flattened.select(
    "province", 
    "population",  # Province population
    "tax_rate", 
    "area_data.name",  # Area name
    "area_data.latitude", 
    "area_data.longitude", 
    "area_data.area_size_km2", 
    F.col("area_data.population").alias("area_population"),  # Rename area population to avoid ambiguity
    F.explode("area_data.housing_data").alias("housing_data")
)

# Step 5: Extract fields from `housing_data`
df_flattened = df_flattened.select(
    "province", 
    "population",  # Province population
    "tax_rate", 
    "name",  # Area name
    "area_population",  # Area population
    "latitude", 
    "longitude", 
    "area_size_km2", 
    "housing_data.year", 
    "housing_data.average_price", 
    "housing_data.rooms", 
    "housing_data.area_size_m2", 
    "housing_data.facilities"
)

# Step 6: Handle invalid coordinates (latitude and longitude)
valid_latitude_range = (42.0, 83.0)  # Latitude range for Canada (42°N to 83°N)
valid_longitude_range = (-141.0, -53.0)  # Longitude range for Canada (-141°W to -53°W)

df_flattened = df_flattened.withColumn(
    "invalid_coordinates", 
    F.when(
        (df_flattened.latitude < valid_latitude_range[0]) |
        (df_flattened.latitude > valid_latitude_range[1]) |
        (df_flattened.longitude < valid_longitude_range[0]) |
        (df_flattened.longitude > valid_longitude_range[1]), 
        True
    ).otherwise(False)
)

# Step 7: Replace invalid coordinates with averages (or handle as per your requirement)
average_latitude = df_flattened.select(F.avg("latitude")).first()[0]
average_longitude = df_flattened.select(F.avg("longitude")).first()[0]

df_cleaned = df_flattened.withColumn(
    "latitude", 
    F.when(df_flattened.invalid_coordinates, average_latitude).otherwise(df_flattened.latitude)
).withColumn(
    "longitude", 
    F.when(df_flattened.invalid_coordinates, average_longitude).otherwise(df_flattened.longitude)
)

# Step 8: Display the cleaned DataFrame
display(df_cleaned)

# Step 9: Save the cleaned data to silver1 container (skip the mounting step)
output_path = f"/mnt/silver1/cleaned_canadian_housing_data.json"
df_cleaned.write.json(output_path, mode="overwrite")

# Step 10: Verify that the file has been saved in the new container
display(dbutils.fs.ls(f"/mnt/silver1/"))


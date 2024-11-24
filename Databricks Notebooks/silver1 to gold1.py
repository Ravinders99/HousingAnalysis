# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, StringType

# Step 1: Load the cleaned data from silver1 container
df = spark.read.json("/mnt/silver1/.json")

# Step 2: Handle missing or null values (fill missing numeric and categorical columns separately)
# Fill numerical columns with medians
df_cleaned = df.na.fill({
    'average_price': df.approxQuantile("average_price", [0.5], 0.25)[0],  # Median for average_price
    'rooms': df.approxQuantile("rooms", [0.5], 0.25)[0],                  # Median for rooms
    'area_size_m2': df.approxQuantile("area_size_m2", [0.5], 0.25)[0],    # Median for area size
    'latitude': df.approxQuantile("latitude", [0.5], 0.25)[0],            # Median for latitude
    'longitude': df.approxQuantile("longitude", [0.5], 0.25)[0]           # Median for longitude
})

# Fill the 'facilities' column with an array of strings using withColumn
df_cleaned = df_cleaned.withColumn(
    "facilities",
    F.when(F.col("facilities").isNull(), F.array(F.lit("Unknown"))).otherwise(F.col("facilities"))
)

# Step 3: Remove duplicates
df_cleaned = df_cleaned.dropDuplicates()

# Step 4: Convert data types
df_cleaned = df_cleaned.withColumn("average_price", df_cleaned["average_price"].cast(DoubleType())) \
    .withColumn("rooms", df_cleaned["rooms"].cast(IntegerType())) \
    .withColumn("area_size_m2", df_cleaned["area_size_m2"].cast(DoubleType())) \
    .withColumn("latitude", df_cleaned["latitude"].cast(DoubleType())) \
    .withColumn("longitude", df_cleaned["longitude"].cast(DoubleType()))

# Step 5: Remove outliers for average_price, rooms, and area_size_m2
def remove_outliers(df, column):
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
    Q1, Q3 = quantiles[0], quantiles[1]
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return df.filter((df[column] >= lower_bound) & (df[column] <= upper_bound))

df_cleaned = remove_outliers(df_cleaned, "average_price")
df_cleaned = remove_outliers(df_cleaned, "rooms")
df_cleaned = remove_outliers(df_cleaned, "area_size_m2")

# Step 6: Feature engineering - create price per square meter
df_cleaned = df_cleaned.withColumn("price_per_m2", df_cleaned["average_price"] / df_cleaned["area_size_m2"])

# Step 7: Normalize numerical columns (e.g., average_price, area_size_m2, price_per_m2)
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=["average_price", "area_size_m2", "price_per_m2"], outputCol="features")
df_scaled = assembler.transform(df_cleaned)

scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(df_scaled)
df_normalized = scaler_model.transform(df_scaled)

df_final = df_normalized.drop("features").withColumnRenamed("scaled_features", "features_normalized")

# Step 8: Save the fully cleaned and transformed data back to gold1 container
output_path = f"/mnt/gold1/fully_cleaned_canadian_housing_data.json"
df_final.write.json(output_path, mode="overwrite")

# Step 9: Verify that the cleaned and transformed data is saved in gold1
display(dbutils.fs.ls("/mnt/gold1/"))


# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

storage_account_name = ""  # Your storage account name
container_name = "gold1"  # The gold1 container name
storage_account_access_key = "=="  # Replace with your access key

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = f"/mnt/gold1",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)


# COMMAND ----------

# Load the data
df = spark.read.json("/mnt/gold1/a.json")

# Show columns with missing values
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()


# Databricks notebook source
# MAGIC %md
# MAGIC Predicting Housing Prices with Gradient Boosted Trees

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Initialize Spark session
spark = SparkSession.builder.appName("AdvancedHousingPricePrediction").getOrCreate()

# Load and flatten the data as done before
df = spark.read.json("/mnt/gold1/data.json")

# Flatten necessary columns
df_flattened = df.select(
    "latitude",
    "longitude",
    "name",
    "population",
    "price_per_m2",
    "province",


    F.col("features_normalized.type").alias("features_normalized_type"),
    F.col("features_normalized.values").alias("features_normalized_values"),
    F.col("facilities").alias("facilities"),
    F.col("tax_rate.GST").alias("GST"),
    F.col("tax_rate.HST").alias("HST"),
    F.col("tax_rate.PST").alias("PST"),
    "average_price"
)

# Add total tax rate column
df_flattened = df_flattened.withColumn("total_tax_rate", F.col("GST") + F.col("HST") + F.col("PST"))

# Fully flatten the arrays by exploding them
df_exploded = df_flattened \
    .withColumn("features_normalized_value", F.explode("features_normalized_values")) \
    .withColumn("facility", F.explode("facilities")) \
    .drop("features_normalized_values", "facilities")

# Additional feature engineering: interaction terms and polynomial features
df_exploded = df_exploded.withColumn("population_price_interaction", F.col("population") * F.col("price_per_m2"))
df_exploded = df_exploded.withColumn("rooms_latitude_interaction", F.col("rooms") * F.col("latitude"))

# Select relevant features and define the target variable
feature_columns = [
    "latitude", "longitude", "population", "price_per_m2", "rooms", 
    "GST", "HST", "PST", "total_tax_rate", "features_normalized_value", 
    "population_price_interaction", "rooms_latitude_interaction"
]
label_column = "average_price"

# Assemble feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Use Gradient Boosted Trees for regression
gbt = GBTRegressor(featuresCol="features", labelCol=label_column, maxIter=100, maxDepth=5, stepSize=0.1)

# Create pipeline
pipeline = Pipeline(stages=[assembler, gbt])

# Split the data into training and testing datasets
train_data, test_data = df_exploded.randomSplit([0.8, 0.2], seed=123)

# Train the model
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model performance
evaluator_rmse = RegressionEvaluator(labelCol=label_column, predictionCol="prediction", metricName="rmse")
rmse = evaluator_rmse.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

evaluator_r2 = RegressionEvaluator(labelCol=label_column, predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)
print(f"R-squared (R2): {r2}")

# (Optional) Save predictions to a CSV file for Power BI
predictions.select("latitude", "longitude", "average_price", "prediction") \
    .write.csv("/mnt/machinelearning/advanced_predictions_housing_price.csv", mode="overwrite", header=True)

print("Advanced model training and evaluation complete.")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Initialize Spark session
spark = SparkSession.builder.appName("AdvancedHousingPricePrediction").getOrCreate()

# Load the data
df = spark.read.json("/mnt/gold1/fully_cleaned_canadian_housing_data.json")

# Flatten necessary columns and add population density
df_flattened = df.select(
    "latitude",
    "longitude",
    "name",
    "population",
    "price_per_m2",
    "province",
    "rooms",
    "year",
    F.col("features_normalized.type").alias("features_normalized_type"),
    F.col("features_normalized.values").alias("features_normalized_values"),
    F.col("facilities").alias("facilities"),
    F.col("tax_rate.GST").alias("GST"),
    F.col("tax_rate.HST").alias("HST"),
    F.col("tax_rate.PST").alias("PST"),
    "average_price",
    "area_size_km2"
)

# Calculate population density
df_flattened = df_flattened.withColumn("population_density", F.col("population") / F.col("area_size_km2"))

# Add total tax rate column
df_flattened = df_flattened.withColumn("total_tax_rate", F.col("GST") + F.col("HST") + F.col("PST"))

# Add facility count
df_flattened = df_flattened.withColumn("facility_count", F.size("facilities"))

# Fully flatten the arrays by exploding them
df_exploded = df_flattened \
    .withColumn("features_normalized_value", F.explode("features_normalized_values")) \
    .withColumn("facility", F.explode("facilities")) \
    .drop("features_normalized_values", "facilities")

# Additional feature engineering: interaction terms and polynomial features
df_exploded = df_exploded.withColumn("population_price_interaction", F.col("population") * F.col("price_per_m2"))
df_exploded = df_exploded.withColumn("rooms_latitude_interaction", F.col("rooms") * F.col("latitude"))

# Select relevant features and define the target variable
feature_columns = [
    "latitude", "longitude", "population", "price_per_m2", "rooms", 
    "GST", "HST", "PST", "total_tax_rate", "features_normalized_value", 
    "population_density", "facility_count", "population_price_interaction", "rooms_latitude_interaction", "year"
]
label_column = "average_price"

# Assemble feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Use Gradient Boosted Trees for regression
gbt = GBTRegressor(featuresCol="features", labelCol=label_column, maxIter=100, maxDepth=5, stepSize=0.1)

# Create pipeline
pipeline = Pipeline(stages=[assembler, gbt])

# Split the data into training and testing datasets
train_data, test_data = df_exploded.randomSplit([0.8, 0.2], seed=123)

# Train the model
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model performance
evaluator_rmse = RegressionEvaluator(labelCol=label_column, predictionCol="prediction", metricName="rmse")
rmse = evaluator_rmse.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

evaluator_r2 = RegressionEvaluator(labelCol=label_column, predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)
print(f"R-squared (R2): {r2}")

# Save predictions to a CSV file for Power BI
predictions.select("latitude", "longitude", "average_price", "prediction", "year", "facility_count") \
    .write.csv("/mnt/machinelearning/advanced_predictions.csv", mode="overwrite", header=True)

print("Advanced model training and evaluation complete.")


# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement 2: Tax Rate and Affordability Index by Region

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("CanadianHousingAnalysis").getOrCreate()

# Load the cleaned data
df = spark.read.json("/mnt/gold1/fully_cleaned_canadian_housing_data.json")

# Flatten the necessary fields
df_flattened = df.select(
    "latitude",
    "longitude",
    "name",
    "population",
    "price_per_m2",
    "province",
    "rooms",
    "year",
    F.col("features_normalized.type").alias("features_normalized_type"),
    F.col("features_normalized.values").alias("features_normalized_values"),
    F.col("facilities").alias("facilities"),
    F.col("tax_rate.GST").alias("GST"),
    F.col("tax_rate.HST").alias("HST"),
    F.col("tax_rate.PST").alias("PST"),
    "average_price",
    "area_size_km2"
)

# Calculate additional features: population density and total tax rate
df_flattened = df_flattened.withColumn("population_density", F.col("population") / F.col("area_size_km2"))
df_flattened = df_flattened.withColumn("total_tax_rate", F.col("GST") + F.col("HST") + F.col("PST"))

# Count the number of facilities (if facilities array exists)
df_flattened = df_flattened.withColumn("facility_count", F.size("facilities"))

# Explode the features_normalized_values and facilities arrays if necessary
df_exploded = df_flattened \
    .withColumn("features_normalized_value", F.explode("features_normalized_values")) \
    .withColumn("facility", F.explode("facilities")) \
    .drop("features_normalized_values", "facilities")

# Display schema to verify flattening
df_exploded.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC  Calculate Tax Rate and Affordability Index by Region
# MAGIC

# COMMAND ----------

# Calculate average price per square meter and tax rates by province
affordability_df = df_exploded.groupBy("province").agg(
    F.avg("price_per_m2").alias("avg_price_per_m2"),
    F.avg("total_tax_rate").alias("avg_total_tax_rate")
)

# Define an affordability index (assuming a hypothetical median income, e.g., 50,000)
affordability_df = affordability_df.withColumn("affordability_index", F.lit(50000) / F.col("avg_price_per_m2"))

# Save results for visualization in Power BI
affordability_df.write.csv("/mnt/machinelearning/affordability_by_region.csv", mode="overwrite", header=True)


# COMMAND ----------

# MAGIC %md
# MAGIC Regional Comparison of Housing Characteristics

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("RegionalHousingCharacteristics").getOrCreate()

# Load the data
df = spark.read.json("/mnt/gold1/fully_cleaned_canadian_housing_data.json")

# Flatten necessary columns and add population density and total tax rate
df_flattened = df.select(
    "latitude",
    "longitude",
    "name",
    "population",
    "price_per_m2",
    "province",
    "rooms",
    "year",
    F.col("features_normalized.type").alias("features_normalized_type"),
    F.col("features_normalized.values").alias("features_normalized_values"),
    F.col("facilities").alias("facilities"),
    F.col("tax_rate.GST").alias("GST"),
    F.col("tax_rate.HST").alias("HST"),
    F.col("tax_rate.PST").alias("PST"),
    "average_price",
    "area_size_km2"
)

# Calculate population density and total tax rate
df_flattened = df_flattened.withColumn("population_density", F.col("population") / F.col("area_size_km2"))
df_flattened = df_flattened.withColumn("total_tax_rate", F.col("GST") + F.col("HST") + F.col("PST"))

# Add facility count
df_flattened = df_flattened.withColumn("facility_count", F.size("facilities"))

# Explode any arrays to fully flatten the dataset
df_exploded = df_flattened \
    .withColumn("features_normalized_value", F.explode("features_normalized_values")) \
    .withColumn("facility", F.explode("facilities")) \
    .drop("features_normalized_values", "facilities")

# Aggregate average price, rooms (as integer), and total tax rate by province
regional_stats_df = df_exploded.groupBy("province").agg(
    F.avg("average_price").alias("avg_price"),
    F.avg("rooms").cast("int").alias("avg_rooms"),  # Cast to integer
    F.avg("total_tax_rate").alias("avg_total_tax_rate"),
    F.count("name").alias("num_properties")
)

# Save for visualization
regional_stats_df.write.csv("/mnt/machinelearning/regional_housing_characteristics.csv", mode="overwrite", header=True)


# COMMAND ----------

# Aggregate average price, rooms, and total tax rate by province
regional_stats_df = df_exploded.groupBy("province").agg(
    F.avg("average_price").alias("avg_price"),
    F.avg("rooms").alias("avg_rooms"),
    F.avg("total_tax_rate").alias("avg_total_tax_rate"),
    F.count("name").alias("num_properties")
)

# Save the aggregated data for visualization in Power BI
regional_stats_df.write.csv("/mnt/machinelearning/regional_housing_characteristics.csv", mode="overwrite", header=True)


# COMMAND ----------

# MAGIC %md
# MAGIC

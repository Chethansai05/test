from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, expr

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TransactionSummaryStats") \
    .getOrCreate()

# Load dataset
data_path = "path_to_your_dataset"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Perform transformations, filtering, and aggregations
summary_stats = df.select(
    mean(df["amount"]).alias("mean_amount"),
    expr("percentile_approx(amount, 0.5)").alias("median_amount"),
    stddev(df["amount"]).alias("stddev_amount")
)

# Show summary statistics
summary_stats.show()

# Stop SparkSession
spark.stop()

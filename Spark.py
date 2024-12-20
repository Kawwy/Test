from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Average Age of Babies by Gender") \
    .getOrCreate()

# Load the dataset
baby_data = spark.read.csv("baby_names.csv", header=True, inferSchema=True)

# Function to determine gender
def get_gender(gender_code):
    return 'Male' if gender_code == 'M' else 'Female'

# Register the function as a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

get_gender_udf = udf(get_gender, StringType())

# Add a gender column using the UDF
baby_data = baby_data.withColumn("Gender", get_gender_udf(col("gender_code")))

# Filter the data for the year 2013
baby_data_2013 = baby_data.filter(col("year") == 2013)

# Calculate average age by gender
avg_age_by_gender = (
    baby_data_2013.groupBy("Gender")
    .avg("age")
    .withColumn("Average_Age", round(col("avg(age)"), 1))
    .select("Gender", "Average_Age")
    .orderBy("Gender")
)

# Save the output as a single text file
avg_age_by_gender.coalesce(1).write.format("text").mode("overwrite").save("output_avg_age_by_gender")

# Show the results
avg_age_by_gender.show()

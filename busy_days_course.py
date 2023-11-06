from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
import matplotlib.pyplot as plt

# Initialize a SparkSession
spark = SparkSession.builder.appName("BusyDays").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read the CSV files into DataFrames
courses_df = spark.read.csv("course.csv", header=True, inferSchema=True)
schedule_df = spark.read.csv("schedule.csv", header=True, inferSchema=True)

# Split and explode the COURSE_DAYS column to get individual days
schedule_df = schedule_df.withColumn("COURSE_DAYS", split(schedule_df["COURSE_DAYS"], ","))
schedule_df = schedule_df.select(
    "ID", "COURSE_ID", "LECTURER_ID", "START_DT", "END_DT", explode(schedule_df["COURSE_DAYS"]).alias("DAY")
)

# Group by day and count courses on each day
busy_days = schedule_df.groupBy("DAY").count().orderBy("DAY")

# Collect data to the local Python environment
busy_days_data = busy_days.collect()

# Extract day names and counts
day_names = [row["DAY"] for row in busy_days_data]
course_counts = [row["count"] for row in busy_days_data]

# Plot the data using Matplotlib
plt.bar(day_names, course_counts)
plt.xlabel("Day of the Week")
plt.ylabel("Number of Courses")
plt.title("Busy Days for Courses")
plt.show()

# Stop the SparkSession
spark.stop()

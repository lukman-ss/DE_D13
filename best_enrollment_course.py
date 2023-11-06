from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os

# Set the log4j configuration file
log4j_conf_file = os.path.join(os.path.dirname(__file__), "log4j.properties")
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {log4j_conf_file} pyspark-shell"
# Initialize a Spark session
spark = SparkSession.builder.appName("BestEnrollmentCourse").getOrCreate()
spark.sparkContext.setLogLevel("INFO")
# Load enrollment.csv
enrollment_df = spark.read.csv("enrollment.csv", header=True, inferSchema=True)

# Load course.csv
course_df = spark.read.csv("course.csv", header=True, inferSchema=True)

# Group the enrollment data by COURSE_ID and count the number of enrollments
enrollment_count_df = enrollment_df.groupBy("SCHEDULE_ID").agg(count("*").alias("enrollment_count"))

# Find the schedule with the maximum enrollment count
max_enrollment = enrollment_count_df.agg({"enrollment_count": "max"}).collect()[0][0]

# Join the enrollment and course dataframes to get the course name
best_courses_df = enrollment_count_df \
    .filter(col("enrollment_count") == max_enrollment) \
    .join(enrollment_df, "SCHEDULE_ID", "inner") \
    .join(course_df, "ID", "inner") \
    .select("NAME")

# Collect the best course names into a Pandas DataFrame
best_courses_pandas_df = best_courses_df.toPandas()

# Stop the Spark session
spark.stop()

# Now, you can use Matplotlib to create a bar chart
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.bar(best_courses_pandas_df["NAME"], [max_enrollment] * len(best_courses_pandas_df))
plt.xlabel("Best Enrollment Courses")
plt.ylabel("Enrollment Count")
plt.title("Enrollment Count for Best Courses")
plt.xticks(rotation=45, ha="right")

# Show the bar chart
plt.tight_layout()
plt.show()

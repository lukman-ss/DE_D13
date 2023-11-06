from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max, col
import matplotlib.pyplot as plt


# Initialize a SparkSession
spark = SparkSession.builder.appName("BestStudentAttendance").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Read the CSV files into DataFrames
course_attendance_df = spark.read.csv("course_attendance.csv", header=True, inferSchema=True)
courses_df = spark.read.csv("course.csv", header=True, inferSchema=True)

# Join the DataFrames to get course names
joined_df = course_attendance_df.join(courses_df, course_attendance_df["SCHEDULE_ID"] == courses_df["ID"], "inner")

# Group by STUDENT_ID and get the count of attendances and the maximum date
student_attendance = joined_df.groupBy("STUDENT_ID") \
    .agg(count("ATTEND_DT").alias("ATTENDANCE_COUNT"), max("ATTEND_DT").alias("LAST_ATTENDANCE_DATE")) \
    .orderBy(col("ATTENDANCE_COUNT").desc())

# Collect data to the local Python environment
student_attendance_data = student_attendance.limit(5).collect()

# Extract student IDs and attendance counts
student_ids = [row["STUDENT_ID"] for row in student_attendance_data]
attendance_counts = [row["ATTENDANCE_COUNT"] for row in student_attendance_data]

# Plot the data using Matplotlib
plt.bar(student_ids, attendance_counts, tick_label=student_ids)
plt.xlabel("Student ID")
plt.ylabel("Attendance Count")
plt.title("Top 5 Students with the Best Attendance")
plt.show()

# Stop the SparkSession
spark.stop()

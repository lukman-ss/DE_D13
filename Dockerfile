# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory to /app
WORKDIR /app

# Copy all files from the current directory into the container
COPY best_attendanced.py best_enrollment_course.py busy_days_course.py dibimbing_pyspark_pt_2.py ./
COPY course.csv course_attendance.csv enrollment.csv schedule.csv ./
COPY requirements.txt ./
# Install any necessary dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Make sure your Python scripts are executable
RUN chmod +x best_attendanced.py best_enrollment_course.py busy_days_course.py dibimbing_pyspark_pt_2.py

# Define the command to run the Python script with arguments
CMD ["python", "main.py"]

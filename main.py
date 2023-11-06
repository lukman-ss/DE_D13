import sys
import subprocess

# Define the available script names
available_scripts = [
    "best_attendanced.py",
    "best_enrollment_course.py",
    "busy_days_course.py",
    "dibimbing_pyspark_pt_2.py",
]

# Check if a valid script name is provided as an argument
if len(sys.argv) < 2 or sys.argv[1] not in available_scripts:
    print("Usage: python main.py [script_name.py]")
    sys.exit(1)

# Run the selected Python script with the remaining arguments
script_name = sys.argv[1]
subprocess.run(["python", script_name] + sys.argv[2:])

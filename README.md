# ECE875
Computer System in Data Analytics
Sean Cowley
V688J944
SPRING 2024
Project (Group 8 - Solo)

This repository documents the following efforts to build a benchmark tool for a permutation of scheduling settings on a diverse range of system architectures (x86, ARM64, ARM32).

# PySpark Scheduler Test

This script tests the performance of different PySpark scheduler configurations by running a computationally intensive task and recording the time taken and CPU usage.

## Author

Sean Cowley

## Date

2024 May 4

## Resource Requirements

- PySpark: This script requires a Spark environment with PySpark installed.
- Memory: The script is configured to use 8GB of memory for the Spark executor and driver. Make sure your system has enough memory to accommodate this.
- CPU: The script measures CPU usage, so it may consume significant CPU resources while running.

## Required Packages

- pyspark: This package provides the PySpark API for Python.
- numpy: This package is used for calculating the mean of the time taken and CPU usage.
- psutil: This package is used for getting the CPU usage.
- csv: This package is used for writing the results to CSV files.
- gc: This package is used for triggering garbage collection to help manage memory.
- datetime: This package is used for getting the current date and time to include in the CSV file names.

## How to Run

1. Make sure you have all the required packages installed.
2. Adjust the memory settings in the `SparkSession.builder` configuration if necessary.
3. Run the script in a Python environment with PySpark installed.

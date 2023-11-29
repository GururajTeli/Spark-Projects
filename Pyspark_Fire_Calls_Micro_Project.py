""" Project Description: San Francisco Fire Calls.
In this micro project, we will see how to use DataFrame for common data analytics patterns 
and operations on a San Francisco Fire Department Calls dataset.

Data Set: San Francisco Fire Department Calls for Service
Databricks Cloud Path: /databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv """

# Requirements

# Load the given data file and create a Spark Dataframe.

from pyspark.sql.functions import *

raw_fire_df = spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

""" Problems with the data set:
1. Column Names are not standardized.
2. Some Date fields are of string type."""

# Column Names standardization
renamed_fire_df = raw_fire_df \
.withColumnRenamed("Call Number", "CallNumber") \
.withColumnRenamed("Unit ID", "UnitID") \
.withColumnRenamed("Incident Number", "IncidentNumber") \
.withColumnRenamed("Call Date", "CallDate") \
.withColumnRenamed("Watch Date", "WatchDate") \
.withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
.withColumnRenamed("Available DtTm", "AvailableDtTm") \
.withColumnRenamed("Zipcode of Incident", "Zipcode") \
.withColumnRenamed("Station Area", "StationArea") \
.withColumnRenamed("Final Priority", "FinalPriority") \
.withColumnRenamed("ALS Unit", "ALSUnit") \
.withColumnRenamed("Call Type Group", "CallTypeGroup") \
.withColumnRenamed("Unit sequence in call dispatch", "Unitsequenceincalldispatch") \
.withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
.withColumnRenamed("Supervisor District", "SupervisorDistrict")

# Converting Some Date fields from string to timestamp
# Rounding Delay Column
fire_df = renamed_fire_df \
    .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("Delay", round("Delay", 2))

# Q-1) How many distinct types of calls were made to the Fire Department?

""" 1. SQL Approach
- Convert your dataframe to a temporary view
- Run your SQL on the view """

fire_df.createOrReplaceTempView("fire_service_calls_view")
q1_sql_df = spark.sql("""
                      select count(distinct CallType) as no_of_distinct_calls 
                      from fire_service_calls_view 
                      """)
q1_sql_df.show()

# Dataframe Transformation Approach

q1_df = fire_df.where("CallType is not null") \
        .select("CallType") \
        .distinct()
display(q1_df.count())

# Q-2) What are distinct types of calls were made to the Fire Department?

q2_df = fire_df.where("CallType is not null") \
        .select("CallType") \
        .distinct()
q2_df.show()

# Q-3) Find out all response for delayed times greater than 5 mins?

q3_df = fire_df.where("Delay > 5") \
        .select("CallNumber", "Delay")
q3_df.show()

# Q-4) What were the most common call types?

q4_df = fire_df.select("CallType") \
        .groupBy("CallType") \
        .count() \
        .orderBy("count", ascending=False)
q4_df.show(1)

# Q-5) What zip codes accounted for most common calls?

fire_df.select("CallType", "Zipcode") \
    .groupBy("CallType", "Zipcode") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# Q-6) What San Francisco neighborhoods are in the zip codes 94102 and 94103

fire_df.select("Neighborhood", "Zipcode") \
    .where("Zipcode in (94102, 94103)") \
    .distinct() \
    .show()

# Q-7) What was the sum of all call alarms, average, min and max of the response times for calls?

fire_df.select(
    sum("NumAlarms").alias("total_call_alarms"),
    avg("Delay").alias("avg_call_response_time"),
    min("Delay").alias("min_call_response_time"),
    max("Delay").alias("max_call_response_time"),
).show()

# Q-8) How many distinct years of data is in the dataset?

fire_df.select(year("CallDate").alias("call_year")) \
    .distinct() \
    .orderBy("call_year") \
    .show()

# Q-9) What week of the year in 2018 had the most fire calls?

fire_df.withColumn('week_of_year',weekofyear("CallDate")).select("week_of_year") \
    .filter(year("CallDate") == 2018) \
    .groupBy("week_of_year") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(1)

# Q-10) What neighborhoods in San Francisco had the worst response time in 2018?

fire_df.select("Neighborhood", "Delay") \
    .filter(year("CallDate") == 2018) \
    .orderBy("Delay", ascending=False) \
    .show(1)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, avg, max, min, year, first

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Temperature Data Analysis") \
    .getOrCreate()

# File path to the CSV file (update the path as necessary)
file_path = "GlobalLandTemperatures_GlobalLandTemperaturesByMajorCity.csv"

# Load the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Display the schema of the DataFrame
df.printSchema()

# Filter the DataFrame to only include records from India
df_india = df.filter(df['Country'] == 'India')

# Extract state, temperature, and year information (assuming 'City' can be mapped to states)
df_india = df_india.withColumnRenamed("City", "State")
df_india = df_india.withColumn("Year", year(col("dt")))

# Select relevant columns (date, state, temperature, year)
df_india = df_india.select("dt", "Year", "State", "AverageTemperature")

# Remove null temperature values
df_india = df_india.filter(df_india["AverageTemperature"].isNotNull())

# Filter data to only include temperatures for the years 2000 to 2010
df_india_2000_2010 = df_india.filter((df_india["Year"] >= 2000) & (df_india["Year"] <= 2010))

# Calculate the average temperature for each state for 2000-2010
df_avg_temp_by_state = df_india_2000_2010.groupBy("State").agg(avg("AverageTemperature").alias("AvgTemperature"))

# Show the average temperature for each state for 2000-2010
df_avg_temp_by_state.orderBy("State").show()

# Group by state and year to get the maximum temperature for each state and year for 2000-2010
df_max_temp_by_state = df_india_2000_2010.groupBy("State", "Year").agg(max("AverageTemperature").alias("MaxTemperature"))

# Get the top 10 states with the highest maximum temperature for 2000-2010
top_10_hottest_states = df_max_temp_by_state.orderBy(desc("MaxTemperature")).limit(10)

# Show the top 10 hottest states for 2000-2010
top_10_hottest_states.show()

# Group by state and year to get the minimum temperature for each state and year for 2000-2010
df_min_temp_by_state = df_india_2000_2010.groupBy("State", "Year").agg(min("AverageTemperature").alias("MinTemperature"))

# Get the top 10 states with the lowest minimum temperature for 2000-2010
top_10_coldest_states = df_min_temp_by_state.orderBy(asc("MinTemperature")).limit(10)

# Show the top 10 coldest states for 2000-2010
top_10_coldest_states.show()

# Stop the SparkSession
spark.stop()
